package activitytracker

import activitytracker.liveplugin.whenDisposed
import activitytracker.TrackerEvent.Companion.printEvent
import activitytracker.TrackerEvent.Companion.toTrackerEvent
import com.clickhouse.client.ClickHouseClient
import com.clickhouse.client.ClickHouseNode
import com.clickhouse.client.ClickHouseProtocol
import com.intellij.concurrency.JobScheduler
import com.intellij.openapi.Disposable
import com.intellij.openapi.diagnostic.Logger
import com.intellij.openapi.util.io.FileUtil
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVPrinter
import java.io.File
import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.text.Charsets.UTF_8

class TrackerLog(private val eventsFilePath: String) {
    private val log = Logger.getInstance(TrackerLog::class.java)
    private val eventQueue: Queue<TrackerEvent> = ConcurrentLinkedQueue()
    private val sendQueue: Queue<TrackerEvent> = ConcurrentLinkedQueue()

    fun writeToClickhouse() {

        val endpoint = ClickHouseNode.of(
            "localhost",
            ClickHouseProtocol.HTTP,
            8123,
            "productivity"
        ); // http://localhost:8443?ssl=true&sslmode=NONE

        val client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);

        client.connect(endpoint).query("\n" +
                "\n" +
                "INSERT INTO productivity.stats VALUES \n" +
                "(now(), 'bis', 'First', 'ValueValue')\n" +
                "\n").execute().get()
    }

    fun initWriter(parentDisposable: Disposable, writeFrequencyMs: Long): TrackerLog {
        val fileLogAppenderWork = {
            try {
                val file = File(eventsFilePath)
                FileUtil.createIfDoesntExist(file)
                FileOutputStream(file, true).buffered().writer(UTF_8).use { writer ->
                    val csvPrinter = CSVPrinter(writer, CSVFormat.RFC4180)
                    var event: TrackerEvent? = eventQueue.poll()
                    while (event != null) {
                        csvPrinter.printEvent(event)
                        event = eventQueue.poll()
                    }
                    csvPrinter.flush()
                    csvPrinter.close()
                }
            } catch (e: Exception) {
                log.error(e)
            }
        }

        val networkLogAppenderWork = {
            try {
                val endpoint = ClickHouseNode.of(
                    "localhost",
                    ClickHouseProtocol.HTTP,
                    8123,
                    "productivity"
                ); // http://localhost:8443?ssl=true&sslmode=NONE

                val client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);

                var event: TrackerEvent? = sendQueue.poll()
                while (event != null) {


                    val json = Json.encodeToString(
                        TrackerEventWithoutDate(
                            event?.userName,
                            event?.type,
                            event?.data,
                            event?.projectName,
                            event?.focusedComponent,
                            event?.file,
                            event?.psiPath,
                            event?.editorLine,
                            event?.editorColumn,
                            event?.task
                        )
                    )

                    client.connect(endpoint).query("\n" +
                            "\n" +
                            "INSERT INTO productivity.stats VALUES \n" +
                            "('${event?.time?.toString("YYYY-MM-dd HH:mm:ss")}', '${event?.userName}', '${event?.type}', '$json')\n" )
                        .execute().get()

                    event = sendQueue.poll()
                }
            } catch (e: Exception) {
                log.error(e)
            }
        }

        val future = JobScheduler.getScheduler()
            .scheduleWithFixedDelay(fileLogAppenderWork, writeFrequencyMs, writeFrequencyMs, MILLISECONDS)
        val networkFuture = JobScheduler.getScheduler()
            .scheduleWithFixedDelay(networkLogAppenderWork, writeFrequencyMs, writeFrequencyMs, MILLISECONDS)

        parentDisposable.whenDisposed {
            future.cancel(true)
        }
        parentDisposable.whenDisposed {
            networkFuture.cancel(true)
        }
        return this
    }

    fun append(event: TrackerEvent?) {
        if (event == null) return
        eventQueue.add(event)
        sendQueue.add(event)
    }

    fun clearLog(): Boolean = FileUtil.delete(File(eventsFilePath))

    fun readEvents(onParseError: (String, Exception) -> Any): Sequence<TrackerEvent> {
        val reader = File(eventsFilePath).bufferedReader(UTF_8)
        val parser = CSVParser(reader, CSVFormat.RFC4180)
        val sequence = parser.asSequence().map { csvRecord ->
            try {
                csvRecord.toTrackerEvent()
            } catch (e: Exception) {
                onParseError(csvRecord.toString(), e)
                null
            }
        }

        return sequence.filterNotNull().onClose {
            parser.close()
            reader.close()
        }
    }

    fun rollLog(now: Date = Date()): File {
        writeToClickhouse();

        val postfix = SimpleDateFormat("_yyyy-MM-dd").format(now)
        var rolledStatsFile = File(eventsFilePath + postfix)
        var i = 1
        while (rolledStatsFile.exists()) {
            rolledStatsFile = File(eventsFilePath + postfix + "_" + i)
            i++
        }

        FileUtil.rename(File(eventsFilePath), rolledStatsFile)
        return rolledStatsFile
    }

    fun currentLogFile(): File = File(eventsFilePath)

    fun isTooLargeToProcess(): Boolean {
        val `2gb` = 2000000000L
        return File(eventsFilePath).length() > `2gb`
    }
}


private fun <T> Sequence<T>.onClose(action: () -> Unit): Sequence<T> {
    val iterator = this.iterator()
    return object : Sequence<T> {
        override fun iterator() = object : Iterator<T> {
            override fun hasNext(): Boolean {
                val result = iterator.hasNext()
                if (!result) action()
                return result
            }

            override fun next() = iterator.next()
        }
    }
}
