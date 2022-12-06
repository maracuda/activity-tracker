package activitytracker

import com.clickhouse.client.*
import kotlinx.serialization.encodeToString
import org.joda.time.DateTime
import org.junit.Test
import kotlinx.serialization.json.Json
import java.util.*

class ClickHouseTests {
    @Test fun `try insert`() {
        writeToClickhouse();
    }

    fun writeToClickhouse() {
        val endpoint = ClickHouseNode.of(
            "localhost",
            ClickHouseProtocol.HTTP,
            8123,
            "productivity"
        ); // http://localhost:8443?ssl=true&sslmode=NONE

        val client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);

        val event = TrackerEvent(
            DateTime.now(), "tester", Type.IdeState, "DATATA", "PROJECT",
            "COMPONENT", "FILE", "PATH", 44, 22, "232"
        )


        val toSerialize = TrackerEventWithoutDate(
            event.userName,
            event.type,
            event.data,
            event.projectName,
            event.focusedComponent,
            event.file,
            event.psiPath,
            event.editorLine,
            event.editorColumn,
            event.task
        )

        val json = Json.encodeToString(toSerialize)

        client.connect(endpoint).query("\n" +
                "\n" +
                "INSERT INTO productivity.stats VALUES \n" +
                "('${UUID.randomUUID()}','${event?.time?.toString("YYYY-MM-dd HH:mm:ss")}', '${event?.userName}', '${event?.type}', '$json')\n" )
            .execute().get()
    }
}