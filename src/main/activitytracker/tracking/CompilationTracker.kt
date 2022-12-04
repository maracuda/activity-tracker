package activitytracker.tracking

import activitytracker.Type
import activitytracker.Type.CompilationFinished
import activitytracker.liveplugin.newDisposable
import activitytracker.liveplugin.registerProjectListener
import com.intellij.ide.AppLifecycleListener
import com.intellij.openapi.Disposable
import com.intellij.openapi.compiler.CompilationStatusListener
import com.intellij.openapi.compiler.CompileContext
import com.intellij.openapi.compiler.CompilerTopics

interface CompilationTracker {
    fun startActionListener(
        parentDisposable: Disposable,
        callback: (eventType: Type, originalEventData: String) -> Unit
    ) {}

    companion object {
        var instance = object: CompilationTracker {}
    }
}

/**
 * Also works for other JVM-based languages.
 */
class InitJavaCompilationTracker: AppLifecycleListener {
    override fun appFrameCreated(commandLineArgs: List<String>) {
        CompilationTracker.instance = object: CompilationTracker {
            override fun startActionListener(
                parentDisposable: Disposable,
                callback: (eventType: Type, originalEventData: String) -> Unit
            ) {
                registerCompilationListener(parentDisposable, object: CompilationStatusListener {
                    override fun compilationFinished(aborted: Boolean, errors: Int, warnings: Int, compileContext: CompileContext) {
                        callback(CompilationFinished, errors.toString())
                    }
                })
            }

            private fun registerCompilationListener(disposable: Disposable, listener: CompilationStatusListener) {
                registerProjectListener(disposable) { project ->
                    project.messageBus
                        .connect(newDisposable(disposable, project))
                        .subscribe(CompilerTopics.COMPILATION_STATUS, listener)
                }
            }
        }
    }
}