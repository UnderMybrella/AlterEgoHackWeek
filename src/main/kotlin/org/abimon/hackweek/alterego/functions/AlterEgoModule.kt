package org.abimon.hackweek.alterego.functions

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import org.abimon.hackweek.alterego.AlterEgo
import java.util.concurrent.Executors

@ExperimentalUnsignedTypes
@ExperimentalCoroutinesApi
abstract class AlterEgoModule(val alterEgo: AlterEgo) {
    companion object {
        fun newDefaultThread(runnable: Runnable): Thread {
            val thread = Thread(runnable, "alter-module")
            thread.isDaemon = true
            return thread
        }
    }

    val loggerContext = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    val defaultContext = Executors.newCachedThreadPool(Companion::newDefaultThread).asCoroutineDispatcher()

    fun command(name: String): String? = alterEgo.config["command.$name"]?.toString()

    abstract fun register()

    suspend fun newID(): String = newIDLong().toString()
    suspend fun newIDLong(): Long = alterEgo.snowstorm.generate()
}