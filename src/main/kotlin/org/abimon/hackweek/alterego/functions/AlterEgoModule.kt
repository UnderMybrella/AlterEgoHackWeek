package org.abimon.hackweek.alterego.functions

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import org.abimon.hackweek.alterego.AlterEgo
import java.util.concurrent.Executors

@ExperimentalUnsignedTypes
@ExperimentalCoroutinesApi
abstract class AlterEgoModule(val alterEgo: AlterEgo) {
    val loggerContext = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    abstract fun register()
}