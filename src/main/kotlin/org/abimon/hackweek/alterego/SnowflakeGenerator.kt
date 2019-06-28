package org.abimon.hackweek.alterego

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Clock

class SnowflakeGenerator(workerID: Int, processID: Int, val epoch: Long = 1561395600000L) {
    private val mutex = Mutex()
    private var clock = Clock.systemUTC()
    private var thisMs = 0L
    private var lastTime = clock.millis()

    val workerID = (workerID.toLong() % 0b100000) shl 17
    val processID = (processID.toLong() % 0b10000) shl 13

    suspend fun generate(): Long {
        mutex.withLock {
            var now = clock.millis()
            if (now < lastTime) { //Can't go backwards
                delay(lastTime - now)
                now = clock.millis()
            } else if (now > lastTime) {
                thisMs = 0
                lastTime = clock.millis()
            } else if (thisMs > 4095) {
                delay(1)
                thisMs = 0
                lastTime = clock.millis()
            }

            return ((now - epoch) shl 22) or workerID or processID or (thisMs++ shl 1)
        }
    }

    fun timeFrom(snowflake: Long): Long = (snowflake shr 22) + epoch
}