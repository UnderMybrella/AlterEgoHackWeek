package org.abimon.hackweek.alterego.functions

import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.entity.TextChannel
import discord4j.core.`object`.util.Snowflake
import discord4j.core.event.domain.message.MessageCreateEvent
import kotlinx.coroutines.*
import org.abimon.hackweek.alterego.*
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.sql.ResultSet
import java.sql.Types
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class AutoChannelSlowMode(alterEgo: AlterEgo) : AlterEgoModule(alterEgo), CoroutineScope by GlobalScope {
    data class SlowModeConfig(
        val channel: Long,
        val period: Int,
        val threshold: Int,
        val slowdown: Int,
        val duration: Int,
        val enabledText: String?,
        val disabledText: String?
    ) {
        constructor(rs: ResultSet) : this(
            rs.getString("channel_id").toULong().toLong(),
            rs.getInt("message_period_ms"),
            rs.getInt("message_threshold"),
            rs.getInt("slowdown"),
            rs.getInt("slowdown_duration"),
            rs.getString("enabled_text"),
            rs.getString("disabled_text")
        )
    }

    companion object {
        const val TABLE_NAME = "slowmode_config"
        fun slowmodeThread(runnable: Runnable): Thread {
            val thread = Thread(runnable, "slowmode-scheduler")
            thread.isDaemon = true
            return thread
        }
    }

    val logger = LoggerFactory.getLogger("AutoChannelSlowMode")

    val channelBuckets: MutableMap<Long, AtomicInteger> = HashMap()
    val channelThresholds: MutableMap<Long, Int> = HashMap()
    val channelPeriods: MutableMap<Long, Int> = HashMap()
    val channelSlowdown: MutableMap<Long, Int> = HashMap()
    val channelSlowModeDuration: MutableMap<Long, Int> = HashMap()
    val channelSlowModeEnabledText: MutableMap<Long, String> = HashMap()
    val channelSlowModeDisabledText: MutableMap<Long, String> = HashMap()
    val channelInLockdown: MutableMap<Long, Boolean> = HashMap()

    val context = Executors.newCachedThreadPool(Companion::slowmodeThread).asCoroutineDispatcher()
    val scheduler = CoroutineReactorScheduler(context = context) //Schedulers.newElastic("slowmode-scheduler", 5)

    override fun register() {
        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .publishOn(scheduler)
            .map(MessageCreateEvent::getMessage)
            .map(Message::getChannelId)
            .flatMap<TextChannel> { id ->
                val atomicInt = channelBuckets[id.asLong()] ?: return@flatMap Mono.empty()
                val count = atomicInt.incrementAndGet()
                val period = channelPeriods.getValue(id.asLong())
                launch(context) {
                    delay(period.toLong())
                    channelBuckets[id.asLong()]?.decrementButNotBelow(0)
                }

                val threshold = channelThresholds.getValue(id.asLong())
                if (count > threshold && channelInLockdown[id.asLong()] != true) {
                    launch(loggerContext) {
                        logger.debug(
                            "{} hit threshold of {} messages/{} seconds",
                            id,
                            threshold,
                            period.toDouble() / 1000.0
                        )
                    }
                    return@flatMap surpassedThreshold(id)
                } else {
                    return@flatMap Mono.empty()
                }
            }
            .subscribe()

        val restorationProject: MutableList<Pair<String, Int>> = ArrayList()
        alterEgo.useStatement("SELECT channel_id, old_slow_duration FROM $TABLE_NAME WHERE old_slow_duration IS NOT NULL;") { statement ->
            statement.resultSet.use { rs ->
                while (rs.next()) restorationProject.add(rs.getString("channel_id") to rs.getInt("old_slow_duration"))
            }
        }
        Flux.fromIterable(restorationProject)
            .publishOn(scheduler)
            .flatMap { (id, duration) ->
                alterEgo.client.getChannelById(Snowflake.of(id))
                    .ofType(TextChannel::class.java)
                    .filter { channel -> channel.rateLimitPerUser == channelSlowdown[channel.id.asLong()] }
                    .doOnNext { channel -> launch(loggerContext) { logger.info("Restoring slow mode for ${channel.name}") } }
                    .flatMap { channel -> channel.edit { spec -> spec.setRateLimitPerUser(duration) } }
            }
            .subscribe()
    }

    fun surpassedThreshold(id: Snowflake): Mono<TextChannel> {
        return alterEgo.client.getChannelById(id)
            .ofType(TextChannel::class.java)
            .doOnNext { channel -> channelInLockdown[channel.id.asLong()] = true }
            .flatMap { channel -> setOldSlowMode(channel, true) }
            .flatMap { globalChannel ->
                val rateLimit = globalChannel.rateLimitPerUser
                globalChannel.edit { spec ->
                    spec.setRateLimitPerUser(channelSlowdown.getValue(id.asLong()))
                    spec.reason = "Spam filter; message count surpassed threshold"
                }
                    .flatMap { channel ->
                        channelSlowModeEnabledText[id.asLong()]?.let(channel::createMessage)?.map { channel }
                            ?: Mono.just(channel)
                    }
                    .delayElement(Duration.ofMillis(channelSlowModeDuration.getValue(id.asLong()).toLong()))
                    .doOnNext { channel -> channelInLockdown[channel.id.asLong()] = false }
                    .flatMap { channel ->
                        logger.trace("Restoring old rate limit of $rateLimit")
                        channel.edit { spec -> spec.setRateLimitPerUser(rateLimit) }
                    }
                    .flatMap { channel -> setOldSlowMode(channel, false) }
                    .flatMap { channel ->
                        channelSlowModeDisabledText[id.asLong()]?.let(channel::createMessage)?.map { channel }
                            ?: Mono.just(channel)
                    }
            }
    }

    fun setOldSlowMode(channel: TextChannel, slowing: Boolean): Mono<TextChannel> =
        wrapMono(context) {
            alterEgo.usePreparedStatement("UPDATE $TABLE_NAME SET old_slow_duration = ? WHERE channel_id = ?;") { prepared ->
                if (slowing) {
                    prepared.setInt(1, channel.rateLimitPerUser)
                } else {
                    prepared.setNull(1, Types.INTEGER)
                }
                prepared.setString(2, channel.id.asString())
                prepared.execute()
            }

            channel
        }

    init {
        alterEgo.useStatement { statement ->
            statement.execute(
                createTableSql(
                    TABLE_NAME,
                    "channel_id VARCHAR(32) NOT NULL PRIMARY KEY",
                    "message_period_ms INT DEFAULT ${10_000} NOT NULL",
                    "message_threshold INT DEFAULT 15 NOT NULL",
                    "slowdown INT DEFAULT 10 NOT NULL",
                    "slowdown_duration INT DEFAULT ${5_000} NOT NULL",
                    "enabled_text VARCHAR(2000)",
                    "disabled_text VARCHAR(2000)",
                    "old_slow_duration INT"
                )
            )
        }

        launch {
            alterEgo.useStatement("SELECT channel_id, message_period_ms, message_threshold, slowdown, slowdown_duration, enabled_text, disabled_text FROM $TABLE_NAME;") { statement ->
                statement.resultSet.use { rs ->
                    while (rs.next()) {
                        val id = rs.getString("channel_id").toULong().toLong()
                        channelBuckets[id] = AtomicInteger(0)
                        channelPeriods[id] = rs.getInt("message_period_ms")
                        channelThresholds[id] = rs.getInt("message_threshold")
                        channelSlowdown[id] = rs.getInt("slowdown")
                        channelSlowModeDuration[id] = rs.getInt("slowdown_duration")
                        channelSlowModeEnabledText[id] = rs.getString("enabled_text")
                        channelSlowModeDisabledText[id] = rs.getString("disabled_text")
                    }
                }
            }
        }
    }
}