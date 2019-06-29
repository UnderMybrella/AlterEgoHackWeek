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
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class AutoChannelSlowMode(alterEgo: AlterEgo) : AlterEgoModule(alterEgo), CoroutineScope by GlobalScope {
    companion object {
        val COMMAND_SLOWDOWN_CHANGE_NAME = "slowdown.change"
        val COMMAND_SLOWDOWN_CHANGE_DEFAULT = "configure slowdown for"

        val COMMAND_SLOWDOWN_REMOVE_NAME = "slowdown.remove"
        val COMMAND_SLOWDOWN_REMOVE_DEFAULT = "remove slowdown from"

        val COMMAND_SLOWDOWN_SHOW_NAME = "slowdown.show"
        val COMMAND_SLOWDOWN_SHOW_DEFAULT = "show slowdown for"

        const val TABLE_NAME = "slowmode_config"
        fun slowmodeThread(runnable: Runnable): Thread {
            val thread = Thread(runnable, "slowmode-scheduler")
            thread.isDaemon = true
            return thread
        }
    }

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

    val logger = LoggerFactory.getLogger("AutoChannelSlowMode")

    val channelBuckets: MutableMap<Long, AtomicInteger> = HashMap()
    val channelThresholds: MutableMap<Long, Int> = HashMap()
    val channelPeriods: MutableMap<Long, Int> = HashMap()
    val channelSlowdown: MutableMap<Long, Int> = HashMap()
    val channelSlowModeDuration: MutableMap<Long, Int> = HashMap()
    val channelSlowModeEnabledText: MutableMap<Long, String> = HashMap()
    val channelSlowModeDisabledText: MutableMap<Long, String> = HashMap()
    val channelInLockdown: MutableMap<Long, Boolean> = HashMap()

    val commandSlowdownConfigure = command(COMMAND_SLOWDOWN_CHANGE_NAME) ?: COMMAND_SLOWDOWN_CHANGE_DEFAULT
    val commandSlowdownRemove = command(COMMAND_SLOWDOWN_REMOVE_NAME) ?: COMMAND_SLOWDOWN_REMOVE_DEFAULT
    val commandSlowdownShow = command(COMMAND_SLOWDOWN_SHOW_NAME) ?: COMMAND_SLOWDOWN_SHOW_DEFAULT

    val context = Executors.newCachedThreadPool(Companion::slowmodeThread).asCoroutineDispatcher()
    val scheduler = CoroutineReactorScheduler(context = context) //Schedulers.newElastic("slowmode-scheduler", 5)

    fun registerConfiguration() {
        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .filterWhen(alterEgo::messageRequiresManageRolesPermission)
            .flatMap { msg -> alterEgo.stripMessagePrefix(msg) }
            .flatMap { msg ->
                alterEgo.stripCommandFromMessage(
                    msg,
                    COMMAND_SLOWDOWN_CHANGE_NAME,
                    commandSlowdownConfigure
                )
            }
            .flatMap { msg ->
                val parameters = msg.content.orElse("").parameters(3)

                if (parameters.size < 3 || !parameters[1].let { str ->
                        str.equals("current", true)
                                || str.equals("threshold", true)
                                || str.equals("period", true)
                                || str.equals("limit", true)
                                || str.equals("duration", true)
                                || str.equals("enabled_text", true)
                                || str.equals("disabled_text", true)
                    })
                    return@flatMap msg.channel.flatMap { channel ->
                        channel.createMessage(
                            "Invalid parameters supplied!\n" +
                                    "Syntax: ${alterEgo.prefixFor(channel)}$commandSlowdownConfigure [channel] [current | threshold | period | limit | duration | enabled_text | disabled_text] [value]"
                        )
                    }

                msg.guild.flatMap { guild ->
                    guild.findChannelByIdentifier(parameters[0]).flatMap { targetChannel ->
                        val id = targetChannel.id.asLong()
                        val strID = targetChannel.id.asString()
                        wrapMono { ensureExists(id) }
                            .flatMap local@{
                                val key = parameters[1].toLowerCase()
                                val value = parameters[2]

                                when (key) {
                                    "current" -> return@local Mono.fromCallable {
                                        channelBuckets[id]?.set(
                                            value.toIntOrNull() ?: 0
                                        )
                                    }
                                        .then()
                                    "threshold" -> return@local Mono.justOrEmpty<Int>(value.toIntOrNull())
                                        .map { threshold ->
                                            channelThresholds[id] = threshold
                                            threshold
                                        }
                                        .flatMap { threshold ->
                                            wrapMono(context) {
                                                alterEgo.usePreparedStatement("UPDATE $TABLE_NAME SET message_threshold = ? WHERE channel_id = ?;") { prepared ->
                                                    prepared.setInt(1, threshold)
                                                    prepared.setString(2, strID)
                                                    prepared.execute()
                                                }
                                            }
                                        }
                                        .then()

                                    "period" -> return@local Mono.justOrEmpty<Int>(value.toIntOrNull())
                                        .map { period ->
                                            channelPeriods[id] = period
                                            period
                                        }
                                        .flatMap { period ->
                                            wrapMono(context) {
                                                alterEgo.usePreparedStatement("UPDATE $TABLE_NAME SET message_period_ms = ? WHERE channel_id = ?;") { prepared ->
                                                    prepared.setInt(1, period)
                                                    prepared.setString(2, strID)
                                                    prepared.execute()
                                                }
                                            }
                                        }
                                        .then()

                                    "limit" -> return@local Mono.justOrEmpty<Int>(value.toIntOrNull())
                                        .map { limit ->
                                            channelSlowdown[id] = limit
                                            limit
                                        }
                                        .flatMap { limit ->
                                            wrapMono(context) {
                                                alterEgo.usePreparedStatement("UPDATE $TABLE_NAME SET slowdown = ? WHERE channel_id = ?;") { prepared ->
                                                    prepared.setInt(1, limit)
                                                    prepared.setString(2, strID)
                                                    prepared.execute()
                                                }
                                            }
                                        }
                                        .then()

                                    "duration" -> return@local Mono.justOrEmpty<Int>(value.toIntOrNull())
                                        .map { duration ->
                                            channelSlowModeDuration[id] = duration
                                            duration
                                        }
                                        .flatMap { duration ->
                                            wrapMono(context) {
                                                alterEgo.usePreparedStatement("UPDATE $TABLE_NAME SET slowdown_duration = ? WHERE channel_id = ?;") { prepared ->
                                                    prepared.setInt(1, duration)
                                                    prepared.setString(2, strID)
                                                    prepared.execute()
                                                }
                                            }
                                        }
                                        .then()

                                    "enabled_text" -> return@local Mono.just(Optional.ofNullable(value.takeUnless { str ->
                                        str.equals(
                                            "null",
                                            true
                                        )
                                    }))
                                        .map { enabled ->
                                            if (enabled.isPresent) {
                                                channelSlowModeEnabledText[id] = enabled.get()
                                            } else {
                                                channelSlowModeEnabledText.remove(id)
                                            }

                                            enabled
                                        }
                                        .flatMap { enabled ->
                                            wrapMono(context) {
                                                alterEgo.usePreparedStatement("UPDATE $TABLE_NAME SET enabled_text = ? WHERE channel_id = ?;") { prepared ->
                                                    prepared.setString(1, enabled.orElse(null))
                                                    prepared.setString(2, strID)
                                                    prepared.execute()
                                                }
                                            }
                                        }
                                        .then()

                                    "disabled_text" -> return@local Mono.just(Optional.ofNullable(value.takeUnless { str ->
                                        str.equals(
                                            "null",
                                            true
                                        )
                                    }))
                                        .map { disabled ->
                                            if (disabled.isPresent) {
                                                channelSlowModeDisabledText[id] = disabled.get()
                                            } else {
                                                channelSlowModeDisabledText.remove(id)
                                            }

                                            disabled
                                        }
                                        .flatMap { disabled ->
                                            wrapMono(context) {
                                                alterEgo.usePreparedStatement("UPDATE $TABLE_NAME SET disabled_text = ? WHERE channel_id = ?;") { prepared ->
                                                    prepared.setString(1, disabled.orElse(null))
                                                    prepared.setString(2, strID)
                                                    prepared.execute()
                                                }
                                            }
                                        }
                                        .then()
                                    else -> return@local Mono.empty<Void>()
                                }
                            }
                            .then(msg.channel)
                            .flatMap { channel ->
                                channel.createMessage("I've updated the slowing mechanics for ${targetChannel.mention}")
                            }
                    }
                }
            }
            .subscribe()

        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .filterWhen(alterEgo::messageRequiresManageRolesPermission)
            .flatMap { msg -> alterEgo.stripMessagePrefix(msg) }
            .flatMap { msg ->
                alterEgo.stripCommandFromMessage(
                    msg,
                    COMMAND_SLOWDOWN_REMOVE_NAME,
                    commandSlowdownRemove
                )
            }
            .flatMap { msg ->
                val channelKey =
                    msg.content.orElse("").takeIf(String::isNotBlank)
                        ?: return@flatMap msg.channel.flatMap { channel ->
                            channel.createMessage(
                                "Invalid parameters supplied!\n" +
                                        "Syntax: ${alterEgo.prefixFor(channel)}$commandSlowdownRemove [channel]"
                            )
                        }

                msg.guild.flatMap { guild ->
                    guild.findChannelByIdentifier(channelKey).flatMap { targetChannel ->
                        val id = targetChannel.id.asLong()

                        channelBuckets.remove(id)
                        channelThresholds.remove(id)
                        channelPeriods.remove(id)
                        channelSlowdown.remove(id)
                        channelSlowModeDuration.remove(id)
                        channelSlowModeEnabledText.remove(id)
                        channelSlowModeDisabledText.remove(id)
                        channelInLockdown.remove(id)

                        wrapMono {
                            alterEgo.usePreparedStatement("DELETE FROM $TABLE_NAME WHERE channel_id = ?;") { prepared ->
                                prepared.setString(1, targetChannel.id.asString())
                                prepared.execute()
                            }
                        }.flatMap {
                            msg.channel.flatMap { channel ->
                                channel.createMessage("Removed the slow mechanic from ${targetChannel.mention}")
                            }
                        }
                    }
                }
            }
            .subscribe()

        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .filterWhen(alterEgo::messageRequiresManageRolesPermission)
            .flatMap { msg -> alterEgo.stripMessagePrefix(msg) }
            .flatMap { msg ->
                alterEgo.stripCommandFromMessage(
                    msg,
                    COMMAND_SLOWDOWN_SHOW_NAME,
                    commandSlowdownShow
                )
            }
            .flatMap { msg ->
                val channelKey =
                    msg.content.orElse("").takeIf(String::isNotBlank)
                        ?: return@flatMap msg.channel.flatMap { channel ->
                            channel.createMessage(
                                "Invalid parameters supplied!\n" +
                                        "Syntax: ${alterEgo.prefixFor(channel)}$commandSlowdownShow [channel]"
                            )
                        }

                msg.guild.flatMap { guild ->
                    guild.findChannelByIdentifier(channelKey).flatMap { targetChannel ->
                        msg.channel.flatMap { channel ->
                            channel.createEmbed { spec ->
                                val id = targetChannel.id.asLong()

                                spec.setTitle("Slowdown for ${targetChannel.name}")
                                spec.setDescription(buildString {
                                    val bucket = channelBuckets[id]
                                    if (bucket == null) {
                                        appendln("This channel does not seem to have a slowdown configured for it")
                                    } else {
                                        val count = bucket.get()
                                        appendln("This channel has had $count messages sent over the last ${secondsFormatter.format(channelPeriods.getValue(id).toDouble() / 1000.0)} seconds")
                                        appendln("It takes ${channelThresholds[id]} messages sent to lock down the channel, at which point users can only send one message every ${channelSlowdown[id]} seconds (This lasts for ${secondsFormatter.format(channelSlowModeDuration.getValue(id).toDouble() / 1000.0)} seconds)")
                                    }
                                })

                                channelSlowModeEnabledText[id]?.apply { spec.addField("Slowmode Enabled Text", this, false) }
                                channelSlowModeDisabledText[id]?.apply { spec.addField("Slowmode Disabled Text", this, false) }
                            }
                        }
                    }
                }
            }
            .subscribe()
    }

    override fun register() {
        registerConfiguration()

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

    fun ensureExists(channel: Long) {
        if (channel !in channelBuckets) {
            val period = 10_000
            val threshold = 15
            val slowdown = 10
            val duration = 5_000

            channelBuckets[channel] = AtomicInteger(0)
            channelPeriods[channel] = period
            channelThresholds[channel] = threshold
            channelSlowdown[channel] = slowdown
            channelSlowModeDuration[channel] = duration

            alterEgo.usePreparedStatement("INSERT INTO $TABLE_NAME (channel_id, message_period_ms, message_threshold, slowdown, slowdown_duration) VALUES (?, ?, ?, ?, ?);") { prepared ->
                prepared.setString(1, channel.toUString())
                prepared.setInt(2, period)
                prepared.setInt(3, threshold)
                prepared.setInt(4, slowdown)
                prepared.setInt(5, duration)
                prepared.execute()
            }
        }
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
                        rs.getString("enabled_text")?.apply { channelSlowModeEnabledText[id] = this }
                        rs.getString("disabled_text")?.apply { channelSlowModeDisabledText[id] = this }
                    }
                }
            }
        }
    }
}