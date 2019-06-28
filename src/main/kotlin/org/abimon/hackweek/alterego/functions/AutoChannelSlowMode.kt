package org.abimon.hackweek.alterego.functions

import discord4j.core.DiscordClient
import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.entity.TextChannel
import discord4j.core.`object`.util.Snowflake
import discord4j.core.event.domain.message.MessageCreateEvent
import kotlinx.coroutines.*
import org.abimon.hackweek.alterego.decrementButNotBelow
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class AutoChannelSlowMode : CoroutineScope by GlobalScope {
    val logger = LoggerFactory.getLogger("AutoChannelSlowMode")
    val channelBuckets: MutableMap<Long, AtomicInteger> = hashMapOf(594113266909970432L to AtomicInteger(0))
    val channelThresholds: MutableMap<Long, Int> = hashMapOf(594113266909970432L to 10)
    val context = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

    fun register(client: DiscordClient) {
        client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .map(Message::getChannelId)
            .flatMap<TextChannel> { id ->
                val atomicInt = channelBuckets[id.asLong()] ?: return@flatMap Mono.empty()
                val count = atomicInt.incrementAndGet()
                launch(context) {
                    delay(10_000)
                    channelBuckets[id.asLong()]?.decrementButNotBelow(0)
                }

                if (count > channelThresholds.getValue(id.asLong())) {
                    channelBuckets[id.asLong()]?.set(0)
                    return@flatMap surpassedThreshold(client, id)
                } else {
                    return@flatMap Mono.empty()
                }
            }
            .subscribe()
    }

    fun surpassedThreshold(client: DiscordClient, id: Snowflake): Mono<TextChannel> {
        logger.debug("$id hit threshold of ${channelThresholds[id.asLong()]} messages/10 seconds")
        return client.getChannelById(id)
            .ofType(TextChannel::class.java)
            .flatMap { globalChannel ->
                val rateLimit = globalChannel.rateLimitPerUser
                globalChannel.edit { spec ->
                    spec.setRateLimitPerUser(120)
                    spec.reason = "Spam filter; message count surpassed threshold"
                }
                    .delayElement(Duration.ofSeconds(10))
                    .flatMap { channel ->
                        logger.trace("Restoring old rate limit of $rateLimit")
                        channel.edit { spec -> spec.setRateLimitPerUser(rateLimit) }
                    }
            }
    }
}