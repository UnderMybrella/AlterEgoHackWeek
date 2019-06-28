package org.abimon.hackweek.alterego

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import discord4j.core.DiscordClient
import discord4j.core.DiscordClientBuilder
import discord4j.core.`object`.data.stored.MessageBean
import discord4j.core.`object`.data.stored.UserBean
import discord4j.core.`object`.entity.Guild
import discord4j.core.`object`.entity.GuildMessageChannel
import discord4j.core.`object`.util.Snowflake
import discord4j.core.event.domain.guild.GuildCreateEvent
import discord4j.core.event.domain.lifecycle.ReadyEvent
import discord4j.core.event.domain.lifecycle.ResumeEvent
import discord4j.store.api.mapping.MappingStoreService
import discord4j.store.jdk.JdkStoreService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.abimon.hackweek.alterego.stores.GrandCentralService
import org.abimon.hackweek.alterego.stores.GrandJdbcStation
import org.abimon.hackweek.alterego.stores.MessageTableBean
import reactor.core.publisher.Mono
import reactor.core.publisher.switchIfEmpty
import java.io.File
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*


@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class AlterEgo(val config: Properties) {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val configFile = File(args.getOrNull(0) ?: "config.properties")

            if (!configFile.exists())
                error("Error: $configFile does not exist!")
            if (!configFile.isFile)
                error("Error; $configFile is not a file")

            val config = Properties()
            configFile.inputStream().use(config::load)

            AlterEgo(config)
        }
    }

    val hikariDataSource: HikariDataSource
    val mapping: MappingStoreService
    val client: DiscordClient
    val snowstorm: SnowflakeGenerator = SnowflakeGenerator(
        config["workerID"]?.toString()?.toIntOrNull() ?: 0,
        config["processID"]?.toString()?.toIntOrNull() ?: 0
    )
    val station: GrandJdbcStation

    init {
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = config["jdbcUrl"] as String
        hikariConfig.addDataSourceProperty("cachePrepStatements", "true")
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250")
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")

        hikariDataSource = HikariDataSource(hikariConfig)
        station = GrandJdbcStation(config["stationName"]?.toString() ?: "gcs", hikariDataSource, snowstorm)

        mapping = MappingStoreService.create()
            .setMappings(GrandCentralService(station), MessageBean::class.java, UserBean::class.java)
            .setFallback(JdkStoreService())

        client = DiscordClientBuilder(config["token"] as String)
            .setStoreService(mapping)
            .build()

        client.eventDispatcher.on(ReadyEvent::class.java)
            .flatMap { event -> event.client.self }
            .delayElements(Duration.ofSeconds(10))
            .flatMap { event -> event.client.self }
            .subscribe()

        client.eventDispatcher.on(ResumeEvent::class.java)
            .doOnNext { println("Resumed") }
            .flatMap { resume -> resume.client.guilds }
            .flatMap(this::resumeGuild)
            .subscribe()

        client.eventDispatcher.on(GuildCreateEvent::class.java)
            .flatMap { event -> resumeGuild(event.guild) }
            .subscribe()

        client.login().block()
    }

    fun resumeGuild(guild: Guild) = guild.channels
        .ofType(GuildMessageChannel::class.java)
        .flatMap { channel ->
            wrapMono { station.lastMessageIn(channel.id.asString()) }
                .map(MessageTableBean::id)
                .switchIfEmpty {
                    channel.lastMessageId.map { id ->
                        Mono.just(discordSnowflakeForTime(id.timestamp.minus(1, ChronoUnit.HOURS)))
                    }.orElse(Mono.empty())
                }.flatMapMany { id -> channel.getMessagesAfter(Snowflake.of(id)) }
                .flatMap { msg -> wrapMono { station.save(msg.bean) } }
        }
}