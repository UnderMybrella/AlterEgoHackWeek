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
import kotlinx.coroutines.*
import org.abimon.hackweek.alterego.functions.AutoChannelSlowMode
import org.abimon.hackweek.alterego.functions.MetaModule
import org.abimon.hackweek.alterego.stores.GrandCentralService
import org.abimon.hackweek.alterego.stores.GrandJdbcStation
import org.abimon.hackweek.alterego.stores.MessageTableBean
import org.intellij.lang.annotations.Language
import reactor.core.publisher.Mono
import reactor.core.publisher.switchIfEmpty
import java.io.File
import java.sql.*
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.Executors
import kotlin.collections.HashMap


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

        fun alterThread(runnable: Runnable): Thread {
            val thread = Thread(runnable, "alter-scheduler")
            thread.isDaemon = true
            return thread
        }
    }

    val context = Executors.newCachedThreadPool(Companion::alterThread).asCoroutineDispatcher()
    val scheduler = CoroutineReactorScheduler(context = context)

    val hikariDataSource: HikariDataSource
    val mapping: MappingStoreService
    val client: DiscordClient
    val snowstorm: SnowflakeGenerator = SnowflakeGenerator(
        config["workerID"]?.toString()?.toIntOrNull() ?: 0,
        config["processID"]?.toString()?.toIntOrNull() ?: 0
    )
    val station: GrandJdbcStation

    private val guildPrefixes: MutableMap<Long, String> = HashMap()

    public inline fun <R> useConnection(block: (Connection) -> R): R = hikariDataSource.connection.use(block)
    public inline fun <R> useStatement(block: (Statement) -> R): R =
        hikariDataSource.connection.use { it.createStatement().use(block) }

    public inline fun <R> useStatement(@Language("SQL") sql: String, block: (Statement) -> R): R =
        hikariDataSource.connection.use { it.createStatement().use { stmt -> stmt.execute(sql); block(stmt) } }

    public inline fun <R> usePreparedStatement(@Language("SQL") sql: String, block: (PreparedStatement) -> R): R =
        hikariDataSource.connection.use { it.prepareStatement(sql).use(block) }

    public inline fun execute(@Language("SQL") sql: String) =
        hikariDataSource.connection.use { it.createStatement().use { stmt -> stmt.execute(sql) } }

    val defaultPrefix = config["defaultPrefix"]?.toString() ?: "~|"

    fun prefixFor(guildID: String) = prefixFor(guildID.toULong().toLong())
    fun prefixFor(guildID: Long): String = guildPrefixes[guildID] ?: defaultPrefix

    fun setPrefixFor(guildID: String, prefix: String) = setPrefixFor(guildID.toULong().toLong(), prefix)
    fun setPrefixFor(guildID: Long, prefix: String): Job {
        guildPrefixes[guildID] = prefix

        return GlobalScope.launch(context) {
            val exists = usePreparedStatement("SELECT prefix FROM prefixes WHERE guild_id = ?;") { prepared ->
                prepared.setString(1, guildID.toUString())
                prepared.execute()

                prepared.resultSet.use(ResultSet::next)
            }

            if (exists) {
                usePreparedStatement("UPDATE prefixes SET prefix = ? WHERE guild_id = ?;") { prepared ->
                    prepared.setString(1, prefix)
                    prepared.setString(2, guildID.toUString())

                    prepared.execute()
                }
            } else {
                try {
                    usePreparedStatement("INSERT INTO prefixes (guild_id, prefix) VALUES (?, ?);") { prepared ->
                        prepared.setString(1, guildID.toUString())
                        prepared.setString(2, prefix)

                        prepared.execute()
                    }
                } catch (sql: SQLException) {
                    cancel(CancellationException("An error occurred when inserting a prefix into the prefixes table", sql))
                }
            }
        }
    }

//    fun stripMessagePrefix(msg: Message): Mono<Message> =
//        msg.guild.flatMap { guild ->
//            wrapMono(context) {
//                val prefix =
//
//                if (msg.content.map { str -> str.startsWith(prefix) }.orElse(false)) {
//                    msg.bean.let { bean ->
//                        bean.content = bean.content?.replaceFirst(prefix, "")
//                    }
//                }
//            }
//        }

    init {
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = config["jdbcUrl"] as String
        hikariConfig.addDataSourceProperty("cachePrepStatements", "true")
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250")
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")

        hikariDataSource = HikariDataSource(hikariConfig)
        execute("CREATE TABLE IF NOT EXISTS prefixes (guild_id VARCHAR(32) NOT NULL PRIMARY KEY, prefix VARCHAR(64) DEFAULT $defaultPrefix NOT NULL);")

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

        AutoChannelSlowMode(this).register()
        MetaModule(this).register()

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