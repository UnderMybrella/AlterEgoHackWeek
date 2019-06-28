package org.abimon.hackweek.alterego

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import discord4j.core.DiscordClient
import discord4j.core.DiscordClientBuilder
import discord4j.core.`object`.data.stored.MessageBean
import discord4j.core.`object`.data.stored.UserBean
import discord4j.core.`object`.entity.*
import discord4j.core.`object`.util.Permission
import discord4j.core.`object`.util.Snowflake
import discord4j.core.event.domain.guild.GuildCreateEvent
import discord4j.core.event.domain.lifecycle.ReadyEvent
import discord4j.core.event.domain.lifecycle.ResumeEvent
import discord4j.rest.request.RouterOptions
import discord4j.rest.response.ResponseFunction
import discord4j.store.api.mapping.MappingStoreService
import discord4j.store.jdk.JdkStoreService
import kotlinx.coroutines.*
import org.abimon.hackweek.alterego.functions.AutoChannelSlowMode
import org.abimon.hackweek.alterego.functions.MetaModule
import org.abimon.hackweek.alterego.functions.RolesModule
import org.abimon.hackweek.alterego.functions.UserThoroughfare
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
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.HashMap
import kotlin.concurrent.withLock


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
    val hikariLock = ReentrantLock()
    val mapping: MappingStoreService
    val client: DiscordClient
    val snowstorm: SnowflakeGenerator = SnowflakeGenerator(
        config["workerID"]?.toString()?.toIntOrNull() ?: 0,
        config["processID"]?.toString()?.toIntOrNull() ?: 0
    )
    val station: GrandJdbcStation

    private val guildPrefixes: MutableMap<Long, String> = HashMap()
    private val guildCommandTriggers: MutableMap<Long, MutableMap<String, String>> = HashMap()

    public inline fun connection(): Connection {
        //println(Thread.currentThread().stackTrace[1].toString())
        return hikariLock.withLock { hikariDataSource.connection }
    }

    public inline fun <R> useConnection(block: (Connection) -> R): R = connection().use(block)
    public inline fun <R> useStatement(block: (Statement) -> R): R =
        connection().use { it.createStatement().use(block) }

    public inline fun <R> useStatement(@Language("SQL") sql: String, block: (Statement) -> R): R =
        connection().use { it.createStatement().use { stmt -> stmt.execute(sql); block(stmt) } }

    public inline fun <R> usePreparedStatement(@Language("SQL") sql: String, block: (PreparedStatement) -> R): R =
        connection().use { it.prepareStatement(sql).use(block) }

    public inline fun execute(@Language("SQL") sql: String) =
        connection().use { it.createStatement().use { stmt -> stmt.execute(sql) } }

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
                    cancel(
                        CancellationException(
                            "An error occurred when inserting a prefix into the prefixes table",
                            sql
                        )
                    )
                }
            }
        }
    }

    fun commandFor(guildID: String, commandName: String) = commandFor(guildID.toULong().toLong(), commandName)
    fun commandFor(guildID: Long, commandName: String): String? = guildCommandTriggers[guildID]?.get(commandName)

    fun setCommandFor(guildID: String, commandName: String, commandTrigger: String) =
        setCommandFor(guildID.toULong().toLong(), commandName, commandTrigger)

    fun setCommandFor(guildID: Long, commandName: String, commandTrigger: String?): Job {
        guildCommandTriggers.compute(guildID) { _, map ->
            (map ?: HashMap()).apply {
                if (commandTrigger == null)
                    remove(commandName)
                else
                    put(commandName, commandTrigger)
            }
        }

        return GlobalScope.launch(context) {
            val id =
                usePreparedStatement("SELECT id FROM command_triggers WHERE guild_id = ? AND command_name = ?;") { prepared ->
                    prepared.setString(1, guildID.toUString())
                    prepared.setString(2, commandName)
                    prepared.execute()

                    prepared.resultSet.use { rs -> rs.takeIf(ResultSet::next)?.getString("id") }
                }

            if (id != null) {
                usePreparedStatement("UPDATE command_triggers SET command_trigger = ? WHERE id = ?;") { prepared ->
                    prepared.setString(1, commandTrigger)
                    prepared.setString(2, id)

                    prepared.execute()
                }
            } else {
                try {
                    usePreparedStatement("INSERT INTO command_triggers (id, guild_id, command_name, command_trigger) VALUES (?, ?, ?, ?);") { prepared ->
                        prepared.setString(1, snowstorm.generate().toString())
                        prepared.setString(2, guildID.toUString())
                        prepared.setString(3, commandName)
                        prepared.setString(4, commandTrigger)

                        prepared.execute()
                    }
                } catch (sql: SQLException) {
                    cancel(
                        CancellationException(
                            "An error occurred when inserting a command into the commands table",
                            sql
                        )
                    )
                }
            }
        }
    }

    fun messageSentByUser(msg: Message): Boolean = !msg.author.map(User::isBot).orElse(true)
    fun messageSentByBrella(msg: Message): Boolean =
        !msg.author.map { user -> user.id.asLong() == 149031328132628480L }.orElse(false)

    fun messageRequiresManageServerPermission(msg: Message): Mono<Boolean> =
        msg.authorAsMember.flatMap(Member::getBasePermissions)
            .map { perms -> perms.contains(Permission.ADMINISTRATOR) || perms.contains(Permission.MANAGE_GUILD) }

    fun messageRequiresManageRolesPermission(msg: Message): Mono<Boolean> =
        msg.authorAsMember.flatMap(Member::getBasePermissions)
            .map { perms -> perms.contains(Permission.ADMINISTRATOR) || perms.contains(Permission.MANAGE_ROLES) }

    fun stripMessagePrefix(msg: Message): Mono<Message> =
        msg.guild.flatMap { guild ->
            msg.client.self.flatMap { self ->
                wrapMono(context) {
                    val prefix = prefixFor(guild.id.asLong())

                    if (msg.content.map { str -> str.startsWith(prefix) }.orElse(false)) {
                        val copy = MessageBean(msg.bean)
                        copy.content = copy.content?.replaceFirst(prefix, "")
                        Message(msg.serviceMediator, copy)
                    } else if (msg.content.map { str ->
                            str.startsWith("<@${self.id.asString()}> ") || str.startsWith("<@!${self.id.asString()}> ")
                        }.orElse(false)) {
                        val copy = MessageBean(msg.bean)
                        copy.content = copy.content?.substringAfter("> ")

                        Message(msg.serviceMediator, copy)
                    } else {
                        null
                    }
                }
            }
        }

    fun stripCommandFromMessage(msg: Message, commandName: String, default: String): Mono<Message> =
        msg.guild.flatMap { guild ->
            wrapMono(context) {
                val command = commandFor(guild.id.asLong(), commandName) ?: default

                if (msg.content.map { str -> str.startsWith(command) }.orElse(false)) {
                    val copy = MessageBean(msg.bean)
                    copy.content = copy.content?.replaceFirst(command, "")
                    Message(msg.serviceMediator, copy)
                } else if (msg.content.map { str -> str.startsWith(default) }.orElse(false)) {
                    val copy = MessageBean(msg.bean)
                    copy.content = copy.content?.replaceFirst(default, "")
                    Message(msg.serviceMediator, copy)
                } else {
                    null
                }
            }
        }

    init {
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = config["jdbcUrl"] as String
        hikariConfig.addDataSourceProperty("cachePrepStatements", "true")
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250")
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
        hikariConfig.leakDetectionThreshold = 60 * 1000
        hikariConfig.maximumPoolSize = 40
        hikariConfig.connectionTimeout = 10_000

        hikariDataSource = HikariDataSource(hikariConfig)
        execute("CREATE TABLE IF NOT EXISTS prefixes (guild_id VARCHAR(32) NOT NULL PRIMARY KEY, prefix VARCHAR(64) DEFAULT '$defaultPrefix' NOT NULL);")
        execute("CREATE TABLE IF NOT EXISTS command_triggers (id VARCHAR(32) NOT NULL PRIMARY KEY, guild_id VARCHAR(32) NOT NULL, command_name VARCHAR(128) NOT NULL, command_trigger VARCHAR(256));")

        GlobalScope.launch(context) {
            useStatement("SELECT guild_id, prefix FROM prefixes;") { statement ->
                statement.resultSet.use { rs ->
                    while (rs.next()) guildPrefixes[rs.getString("guild_id").toULong().toLong()] =
                        rs.getString("prefix")
                }
            }

            useStatement("SELECT guild_id, command_name, command_trigger FROM command_triggers;") { statement ->
                statement.resultSet.use { rs ->
                    while (rs.next())
                        guildCommandTriggers.compute(rs.getString("guild_id").toULong().toLong()) { _, map ->
                            (map ?: HashMap()).apply {
                                rs.getString("command_trigger")?.let { trigger ->
                                    put(rs.getString("command_name"), trigger)
                                }
                            }
                        }
                }
            }
        }

        station = GrandJdbcStation(config["stationName"]?.toString() ?: "gcs", this, snowstorm)

        mapping = MappingStoreService.create()
            .setMappings(GrandCentralService(station), MessageBean::class.java, UserBean::class.java)
            .setFallback(JdkStoreService())

        client = DiscordClientBuilder(config["token"] as String)
            .setStoreService(mapping)
            .setRouterOptions(RouterOptions.builder().onClientResponse(ResponseFunction.emptyIfNotFound()).build())
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
        UserThoroughfare(this).register()
        RolesModule(this).register()

        println()

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