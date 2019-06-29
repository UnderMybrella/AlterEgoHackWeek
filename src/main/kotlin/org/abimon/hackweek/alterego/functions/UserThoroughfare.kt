package org.abimon.hackweek.alterego.functions

import discord4j.core.DiscordClient
import discord4j.core.`object`.entity.*
import discord4j.core.`object`.reaction.ReactionEmoji
import discord4j.core.`object`.util.Snowflake
import discord4j.core.event.domain.guild.*
import discord4j.core.event.domain.lifecycle.ResumeEvent
import discord4j.core.event.domain.message.ReactionAddEvent
import discord4j.core.event.domain.role.RoleDeleteEvent
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import org.abimon.hackweek.alterego.AlterEgo
import org.abimon.hackweek.alterego.CoroutineReactorScheduler
import org.abimon.hackweek.alterego.createTableSql
import org.abimon.hackweek.alterego.requests.AddRolesRequest
import org.abimon.hackweek.alterego.requests.ClientRequest
import org.abimon.hackweek.alterego.wrapMono
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.sql.ResultSet
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executors
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

@ExperimentalUnsignedTypes
@ExperimentalCoroutinesApi
class UserThoroughfare(alterEgo: AlterEgo) : AlterEgoModule(alterEgo) {
    companion object {
        fun joinThread(runnable: Runnable): Thread {
            val thread = Thread(runnable, "user-join-scheduler")
            thread.isDaemon = true
            return thread
        }
    }

    data class SafetyNet(
        val guildID: String,
        val userInfoChannelID: String?,
        val safetyNetEnabled: Boolean,
        val safetyNetDelay: Int,
        val safetyNetAllowEmoji: ReactionEmoji?,
        val safetyNetDenyEmoji: ReactionEmoji?
    ) {
        companion object {
            fun reactionEmojiOf(str: String): ReactionEmoji {
                val components = str.split('|')
                if (components.size == 1) {
                    return ReactionEmoji.unicode(components[0])
                } else {
                    return ReactionEmoji.custom(
                        Snowflake.of(components[1]),
                        components[0],
                        components.getOrNull(2)?.toBoolean() ?: false
                    )
                }
            }
        }

        constructor(rs: ResultSet) : this(
            rs.getString("guild_id"),
            rs.getString("user_info_channel_id"),
            rs.getBoolean("safety_net_enabled"),
            rs.getInt("safety_net_delay"),
            rs.getString("safety_net_allow_emoji")?.let(Companion::reactionEmojiOf),
            rs.getString("safety_net_deny_emoji")?.let(Companion::reactionEmojiOf)
        )
    }
    data class UserGreeting(
        val guildID: String,
        val greetingChannel: String,
        val greeting: String,
        val newbieRole: String?
    ) {
        constructor(rs: ResultSet) : this(
            rs.getString("guild_id"),
            rs.getString("greetings_channel"),
            rs.getString("greeting"),
            rs.getString("newbie_role")
        )
    }

    val SAFETY_NET_TABLE_NAME = "safety_net"
    val SAFETY_NET_INFO_TABLE_NAME = "safety_net_info"
    val SAFETY_NET_ROLES_LIST_TABLE_NAME = "safety_net_roles"

    val GREETINGS_TABLE_NAME = "greetings"

    val utc = Clock.systemUTC()

    val safetyNets: MutableMap<Long, SafetyNet> = HashMap()
    val userGreetings: MutableMap<Long, UserGreeting> = HashMap()

    val joinEventContext = Executors.newCachedThreadPool(Companion::joinThread).asCoroutineDispatcher()
    val joinEventScheduler = CoroutineReactorScheduler(context = joinEventContext)
    val roleUpdateContext =
        Executors.newSingleThreadExecutor(AlterEgoModule.Companion::newDefaultThread).asCoroutineDispatcher()

    val waitingForRoles: ConcurrentMap<Long, ClientRequest<Void>> = ConcurrentHashMap()

    override fun register() {
        registerOnJoin()

        alterEgo.client.eventDispatcher.on(ReactionAddEvent::class.java)
            .filter { event -> event.messageId.asLong() in waitingForRoles }
            .filterWhen { event ->
                event.message
                    .filterWhen(alterEgo::messageRequiresManageRolesPermission)
                    .map(alterEgo::messageSentByUser)
            }
            .flatMap<Void> { event ->
                val safetyNet = safetyNets[event.guildId.get().asLong()] ?: return@flatMap Mono.empty()
                val request = waitingForRoles.remove(event.messageId.asLong())
                    ?: return@flatMap Mono.empty() //Guess I'll die then :/
                if (event.emoji == safetyNet.safetyNetAllowEmoji) {
                    request.fulfill(event.client)
                        .then(event.message.flatMap(Message::removeAllReactions))
                } else {
                    event.message.flatMap(Message::removeAllReactions)
                }
            }
            .subscribe()

        registerLeaveUpdates()
        registerRoleUpdates()

        alterEgo.useStatement("SELECT guild_id, user_info_channel_id, safety_net_enabled, safety_net_delay, safety_net_allow_emoji, safety_net_deny_emoji FROM $SAFETY_NET_TABLE_NAME;") { statement ->
            statement.resultSet.use { rs ->
                while (rs.next()) {
                    val safetyNet = SafetyNet(rs)
                    safetyNets[safetyNet.guildID.toULong().toLong()] = safetyNet
                }
            }
        }

        alterEgo.useStatement("SELECT guild_id, greetings_channel, greeting, newbie_role FROM $GREETINGS_TABLE_NAME;") { statement ->
            statement.resultSet.use { rs ->
                while (rs.next()) {
                    val greeting = UserGreeting(rs)
                    userGreetings[greeting.guildID.toULong().toLong()] = greeting
                }
            }
        }
    }

    fun registerOnJoin() {
        alterEgo.client.eventDispatcher.on(MemberJoinEvent::class.java)
            .publishOn(joinEventScheduler)
            .flatMap<Void> ignore@{ event ->
                val safetyNet = safetyNets[event.guildId.asLong()]
                val member = event.member
                if (safetyNet?.safetyNetEnabled != true)
                    return@ignore Mono.empty()

                wrapMono(roleUpdateContext) {
                    alterEgo.usePreparedStatement("SELECT role_id FROM $SAFETY_NET_ROLES_LIST_TABLE_NAME WHERE guild_id = ? AND user_id = ?;") { prepared ->
                        prepared.setString(1, event.guildId.asString())
                        prepared.setString(2, member.id.asString())
                        prepared.execute()

                        prepared.resultSet.use { rs ->
                            val results: MutableList<String> = ArrayList()
                            while (rs.next()) results.add(rs.getString("role_id"))
                            results
                        }
                    }
                }.flatMap<Void> { oldRoles ->
                    if (safetyNet.userInfoChannelID != null) {
                        return@flatMap event.guild.flatMap { guild -> guild.getChannelById(Snowflake.of(safetyNet.userInfoChannelID)) }
                            .ofType(GuildMessageChannel::class.java)
                            .flatMap { channel ->
                                wrapMono(roleUpdateContext) {
                                    alterEgo.usePreparedStatement("SELECT last_seen FROM $SAFETY_NET_INFO_TABLE_NAME WHERE guild_id = ? AND user_id = ?;") { prepared ->
                                        prepared.setString(1, channel.guildId.asString())
                                        prepared.setString(2, member.id.asString())
                                        prepared.execute()

                                        Optional.ofNullable(prepared.resultSet.use { rs ->
                                            rs.takeIf(ResultSet::next)?.getLong("last_seen")
                                                ?.takeUnless { rs.wasNull() }
                                        })
                                    }
                                }.flatMap { lastSeenOptional ->
                                    channel.createEmbed { spec ->
                                        spec.setTitle("${member.username}#${member.discriminator} (ID ${member.id.asString()}) has joined")
                                        spec.setThumbnail(member.avatarUrl)
                                        spec.addField(
                                            "Join Time",
                                            member.joinTime.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.RFC_1123_DATE_TIME),
                                            false
                                        )

                                        lastSeenOptional.ifPresent { lastSeen ->
                                            spec.addField(
                                                "Last Seen",
                                                Instant.ofEpochMilli(lastSeen).atOffset(ZoneOffset.UTC).format(
                                                    DateTimeFormatter.RFC_1123_DATE_TIME
                                                ),
                                                false
                                            )
                                        }

                                        if (oldRoles.isNotEmpty()) {
                                            spec.addField(
                                                "Old Roles",
                                                oldRoles.take(10).joinToString("\n") { id -> "<@&$id>" },
                                                false
                                            )
                                        }
                                    }
                                }.flatMap { msg ->
                                    if (oldRoles.isNotEmpty()) {
                                        waitingForRoles[msg.id.asLong()] = AddRolesRequest(
                                            key = "old_roles",
                                            targetGuild = event.guildId,
                                            targetUser = member.id,
                                            roles = oldRoles.map(Snowflake::of).toTypedArray()
                                        )
                                        Mono.zip(
                                            safetyNet.safetyNetAllowEmoji?.let(msg::addReaction) ?: Mono.empty(),
                                            safetyNet.safetyNetDenyEmoji?.let(msg::addReaction) ?: Mono.empty()
                                        )
                                            .then(Mono.delay(Duration.ofMillis(safetyNet.safetyNetDelay.toLong())))
                                            .then(Mono.defer {
                                                waitingForRoles[msg.id.asLong()]?.fulfill(msg.client) ?: Mono.empty()
                                            })
                                            .then(msg.removeAllReactions())
                                    } else {
                                        Mono.empty()
                                    }
                                }
                            }
                    } else {
                        return@flatMap Flux.concat(oldRoles.map { roleID -> member.addRole(Snowflake.of(roleID)) })
                            .then()
                    }
                }
            }
            .subscribe()

        alterEgo.client.eventDispatcher.on(MemberJoinEvent::class.java)
            .publishOn(joinEventScheduler)
            .flatMap { event ->
                val greetings = userGreetings[event.guildId.asLong()] ?: return@flatMap Mono.empty<Message>()

                val messageMono = event.guild.flatMap { guild -> guild.getChannelById(Snowflake.of(greetings.greetingChannel)) }
                    .ofType(GuildMessageChannel::class.java)
                    .delayElement(Duration.ofMillis(2_000))
                    .flatMap { channel ->
                        channel.createMessage(
                            greetings.greeting
                                .replace("%user.mention", event.member.mention)
                                .replace("%user.name", event.member.username)
                                .replace("%user.id", event.member.id.asString())
                                .replace(
                                    "%time",
                                    event.member.joinTime.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.RFC_1123_DATE_TIME)
                                )
                        )
                    }

                if (greetings.newbieRole != null) {
                    event.member.addRole(Snowflake.of(greetings.newbieRole))
                        .then(messageMono)
                } else {
                    messageMono
                }
            }
            .subscribe()
    }

    fun registerLeaveUpdates() {
        alterEgo.client.eventDispatcher.on(MemberLeaveEvent::class.java)
            .flatMap { event -> updateLeavingMemberInfo(event.guildId, event.user) }
            .subscribe()

        alterEgo.client.eventDispatcher.on(BanEvent::class.java)
            .flatMap { event -> updateLeavingMemberInfo(event.guildId, event.user) }
            .subscribe()
    }

    fun registerRoleUpdates() {
        alterEgo.client.eventDispatcher.on(MemberUpdateEvent::class.java)
            .flatMap(MemberUpdateEvent::getMember)
            .flatMap(this::updateMemberRoles)
            .subscribe()

        alterEgo.client.eventDispatcher.on(GuildCreateEvent::class.java)
            .map(GuildCreateEvent::getGuild)
            .flatMap(Guild::getMembers)
            .flatMap { member -> Mono.zip(updateMemberInfo(member), updateMemberRoles(member)) }
            .subscribe()

        alterEgo.client.eventDispatcher.on(ResumeEvent::class.java)
            .map(ResumeEvent::getClient)
            .flatMap(DiscordClient::getGuilds)
            .flatMap(Guild::getMembers)
            .flatMap { member -> Mono.zip(updateMemberInfo(member), updateMemberRoles(member)) }
            .subscribe()

        alterEgo.client.eventDispatcher.on(RoleDeleteEvent::class.java)
            .flatMap { event ->
                wrapMono(roleUpdateContext) {
                    alterEgo.usePreparedStatement("DELETE FROM $SAFETY_NET_ROLES_LIST_TABLE_NAME WHERE role_id = ?;") { prepared ->
                        prepared.setString(1, event.roleId.asString())
                        prepared.execute()
                    }
                }
            }
            .subscribe()
    }

    fun updateMemberRoles(member: Member): Mono<Unit> =
        wrapMono(roleUpdateContext) {
            alterEgo.useConnection { connection ->
                val existingRoleIDs =
                    connection.prepareStatement("SELECT id, role_id FROM $SAFETY_NET_ROLES_LIST_TABLE_NAME WHERE guild_id = ? AND user_id = ?;")
                        .use { select ->
                            select.setString(1, member.guildId.asString())
                            select.setString(2, member.id.asString())
                            select.execute()

                            select.resultSet.use { rs ->
                                val results: MutableList<Pair<String, String>> = ArrayList()
                                while (rs.next()) results.add(
                                    Pair(
                                        rs.getString("role_id"),
                                        rs.getString("id")
                                    )
                                )
                                results
                            }
                        }

                connection.prepareStatement("INSERT INTO $SAFETY_NET_ROLES_LIST_TABLE_NAME (id, guild_id, user_id, role_id) VALUES (?, ?, ?, ?);")
                    .use { insert ->
                        insert.setString(2, member.guildId.asString())
                        insert.setString(3, member.id.asString())

                        member.roleIds.forEach { roleID ->
                            val str = roleID.asString()
                            if (existingRoleIDs.none { (id) -> id == str }) {
                                insert.setString(1, newID())
                                insert.setString(4, roleID.asString())
                                insert.execute()
                            }
                        }
                    }
                connection.prepareStatement("DELETE FROM $SAFETY_NET_ROLES_LIST_TABLE_NAME WHERE id = ?;")
                    .use { delete ->
                        existingRoleIDs.forEach { (roleID, id) ->
                            if (member.roleIds.none { snowflake -> snowflake.asString() == roleID }) {
                                delete.setString(1, id)
                                delete.execute()
                            }
                        }
                    }
            }
        }

    fun updateMemberInfo(member: Member): Mono<Unit> =
        wrapMono(roleUpdateContext) {
            val id =
                alterEgo.usePreparedStatement("SELECT id FROM $SAFETY_NET_INFO_TABLE_NAME WHERE guild_id = ? AND user_id = ?;") { prepared ->
                    prepared.setString(1, member.guildId.asString())
                    prepared.setString(2, member.id.asString())
                    prepared.execute()

                    prepared.resultSet.use { rs -> rs.takeIf(ResultSet::next)?.getString("id") }
                }

            if (id == null) {
                alterEgo.usePreparedStatement("INSERT INTO $SAFETY_NET_INFO_TABLE_NAME (id, guild_id, user_id, last_seen) VALUES (?, ?, ?, ?);") { prepared ->
                    prepared.setString(1, newID())
                    prepared.setString(2, member.guildId.asString())
                    prepared.setString(3, member.id.asString())
                    prepared.setLong(4, utc.millis())
                    prepared.execute()
                }
            } else {
                alterEgo.usePreparedStatement("UPDATE $SAFETY_NET_INFO_TABLE_NAME SET last_seen = ? WHERE id = ?;") { prepared ->
                    prepared.setLong(1, utc.millis())
                    prepared.setString(2, id)
                    prepared.execute()
                }
            }

            Unit
        }

    fun updateLeavingMemberInfo(guildID: Snowflake, user: User): Mono<Unit> =
        wrapMono(roleUpdateContext) {
            val id =
                alterEgo.usePreparedStatement("SELECT id FROM $SAFETY_NET_INFO_TABLE_NAME WHERE guild_id = ? AND user_id = ?;") { prepared ->
                    prepared.setString(1, guildID.asString())
                    prepared.setString(2, user.id.asString())
                    prepared.execute()

                    prepared.resultSet.use { rs -> rs.takeIf(ResultSet::next)?.getString("id") }
                }

            if (id == null) {
                alterEgo.usePreparedStatement("INSERT INTO $SAFETY_NET_INFO_TABLE_NAME (id, guild_id, user_id, last_seen) VALUES (?, ?, ?, ?);") { prepared ->
                    prepared.setString(1, newID())
                    prepared.setString(2, guildID.asString())
                    prepared.setString(3, user.id.asString())
                    prepared.setLong(4, utc.millis())
                    prepared.execute()
                }
            } else {
                alterEgo.usePreparedStatement("UPDATE $SAFETY_NET_INFO_TABLE_NAME SET last_seen = ? WHERE id = ?;") { prepared ->
                    prepared.setLong(1, utc.millis())
                    prepared.setString(2, id)
                    prepared.execute()
                }
            }

            Unit
        }

    init {
        alterEgo.useStatement { statement ->
            statement.execute(
                createTableSql(
                    SAFETY_NET_TABLE_NAME,
                    "guild_id VARCHAR(32) NOT NULL PRIMARY KEY",
                    "user_info_channel_id VARCHAR(32)",
                    "safety_net_enabled BOOLEAN DEFAULT false NOT NULL",
                    "safety_net_delay INT DEFAULT ${5_000} NOT NULL",
                    "safety_net_allow_emoji VARCHAR(32) DEFAULT '${alterEgo.config["safetyNetAllowEmoji"]?.toString()
                        ?: "SafetyNetAllow|594190693782651079"}'",
                    "safety_net_deny_emoji VARCHAR(32) DEFAULT '${alterEgo.config["safetyNetDenyEmoji"]?.toString()
                        ?: "SafetyNetDeny|594189285826101248"}'"
                )
            )

            statement.execute(
                createTableSql(
                    SAFETY_NET_INFO_TABLE_NAME,
                    "id VARCHAR(32) NOT NULL PRIMARY KEY",
                    "guild_id VARCHAR(32) NOT NULL",
                    "user_id VARCHAR(32) NOT NULL",
                    "last_seen BIGINT"
                )
            )

            statement.execute(
                createTableSql(
                    SAFETY_NET_ROLES_LIST_TABLE_NAME,
                    "id VARCHAR(32) NOT NULL PRIMARY KEY",
                    "guild_id VARCHAR(32) NOT NULL",
                    "user_id VARCHAR(32) NOT NULL",
                    "role_id VARCHAR(32) NOT NULL"
                )
            )

            statement.execute(
                createTableSql(
                    GREETINGS_TABLE_NAME,
                    "guild_id VARCHAR(32) NOT NULL PRIMARY KEY",
                    "greetings_channel VARCHAR(32) NOT NULL",
                    "greeting VARCHAR(2000) NOT NULL",
                    "newbie_role VARCHAR(32)"
                )
            )
        }
    }
}