package org.abimon.hackweek.alterego.functions

import discord4j.core.`object`.entity.GuildEmoji
import discord4j.core.`object`.entity.Member
import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.entity.Role
import discord4j.core.`object`.reaction.ReactionEmoji
import discord4j.core.`object`.util.Image
import discord4j.core.`object`.util.Permission
import discord4j.core.`object`.util.Snowflake
import discord4j.core.event.domain.guild.MemberUpdateEvent
import discord4j.core.event.domain.message.MessageCreateEvent
import discord4j.core.event.domain.message.ReactionAddEvent
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.abimon.hackweek.alterego.*
import org.abimon.hackweek.alterego.requests.ToggleRolesRequest
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.switchIfEmpty
import java.awt.Color
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.sql.ResultSet
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.regex.PatternSyntaxException
import javax.imageio.ImageIO

@ExperimentalUnsignedTypes
@ExperimentalCoroutinesApi
class RolesModule(alterEgo: AlterEgo) : AlterEgoModule(alterEgo) {
    companion object {
        val COMMAND_BUNDLE_CREATE_NAME = "roles.bundle.create"
        val COMMAND_BUNDLE_CREATE_DEFAULT = "roles create bundle"

        val COMMAND_BUNDLE_REMOVE_NAME = "roles.bundle.remove"
        val COMMAND_BUNDLE_REMOVE_DEFAULT = "roles remove bundle"

        val COMMAND_BUNDLE_SHOW_NAME = "roles.bundle.show"
        val COMMAND_BUNDLE_SHOW_DEFAULT = "roles list bundles"

        val COMMAND_EXCLUSIVES_CREATE_NAME = "roles.exclusive.add"
        val COMMAND_EXCLUSIVES_CREATE_DEFAULT = "roles add exclusive"

        val COMMAND_EXCLUSIVES_REMOVE_NAME = "roles.exclusive.remove"
        val COMMAND_EXCLUSIVES_REMOVE_DEFAULT = "roles remove exclusive"

        val COMMAND_EXCLUSIVES_SHOW_NAME = "roles.exclusives.list "
        val COMMAND_EXCLUSIVES_SHOW_DEFAULT = "roles list exclusives"

        fun matchThread(runnable: Runnable): Thread {
            val thread = Thread(runnable, "match-scheduler")
            thread.isDaemon = true
            return thread
        }
    }

    val commandBundleCreate = command(COMMAND_BUNDLE_CREATE_NAME) ?: COMMAND_BUNDLE_CREATE_DEFAULT
    val commandBundleRemove = command(COMMAND_BUNDLE_REMOVE_NAME) ?: COMMAND_BUNDLE_REMOVE_DEFAULT
    val commandBundleShow = command(COMMAND_BUNDLE_SHOW_NAME) ?: COMMAND_BUNDLE_SHOW_DEFAULT

    val commandExclusivesCreate = command(COMMAND_EXCLUSIVES_CREATE_NAME) ?: COMMAND_EXCLUSIVES_CREATE_DEFAULT
    val commandExclusivesRemove = command(COMMAND_EXCLUSIVES_REMOVE_NAME) ?: COMMAND_EXCLUSIVES_REMOVE_DEFAULT
    val commandExclusivesShow = command(COMMAND_EXCLUSIVES_SHOW_NAME) ?: COMMAND_EXCLUSIVES_SHOW_DEFAULT

    data class RolePackage(
        val id: String,
        val guildID: String,
        val roleID: String,
        val regex: Regex?,
        val assignable: Boolean
    ) {
        val additionalRoles: MutableList<Pair<String, String>> = ArrayList()

        constructor(rs: ResultSet) : this(
            rs.getString("id"),
            rs.getString("guild_id"),
            rs.getString("role_id"),
            try {
                rs.getString("regex").toRegex()
            } catch (pse: PatternSyntaxException) {
                null
            },
            rs.getBoolean("assignable")
        )
    }

    val ROLE_RECEIVABLE_TABLE = "roles_receivable"
    val ADDITIONAL_ROLES_TABLE = "roles_additional"
    val EXCLUSIVE_ROLES_TABLE = "roles_exclusionary"

    val matchContext = Executors.newCachedThreadPool(Companion::matchThread).asCoroutineDispatcher()
    val matchScheduler = CoroutineReactorScheduler(context = matchContext)

    val rolePackages: MutableMap<Long, MutableList<RolePackage>> = HashMap()
    val exclusiveRoles: MutableMap<Long, MutableList<Long>> = HashMap()
    val awaitingPermissionForRole: MutableMap<Long, MutableList<ToggleRolesRequest>> = ConcurrentHashMap()
    val awaitingConfirmationForRole: MutableMap<Long, MutableList<ToggleRolesRequest>> = ConcurrentHashMap()

    fun registerBundles() {
        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .filterWhen(alterEgo::messageRequiresManageRolesPermission)
            .flatMap { msg -> alterEgo.stripMessagePrefix(msg) }
            .flatMap { msg ->
                alterEgo.stripCommandFromMessage(
                    msg,
                    COMMAND_BUNDLE_CREATE_NAME,
                    commandBundleCreate
                )
            }
            .flatMap<Message> { msg ->
                val parameters = msg.content.orElse("").parameters()
                if (parameters.size < 3 || !(parameters[0].equals(
                        "assignable",
                        true
                    ) || parameters[0].equals("requestable", true))
                )
                    return@flatMap msg.channel.flatMap { channel ->
                        channel.createMessage(
                            "Invalid parameters supplied!\nSyntax: ${alterEgo.prefixFor(
                                channel
                            )}$commandBundleCreate [assignable|requestable] [role id] [regex] {additional role ids}"
                        )
                    }

                val assignable = parameters[0].equals("assignable", true)

                val regex = try {
                    parameters[2].toRegex()
                } catch (pse: PatternSyntaxException) {
                    return@flatMap msg.channel.flatMap { channel -> channel.createMessage("Invalid regex supplied (`${parameters[2]}`)") }
                }

                return@flatMap msg.guild.flatMap { guild ->
                    guild.findRoleByIdentifier(parameters[1])
                        .flatMap { mainRole ->
                            if (rolePackages[guild.id.asLong()]?.any { rolePackage -> rolePackage.roleID == mainRole.id.asString() && rolePackage.assignable == assignable } == true) {
                                msg.channel.flatMap { channel ->
                                    channel.createEmbed { spec ->
                                        spec.setDescription("${mainRole.mention} is already ${if (assignable) "assignable" else "requestable"}!")
                                    }
                                }
                            } else {
                                Flux.concat(parameters.drop(2).map(guild::findRoleByIdentifier))
                                    .collectList()
                                    .flatMap { roleList ->
                                        wrapMono(defaultContext) {
                                            val rolePackage = RolePackage(
                                                newID(),
                                                guild.id.asString(),
                                                mainRole.id.asString(),
                                                regex,
                                                assignable
                                            )
                                            rolePackage.additionalRoles.addAll(roleList.map { role -> role.id.asString() to newID() })

                                            rolePackages.computeIfAbsent(guild.id.asLong()) { ArrayList() }
                                                .add(rolePackage)

                                            alterEgo.usePreparedStatement("INSERT INTO $ROLE_RECEIVABLE_TABLE (id, guild_id, role_id, regex, assignable) VALUES (?, ?, ?, ?, ?);") { prepared ->
                                                prepared.setString(1, rolePackage.id)
                                                prepared.setString(2, rolePackage.guildID)
                                                prepared.setString(3, rolePackage.roleID)
                                                prepared.setString(4, rolePackage.regex?.pattern ?: "")
                                                prepared.setBoolean(5, rolePackage.assignable)
                                                prepared.execute()
                                            }

                                            alterEgo.usePreparedStatement("INSERT INTO $ADDITIONAL_ROLES_TABLE (id, role_package_id, role_id) VALUES (?, ?, ?);") { prepared ->
                                                rolePackage.additionalRoles.forEach { (roleID, id) ->
                                                    prepared.setString(1, id)
                                                    prepared.setString(2, rolePackage.id)
                                                    prepared.setString(3, roleID)
                                                    prepared.addBatch()
                                                }

                                                prepared.executeBatch()
                                            }
                                        }.flatMap {
                                            val roleEmojiName = "AE_Assign_${mainRole.name.replace(
                                                "\\s+".toRegex(),
                                                "_"
                                            ).let { str ->
                                                if (str.length > 16) str.replaceRange(
                                                    16,
                                                    str.length,
                                                    ""
                                                ) else str
                                            }}"

                                            guild.emojis.filter { emoji -> emoji.name.equals(roleEmojiName, true) }
                                                .next()
                                                .switchIfEmpty {
                                                    wrapMono {
                                                        withContext(Dispatchers.IO) {
                                                            val img =
                                                                BufferedImage(256, 256, BufferedImage.TYPE_INT_ARGB)
                                                            val g = img.createGraphics()
                                                            try {
                                                                g.color = mainRole.color.run { Color(red, green, blue) }
                                                                g.fillOval(0, 0, img.width, img.height)
                                                            } finally {
                                                                g.dispose()
                                                            }

                                                            val baos = ByteArrayOutputStream()
                                                            ImageIO.write(img, "PNG", baos)

                                                            baos.toByteArray()
                                                        }
                                                    }.flatMap { data ->
                                                        guild.createEmoji { spec ->
                                                            spec.setName(roleEmojiName)
                                                            spec.setImage(Image.ofRaw(data, Image.Format.PNG))
                                                        }.onErrorResume { Mono.empty<GuildEmoji>() }
                                                    }
                                                }
                                                .flatMap { emoji ->
                                                    msg.channel.flatMap { channel ->
                                                        channel.createEmbed { spec ->
                                                            spec.setDescription("${mainRole.mention} is now ${if (assignable) "an assignable" else "a requestable"} role via messages that match `${regex.pattern}`\n${emoji.asFormat()} (\\${emoji.asFormat()})")
                                                        }
                                                    }
                                                }.switchIfEmpty {
                                                    msg.channel.flatMap { channel ->
                                                        channel.createEmbed { spec ->
                                                            spec.setDescription("${mainRole.mention} is now ${if (assignable) "an assignable" else "a requestable"} role via messages that match `${regex.pattern} (You'll need to create an emoji with the name $roleEmojiName to use this feature)`")
                                                        }
                                                    }
                                                }
                                        }
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
                    COMMAND_BUNDLE_REMOVE_NAME,
                    commandBundleRemove
                )
            }
            .flatMap<Message> ignore@{ msg ->
                val parameters = msg.content.orElse("").parameters()
                if (parameters.size < 3 || !(parameters[0].equals(
                        "assignable",
                        true
                    ) || parameters[0].equals("requestable", true))
                )
                    return@ignore msg.channel.flatMap { channel ->
                        channel.createMessage(
                            "Invalid parameters supplied!\nSyntax: ${alterEgo.prefixFor(
                                channel
                            )}$commandBundleRemove [assignable|requestable] [role id] [regex] {additional role ids}"
                        )
                    }

                val assignable = parameters[0].equals("assignable", true)

                val regex = parameters[2]

                return@ignore msg.guild.flatMap { guild ->
                    val bundles = rolePackages[guild.id.asLong()]
                    if (bundles?.isEmpty() != false) {
                        return@flatMap msg.channel.flatMap { channel -> channel.createMessage("Cannot remove a role bundle, as there are no role bundles for this server") }
                    }

                    val roleBundleToRemove = bundles.firstOrNull { bundle ->
                        bundle.roleID == parameters[1] && bundle.regex?.pattern == regex && bundle.assignable == assignable
                    } ?: return@flatMap msg.channel.flatMap { channel ->
                        channel.createEmbed { spec ->
                            spec.setDescription("No bundle was found that was ${if (assignable) "assignable" else "requestable"} for <@&${parameters[1]}> with regex `$regex`")
                        }
                    }

                    return@flatMap wrapMono(defaultContext) {
                        bundles.remove(roleBundleToRemove)

                        alterEgo.usePreparedStatement("DELETE FROM $ROLE_RECEIVABLE_TABLE WHERE id = ?;") { prepared ->
                            prepared.setString(1, roleBundleToRemove.id)
                            prepared.execute()
                        }

                        alterEgo.usePreparedStatement("DELETE FROM $ADDITIONAL_ROLES_TABLE WHERE role_package_id = ?;") { prepared ->
                            prepared.setString(1, roleBundleToRemove.id)
                            prepared.execute()
                        }
                    }.flatMap {
                        msg.channel.flatMap { channel ->
                            channel.createEmbed { spec ->
                                spec.setDescription("Removed the ${if (assignable) "assignable" else "requestable"} role bundle for <@&${parameters[1]}> with regex `$regex`")
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
                    COMMAND_BUNDLE_SHOW_NAME,
                    commandBundleShow
                )
            }
            .flatMap ignore@{ msg ->
                msg.guild.flatMap { guild ->
                    val bundles = rolePackages[guild.id.asLong()]

                    if (bundles?.isEmpty() != false)
                        return@flatMap msg.channel.flatMap { channel -> channel.createMessage("There are no role bundles set up for this server") }

                    msg.channel.flatMap { channel ->
                        channel.createEmbed { spec ->
                            spec.setTitle("Role Bundles for this server")
                            spec.setDescription(bundles.joinToString("\n") { bundle ->
                                "<@&${bundle.roleID}> with regex `${bundle.regex?.pattern}` (Additional roles: ${bundle.additionalRoles.joinToString { (roleID) -> "<@&$roleID>" }})"
                            })
                        }
                    }
                }
            }
            .subscribe()
    }

    fun registerExclusives() {
        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .filterWhen(alterEgo::messageRequiresManageRolesPermission)
            .flatMap { msg -> alterEgo.stripMessagePrefix(msg) }
            .flatMap { msg ->
                alterEgo.stripCommandFromMessage(
                    msg,
                    COMMAND_EXCLUSIVES_CREATE_NAME,
                    commandExclusivesCreate
                )
            }
            .flatMap { msg ->
                val parameters = msg.content.orElse("").parameters()

                if (parameters.size < 2)
                    return@flatMap msg.channel.flatMap { channel ->
                        channel.createMessage(
                            "Invalid parameters supplied!\n" +
                                    "Syntax: ${alterEgo.prefixFor(channel)}$commandExclusivesCreate [first role id] [second role id] {additional role ids}"
                        )
                    }

                msg.guild.flatMap { guild ->
                    guild.roles.collectList()
                        .flatMap { roleList ->
                            wrapMono(defaultContext) {
                                val parameterRoles = parameters.mapNotNull(roleList::findRoleByIdentifier)
                                    .map(Role::getId)

                                //TODO: Save space with this; it's pretty inefficient
                                alterEgo.usePreparedStatement("INSERT INTO $EXCLUSIVE_ROLES_TABLE (id, first_role_id, second_role_id) VALUES (?, ?, ?);") { prepared ->
                                    parameterRoles.forEach { first ->
                                        val duelers = exclusiveRoles.computeIfAbsent(first.asLong()) { ArrayList() }
                                        parameterRoles.forEach local@{ second ->
                                            if (first == second || second.asLong() in duelers) {
                                                return@local
                                            }

                                            prepared.setString(1, newID())
                                            prepared.setString(2, first.asString())
                                            prepared.setString(3, second.asString())
                                            prepared.execute()

                                            duelers.add(second.asLong())
                                        }
                                    }
                                }

                                parameterRoles
                            }.flatMap { parameterRoles ->
                                msg.channel.flatMap { channel ->
                                    channel.createEmbed { spec ->
                                        spec.setDescription(
                                            "Defined roles as exclusive of each other: ${parameterRoles.joinToString(
                                                prefix = "{",
                                                postfix = "}"
                                            ) { role -> "<@&${role.asString()}>" }}"
                                        )
                                    }
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
                alterEgo.stripCommandFromMessage(msg, COMMAND_EXCLUSIVES_REMOVE_NAME, commandExclusivesRemove)
            }
            .flatMap { msg ->
                val parameters = msg.content.orElse("").parameters()

                if (parameters.size < 2)
                    return@flatMap msg.channel.flatMap { channel ->
                        channel.createMessage(
                            "Invalid parameters supplied!\n" +
                                    "Syntax: ${alterEgo.prefixFor(channel)}$commandExclusivesRemove [first role id] [second role id] {additional role ids}"
                        )
                    }

                msg.guild.flatMap { guild ->
                    guild.roles.collectList()
                        .flatMap { roleList ->
                            wrapMono(defaultContext) {
                                val parameterRoles = parameters.mapNotNull(roleList::findRoleByIdentifier)
                                    .map(Role::getId)

                                alterEgo.usePreparedStatement("DELETE FROM $EXCLUSIVE_ROLES_TABLE WHERE first_role_id = ? AND second_role_id = ?;") { prepared ->
                                    parameterRoles.forEach { first ->
                                        val duelers = exclusiveRoles.computeIfAbsent(first.asLong()) { ArrayList() }
                                        parameterRoles.forEach local@{ second ->
                                            if (first == second || second.asLong() !in duelers) {
                                                return@local
                                            }

                                            prepared.setString(1, first.asString())
                                            prepared.setString(2, second.asString())
                                            prepared.execute()

                                            duelers.remove(second.asLong())
                                        }
                                    }
                                }

                                parameterRoles
                            }.flatMap { parameterRoles ->
                                msg.channel.flatMap { channel ->
                                    channel.createEmbed { spec ->
                                        spec.setDescription(
                                            "The following roles are no longer exclusives of each other: ${parameterRoles.joinToString(
                                                prefix = "{",
                                                postfix = "}"
                                            ) { role -> "<@&${role.asString()}>" }}"
                                        )
                                    }
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
                    COMMAND_EXCLUSIVES_SHOW_NAME,
                    commandExclusivesShow
                )
            }
            .flatMap { msg ->
                msg.guild.flatMapMany { guild ->
                    msg.channel.flatMapMany { channel ->
                        Flux.fromIterable(exclusiveRoles.entries)
                            .filter { (roleID) -> guild.roleIds.any { id -> id.asLong() == roleID } }
                            .delayElements(Duration.ofSeconds(1))
                            .flatMap { (roleID, exclusives) ->
                                channel.createEmbed { spec ->
                                    spec.setDescription(buildString {
                                        appendln("==[Exclusive Roles for <@&${roleID.toUString()}>]==")
                                        exclusives.filter { guild.roleIds.any { id -> id.asLong() == it } }
                                            .forEach { exclusive -> appendln("-| <@&${exclusive.toUString()}>") }
                                    })
                                }
                            }
                    }
                }
            }
            .subscribe()
    }

    override fun register() {
        registerBundles()
        registerExclusives()
        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .publishOn(matchScheduler)
            .filter { event -> alterEgo.messageSentByUser(event.message) }
            .filter { event -> event.guildId.isPresent }
            .filter { event -> event.message.author.isPresent }
            .flatMap { event ->
                val guildID = event.guildId.get()
                val content = event.message.content.orElse("")
                val matched = rolePackages[guildID.asLong()]?.filter { bundle ->
                    bundle.regex?.matches(content) == true
                } ?: return@flatMap Mono.empty<Void>()

                event.guild.flatMap { guild ->
                    guild.emojis.collectList()
                        .flatMap { emojis ->
                            Flux.fromIterable(matched)
                                .flatMap { bundle ->
                                    guild.getRoleById(Snowflake.of(bundle.roleID))
                                        .flatMap local@{ role ->
                                            val roleEmojiName = "AE_Assign_${role.name.replace(
                                                "\\s+".toRegex(),
                                                "_"
                                            ).let { str ->
                                                if (str.length > 16) str.replaceRange(
                                                    16,
                                                    str.length,
                                                    ""
                                                ) else str
                                            }}"

                                            val emoji =
                                                emojis.firstOrNull { emoji -> emoji.name.equals(roleEmojiName, true) }
                                                    ?: return@local Mono.empty<Void>()
                                            if (bundle.assignable) {
                                                awaitingConfirmationForRole.computeIfAbsent(event.message.id.asLong()) { ArrayList() }
                                                    .add(
                                                        ToggleRolesRequest(
                                                            key = emoji.asFormat(),
                                                            targetUser = event.message.author.get().id,
                                                            targetGuild = guild.id,
                                                            roles = bundle.additionalRoles.map { (roleID) ->
                                                                Snowflake.of(
                                                                    roleID
                                                                )
                                                            }.plus(
                                                                role.id
                                                            ).toTypedArray()
                                                        )
                                                    )

                                                event.message.addReaction(ReactionEmoji.custom(emoji))
                                            } else {
                                                awaitingPermissionForRole.computeIfAbsent(event.message.id.asLong()) { ArrayList() }
                                                    .add(
                                                        ToggleRolesRequest(
                                                            key = emoji.asFormat(),
                                                            targetUser = event.message.author.get().id,
                                                            targetGuild = guild.id,
                                                            roles = bundle.additionalRoles.map { (roleID) ->
                                                                Snowflake.of(
                                                                    roleID
                                                                )
                                                            }.plus(role.id).toTypedArray()
                                                        )
                                                    )

                                                event.message.addReaction(ReactionEmoji.custom(emoji))
                                            }
                                        }
                                }
                                .then()
                        }
                }
            }
            .subscribe()

        alterEgo.client.eventDispatcher.on(ReactionAddEvent::class.java)
            .filterWhen { event -> event.user.map { usr -> !usr.isBot } }
            .flatMap { event ->
                event.guild.flatMap { guild ->
                    event.message.flatMap { message ->
                        val reactionString = event.emoji.asFormat()
                        val awaitingPermission = awaitingPermissionForRole[event.messageId.asLong()]
                        val awaitingConfirmation = awaitingConfirmationForRole[event.messageId.asLong()]

                        val awaitingPermissionMono =
                            (awaitingPermission?.run { Flux.fromIterable(this) } ?: Flux.empty())
                                .filter { request -> request.key == reactionString }
                                .filterWhen {
                                    event.user.flatMap { user -> user.asMember(guild.id) }
                                        .flatMap(Member::getBasePermissions)
                                        .map { perms ->
                                            perms.contains(Permission.ADMINISTRATOR) || perms.contains(Permission.MANAGE_ROLES)
                                        }
                                }
                                .flatMap { request -> request.fulfill(event.client).then(Mono.just(request)) }
                                .count()

                        val awaitingConfirmationMono =
                            (awaitingConfirmation?.run { Flux.fromIterable(this) } ?: Flux.empty())
                                .filter { request -> request.key == reactionString }
                                .filter { message.author.map { user -> user.id == event.userId }.orElse(false) }
                                .flatMap { request -> request.fulfill(event.client).then(Mono.just(request)) }
                                .count()

                        awaitingPermissionMono
                            .switchIfEmpty(Mono.just(0))
                            .flatMap { count ->
                                awaitingConfirmationMono.switchIfEmpty(Mono.just(0))
                                    .map { localCount -> localCount + count }
                            }
                            .flatMap { count -> if (count > 0) message.removeAllReactions() else Mono.empty() }
                    }
                }
            }
            .subscribe()

        alterEgo.client.eventDispatcher.on(MemberUpdateEvent::class.java)
            .flatMap { event ->
                if (!event.old.isPresent)
                    Mono.empty<Void>()
                else {
                    val oldRoles = event.old.get().roleIds
                    val currentRoles = event.currentRoles
                    val newRoles = event.currentRoles.toMutableList().apply { removeAll(oldRoles) }

                    Flux.concat(
                        newRoles.filter { roleID -> roleID.asLong() in exclusiveRoles }
                            .flatMap { roleID -> (exclusiveRoles[roleID.asLong()] ?: emptyList<Long>()) }
                            .filter { exclusiveRole -> currentRoles.any { currentID -> currentID.asLong() == exclusiveRole } }
                            .map { removing -> event.member.flatMap { member -> member.removeRole(Snowflake.of(removing)) } }
                    )
                }
            }
            .subscribe()

        alterEgo.useStatement("SELECT id, guild_id, role_id, regex, assignable FROM $ROLE_RECEIVABLE_TABLE;") { statement ->
            statement.resultSet.use { rs ->
                while (rs.next()) {
                    val rolePackage = RolePackage(rs)
                    rolePackages.computeIfAbsent(rolePackage.guildID.toULong().toLong()) { ArrayList() }
                        .add(rolePackage)
                }
            }
        }

        val packages = rolePackages.values.flatten()
        alterEgo.useStatement("SELECT id, role_package_id, role_id FROM $ADDITIONAL_ROLES_TABLE;") { statement ->
            statement.resultSet.use { rs ->
                while (rs.next()) {
                    val id = rs.getString("id")
                    val rolePackageID = rs.getString("role_package_id")
                    val roleID = rs.getString("role_id")
                    packages.firstOrNull { rolePackage -> rolePackage.id == rolePackageID }?.additionalRoles?.add(
                        Pair(
                            roleID,
                            id
                        )
                    )
                }
            }
        }
        alterEgo.useStatement("SELECT id, first_role_id, second_role_id FROM $EXCLUSIVE_ROLES_TABLE;") { statement ->
            statement.resultSet.use { rs ->
                while (rs.next()) {
                    val first = rs.getString("first_role_id").toULong().toLong()
                    val second = rs.getString("second_role_id").toULong().toLong()

                    exclusiveRoles.computeIfAbsent(first) { ArrayList() }.takeIf { second !in it }?.add(second)
                    exclusiveRoles.computeIfAbsent(second) { ArrayList() }.takeIf { first !in it }?.add(first)
                }
            }
        }
    }

    init {
        alterEgo.useStatement { statement ->
            statement.execute(
                createTableSql(
                    ROLE_RECEIVABLE_TABLE,
                    "id VARCHAR(32) NOT NULL",
                    "guild_id VARCHAR(32) NOT NULL",
                    "role_id VARCHAR(32) NOT NULL",
                    "regex VARCHAR(128) NOT NULL",
                    "assignable BOOLEAN DEFAULT FALSE NOT NULL"
                )
            )

            statement.execute(
                createTableSql(
                    ADDITIONAL_ROLES_TABLE,
                    "id VARCHAR(32) NOT NULL",
                    "role_package_id VARCHAR(32) NOT NULL",
                    "role_id VARCHAR(32) NOT NULL"
                )
            )

            statement.execute(
                createTableSql(
                    EXCLUSIVE_ROLES_TABLE,
                    "id VARCHAR(32) NOT NULL",
                    "first_role_id VARCHAR(32) NOT NULL",
                    "second_role_id VARCHAR(32) NOT NULL"
                )
            )
        }
    }
}