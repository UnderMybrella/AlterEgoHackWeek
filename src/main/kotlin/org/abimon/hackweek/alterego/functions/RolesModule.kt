package org.abimon.hackweek.alterego.functions

import discord4j.core.`object`.entity.GuildEmoji
import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.util.Image
import discord4j.core.event.domain.message.MessageCreateEvent
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.abimon.hackweek.alterego.*
import org.abimon.hackweek.alterego.requests.ClientRequest
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.switchIfEmpty
import java.awt.Color
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.sql.ResultSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.regex.PatternSyntaxException
import javax.imageio.ImageIO

@ExperimentalUnsignedTypes
@ExperimentalCoroutinesApi
class RolesModule(alterEgo: AlterEgo) : AlterEgoModule(alterEgo) {
    companion object {
        val COMMAND_BUNDLE_CREATE_NAME = "roles.bundle.create"
        val COMMAND_BUNDLE_CREATE_DEFAULT = "role bundle create"

        val COMMAND_BUNDLE_REMOVE_NAME = "roles.bundle.remove"
        val COMMAND_BUNDLE_REMOVE_DEFAULT = "role bundle remove"

        val COMMAND_BUNDLE_SHOW_NAME = "roles.bundle.show"
        val COMMAND_BUNDLE_SHOW_DEFAULT = "role bundles list"

        fun matchThread(runnable: Runnable): Thread {
            val thread = Thread(runnable, "match-scheduler")
            thread.isDaemon = true
            return thread
        }
    }

    val commandBundleCreate = command(COMMAND_BUNDLE_CREATE_NAME) ?: COMMAND_BUNDLE_CREATE_DEFAULT
    val commandBundleRemove = command(COMMAND_BUNDLE_REMOVE_NAME) ?: COMMAND_BUNDLE_REMOVE_DEFAULT
    val commandBundleShow = command(COMMAND_BUNDLE_SHOW_NAME) ?: COMMAND_BUNDLE_SHOW_DEFAULT

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

    val matchContext = Executors.newCachedThreadPool(Companion::matchThread).asCoroutineDispatcher()
    val matchScheduler = CoroutineReactorScheduler(context = matchContext)

    val rolePackages: MutableMap<Long, MutableList<RolePackage>> = HashMap()
    val awaitingRoles: MutableMap<Long, ClientRequest<Void>> = ConcurrentHashMap()

    override fun register() {
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
                                                    "..."
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
            }.subscribe()

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
        }
    }
}