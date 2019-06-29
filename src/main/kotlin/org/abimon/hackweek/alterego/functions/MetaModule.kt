package org.abimon.hackweek.alterego.functions

import discord4j.core.`object`.entity.GuildChannel
import discord4j.core.`object`.entity.GuildMessageChannel
import discord4j.core.`object`.entity.Message
import discord4j.core.`object`.util.Image
import discord4j.core.event.domain.message.MessageCreateEvent
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.asMono
import org.abimon.hackweek.alterego.AlterEgo
import org.abimon.hackweek.alterego.parameters
import reactor.core.publisher.Mono

@ExperimentalUnsignedTypes
@ExperimentalCoroutinesApi
class MetaModule(alterEgo: AlterEgo) : AlterEgoModule(alterEgo) {
    companion object {
        val COMMAND_SELF_NAME = "meta.self"
        val COMMAND_CHANGE_PREFIX_NAME = "meta.change.prefix"
        val COMMAND_CHANGE_COMMAND_NAME = "meta.change.command"
        val COMMAND_ROLES_NAME = "meta.roles"

        val COMMAND_SELF_DEFAULT = "meta self"
        val COMMAND_CHANGE_PREFIX_DEFAULT = "meta change prefix"
        val COMMAND_CHANGE_COMMAND_DEFAULT = "meta change command"
        val COMMAND_ROLES_DEFAULT = "meta roles"
    }

    val commandSelf = command(COMMAND_SELF_NAME) ?: COMMAND_SELF_DEFAULT
    val commandChangePrefix = command(COMMAND_CHANGE_PREFIX_NAME) ?: COMMAND_CHANGE_PREFIX_DEFAULT
    val commandChangeCommand = command(COMMAND_CHANGE_COMMAND_NAME) ?: COMMAND_CHANGE_COMMAND_DEFAULT
    val commandRoles = command(COMMAND_ROLES_NAME) ?: COMMAND_ROLES_DEFAULT

    override fun register() {
        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .flatMap(alterEgo::stripMessagePrefix)
            .flatMap { msg -> alterEgo.stripCommandFromMessage(msg, COMMAND_SELF_NAME, commandSelf) }
            .flatMap { msg -> Mono.zip(msg.authorAsMember, msg.channel, alterEgo.client.self) }
            .flatMap { tuple ->
                tuple.t2.createEmbed { spec ->
                    spec.setAuthor(tuple.t3.username, alterEgo.config["githubURL"]?.toString(), tuple.t3.avatarUrl)
                    spec.setDescription(buildString {
                        appendln("Hi, ${tuple.t1.displayName}")
                        appendln("I'm Alter Ego, and I'm a moderation bot written by UnderMybrella#4076, originally for the Discord Hack Week")
                        append("You can find out more information ")
                        listOfNotNull(
                            alterEgo.config["githubURL"]?.toString()?.run { "[Github]($this)" },
                            alterEgo.config["discordInvite"]?.toString()?.run { "[Discord]($this)" }
                        ).takeIf(List<String>::isNotEmpty)?.joinToString(", ", prefix = "over on ", postfix = ", or ")
                            ?.let(this::append)
                        val guildPrefix = (tuple.t2 as? GuildChannel)?.guildId?.asLong()?.let(alterEgo::prefixFor)
                            ?: alterEgo.defaultPrefix
                        append("by using the `${guildPrefix}help` command")
                    })
                }
            }
            .subscribe()

        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .filterWhen(alterEgo::messageRequiresManageServerPermission)
            .flatMap(alterEgo::stripMessagePrefix)
            .flatMap { msg -> alterEgo.stripCommandFromMessage(msg, COMMAND_CHANGE_PREFIX_NAME, commandChangePrefix) }
            .flatMap { msg ->
                msg.channel.ofType(GuildMessageChannel::class.java).map { channel -> Pair(msg, channel) }
            }
            .flatMap { (msg, channel) ->
                val newPrefix = msg.content.get().trim('"')
                alterEgo.setPrefixFor(channel.guildId.asLong(), newPrefix)
                    .asMono(defaultContext)
                    .flatMap { channel.createMessage("Set prefix to `$newPrefix`\nExample: `${newPrefix}help`") }
            }
            .subscribe()

        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .filterWhen(alterEgo::messageRequiresManageServerPermission)
            .flatMap(alterEgo::stripMessagePrefix)
            .flatMap { msg -> alterEgo.stripCommandFromMessage(msg, COMMAND_CHANGE_COMMAND_NAME, commandChangeCommand) }
            .flatMap { msg ->
                msg.channel.ofType(GuildMessageChannel::class.java).map { channel -> Pair(msg, channel) }
            }
            .flatMap<Message> { (msg, channel) ->
                val components = msg.content.orElse("").parameters(2)
                val name = components.getOrNull(0) ?: return@flatMap Mono.empty()
                val trigger = components.getOrNull(1)

                alterEgo.setCommandFor(channel.guildId.asLong(), name, trigger)
                    .asMono(defaultContext)
                    .flatMap {
                        if (trigger != null)
                            channel.createMessage(
                                "Set command trigger for `$name` to `$trigger`\nTo use, type: `${alterEgo.prefixFor(
                                    channel.guildId.asLong()
                                )}$trigger`"
                            )
                        else {
                            channel.createMessage("Reset command trigger for `$name`")
                        }
                    }
            }
            .subscribe()

        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .flatMap(alterEgo::stripMessagePrefix)
            .flatMap { msg -> alterEgo.stripCommandFromMessage(msg, COMMAND_ROLES_NAME, commandRoles) }
            .flatMap { msg ->
                msg.channel.ofType(GuildMessageChannel::class.java)
                    .flatMap { channel ->
                        channel.guild.flatMap { guild ->
                            guild.roles.collectList().flatMap { roles ->
                                channel.createEmbed { spec ->
                                    spec.setTitle("Roles for ${guild.name}")
                                    guild.getIconUrl(Image.Format.WEB_P)
                                        .ifPresent { thumbnail -> spec.setThumbnail(thumbnail) }
                                    spec.setDescription(roles.reversed().take(12).joinToString ("\n") { role -> "${role.mention} (`${role.id.asString()}`)" })
                                }
                            }
                        }
                    }
            }
            .subscribe()
    }
}