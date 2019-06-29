package org.abimon.hackweek.alterego.functions

import discord4j.core.event.domain.message.MessageCreateEvent
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.abimon.hackweek.alterego.AlterEgo
import java.util.*

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class HelpModule(alterEgo: AlterEgo) : AlterEgoModule(alterEgo) {
    @ExperimentalUnsignedTypes
    @ExperimentalCoroutinesApi
    companion object {
        val COMMAND_HELP_NAME = "help"
        val COMMAND_HELP_DEFAULT = "help"

        val COMMANDS = mapOf(
            AutoChannelSlowMode.COMMAND_SLOWDOWN_CHANGE_NAME to AutoChannelSlowMode.COMMAND_SLOWDOWN_CHANGE_DEFAULT,
            AutoChannelSlowMode.COMMAND_SLOWDOWN_REMOVE_NAME to AutoChannelSlowMode.COMMAND_SLOWDOWN_REMOVE_DEFAULT,
            AutoChannelSlowMode.COMMAND_SLOWDOWN_SHOW_NAME to AutoChannelSlowMode.COMMAND_SLOWDOWN_SHOW_DEFAULT,

            MetaModule.COMMAND_CHANGE_COMMAND_NAME to MetaModule.COMMAND_CHANGE_COMMAND_DEFAULT,
            MetaModule.COMMAND_CHANGE_PREFIX_NAME to MetaModule.COMMAND_CHANGE_PREFIX_DEFAULT,
            MetaModule.COMMAND_ROLES_NAME to MetaModule.COMMAND_ROLES_DEFAULT,
            MetaModule.COMMAND_SELF_NAME to MetaModule.COMMAND_SELF_DEFAULT,

            RolesModule.COMMAND_BUNDLE_CREATE_NAME to RolesModule.COMMAND_BUNDLE_CREATE_DEFAULT,
            RolesModule.COMMAND_BUNDLE_REMOVE_NAME to RolesModule.COMMAND_BUNDLE_REMOVE_DEFAULT,
            RolesModule.COMMAND_BUNDLE_SHOW_NAME to RolesModule.COMMAND_BUNDLE_SHOW_DEFAULT,
            RolesModule.COMMAND_EXCLUSIVES_CREATE_NAME to RolesModule.COMMAND_EXCLUSIVES_CREATE_DEFAULT,
            RolesModule.COMMAND_EXCLUSIVES_REMOVE_NAME to RolesModule.COMMAND_EXCLUSIVES_REMOVE_DEFAULT,
            RolesModule.COMMAND_EXCLUSIVES_SHOW_NAME to RolesModule.COMMAND_EXCLUSIVES_SHOW_DEFAULT,

            UserThoroughfare.COMMAND_CHANGE_GREETING_NAME to UserThoroughfare.COMMAND_CHANGE_GREETING_DEFAULT,
            UserThoroughfare.COMMAND_CHANGE_SAFETY_NET_NAME to UserThoroughfare.COMMAND_CHANGE_SAFETY_NET_DEFAULT,
            UserThoroughfare.COMMAND_REMOVE_GREETNG_NAME to UserThoroughfare.COMMAND_REMOVE_GREETING_DEFAULT,
            UserThoroughfare.COMMAND_REMOVE_SAFETY_NET_NAME to UserThoroughfare.COMMAND_REMOVE_SAFETY_NET_DEFAULT,
            UserThoroughfare.COMMAND_SHOW_GREETING_NAME to UserThoroughfare.COMMAND_SHOW_GREETING_DEFAULT,
            UserThoroughfare.COMMAND_SHOW_SAFETY_NET_NAME to UserThoroughfare.COMMAND_SHOW_SAFETY_NET_DEFAULT,

            COMMAND_HELP_NAME to COMMAND_HELP_DEFAULT
        )
    }

    private val bundle: ResourceBundle = ResourceBundle.getBundle("AlterEgo")

    val commandHelp = command(COMMAND_HELP_NAME) ?: COMMAND_HELP_DEFAULT

    override fun register() {
        alterEgo.client.eventDispatcher.on(MessageCreateEvent::class.java)
            .map(MessageCreateEvent::getMessage)
            .filter(alterEgo::messageSentByUser)
            .flatMap { msg -> alterEgo.stripMessagePrefix(msg) }
            .flatMap { msg ->
                alterEgo.stripCommandFromMessage(
                    msg,
                    COMMAND_HELP_NAME,
                    commandHelp
                )
            }
            .flatMap { msg ->
                val command = msg.content.orElse("").takeIf(String::isNotBlank)


                msg.channel.flatMap { channel ->
                    if (command == null) {
                        channel.createEmbed { spec ->
                            spec.setTitle("Alter Ego Commands")
                            spec.setDescription(COMMANDS.entries.sortedBy { (key) -> key }
                                .joinToString("\n") { (key) ->
                                    val descKey = "$key.desc"
                                    "$key - ${if (bundle.containsKey(descKey)) bundle.getString(descKey) else descKey}"
                                })
                        }
                    } else {
                        if (command in COMMANDS) {
                            channel.createEmbed { spec ->
                                spec.setTitle("Alter Ego Command - $command")
                                val descKey = "$command.desc"
                                val syntaxKey = "$command.syntax"
                                spec.setDescription(if (bundle.containsKey(descKey)) bundle.getString(descKey) else descKey)
                                spec.addField("Syntax", if (bundle.containsKey(syntaxKey)) bundle.getString(syntaxKey) else "~|${COMMANDS[command]}", false)
                            }
                        } else {
                            channel.createMessage("No such command: `$command`")
                        }
                    }
                }
            }
            .subscribe()
    }
}