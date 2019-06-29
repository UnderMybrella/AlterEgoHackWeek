package org.abimon.hackweek.alterego.requests

import discord4j.core.DiscordClient
import discord4j.core.`object`.util.Snowflake
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class ToggleRolesRequest(
    val key: String,
    val targetUser: Snowflake,
    val targetGuild: Snowflake,
    val roles: Array<Snowflake>,
    val reason: String? = null
) : ClientRequest<Void> {
    override fun fulfill(client: DiscordClient): Mono<Void> =
        client.getMemberById(targetGuild, targetUser)
            .flatMapMany { member ->
                Flux.concat(roles.map { roleID ->
                    if (member.roleIds.any { snowflake -> snowflake == roleID }) member.removeRole(
                        roleID,
                        reason
                    ) else member.addRole(roleID, reason)
                })
            }
            .then()
}