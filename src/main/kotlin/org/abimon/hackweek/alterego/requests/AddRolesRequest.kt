package org.abimon.hackweek.alterego.requests

import discord4j.core.DiscordClient
import discord4j.core.`object`.util.Snowflake
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class AddRolesRequest(val key: String, val targetUser: Snowflake, val targetGuild: Snowflake, val roles: Array<Snowflake>, val reason: String? = null): ClientRequest<Void> {
    override fun fulfill(client: DiscordClient): Mono<Void> =
        client.getMemberById(targetGuild, targetUser)
            .flatMapMany { member -> Flux.concat(roles.map { roleID -> member.addRole(roleID, reason) }) }
            .then()
}