package org.abimon.hackweek.alterego.requests

import discord4j.core.DiscordClient
import reactor.core.publisher.Mono

@FunctionalInterface
interface ClientRequest<T> {
    fun fulfill(client: DiscordClient): Mono<T>
}