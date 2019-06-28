package org.abimon.hackweek.alterego.stores

import discord4j.core.`object`.data.stored.MessageBean
import discord4j.store.api.util.LongObjTuple2
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import kotlinx.coroutines.reactive.publish
import kotlinx.coroutines.reactor.asMono
import org.abimon.hackweek.alterego.toUString
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class GrandJdbcMessageTrain(station: GrandJdbcStation) : GrandJdbcTrain<MessageBean>(station, "message") {
    /**
     * Attempts to find the value associated with the provided id.
     *
     * @param id The id to search with.
     * @return A mono, which may or may not contain an associated object.
     */
    override fun find(id: Long): Mono<MessageBean> = GlobalScope.async { station.messageForID(id.toUString()) }.asMono(dispatcher)

    /**
     * Gets a stream of all values in the data source.
     *
     * @return The stream of values stored.
     */
    override fun values(): Flux<MessageBean> = GlobalScope.publish<MessageBean>(dispatcher) {
        var offset = 0
        val limit = 100
        val list = ArrayList<MessageBean>(limit)
        do {
            station.messagesInRange(offset, limit, list)
            offset += list.size
            list.forEach { bean -> offer(bean) }
        } while (isActive && list.isNotEmpty())

        channel.close()
    }.toFlux()

    /**
     * Retrieves the amount of stored values in the data source currently.
     *
     * @return A mono which provides the amount of stored values.
     */
    override fun count(): Mono<Long> = GlobalScope.async { station.countMessages() }.asMono(dispatcher)

    /**
     * Retrieves all stored values with ids within a provided range.
     *
     * @param start The starting key (inclusive).
     * @param end The ending key (exclusive).
     * @return The stream of values with ids within the provided range.
     */
    override fun findInRange(start: Long, end: Long): Flux<MessageBean> = GlobalScope.publish<MessageBean>(dispatcher) {
        var offset = 0
        val limit = 100
        val list = ArrayList<MessageBean>(limit)
        val startStr = start.toUString()
        val endStr = end.toUString()
        do {
            station.messagesInRange(offset, limit, startStr, endStr, list)
            offset += list.size
            list.forEach { bean -> offer(bean) }
        } while (isActive && list.isNotEmpty())

        channel.close()
    }.toFlux()

    /**
     * Stores a key value pair.
     *
     * @param key The key representing the value.
     * @param value The value.
     * @return A mono which signals the completion of the storage of the pair.
     */
    override fun saveWithLong(key: Long, value: MessageBean): Mono<Void> = GlobalScope.async { station.save(key, value) }.asMono(dispatcher).then()

    /**
     * Stores key value pairs.
     *
     * @param entryStream A flux providing the key value pairs.
     * @return A mono which signals the completion of the storage of the pairs.
     */
    override fun saveWithLong(entryStream: Publisher<LongObjTuple2<MessageBean>>): Mono<Void> = entryStream
    .toFlux()
    .flatMap { tuple -> GlobalScope.async { station.save(tuple.t1, tuple.t2) }.asMono(dispatcher) }
    .then()

    /**
     * Gets a stream of all keys in the data source.
     *
     * @return The stream of keys stored.
     */
    override fun keys(): Flux<Long> = GlobalScope.publish<Long>(dispatcher) {
        station.messageKeys { key -> offer(key.toULong().toLong()) }
        channel.close()
    }.toFlux()
}