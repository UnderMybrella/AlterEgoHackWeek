package org.abimon.hackweek.alterego.stores

import discord4j.store.api.primitive.LongObjStore
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.io.Serializable

abstract class GrandJdbcTrain<T: Serializable>(val station: GrandJdbcStation, val name: String) : LongObjStore<T> {
    val conductor = Schedulers.newElastic("gjs-$name-store-scheduler")
    val dispatcher = conductor.asCoroutineDispatcher()

    /**
     * Deletes all entries in the data source.
     *
     * @return A mono which signals the completion of the deletion of all values.
     */
    override fun deleteAll(): Mono<Void> = Mono.empty()

    /**
     * Invalidates the contents of the store. Once this is invoked, there is no longer a guarantee that the
     * data in the store is reliable.
     *
     * @return A mono which signals the completion of the invalidation of all values.
     */
    override fun invalidate(): Mono<Void> = Mono.empty()

    /**
     * Deletes values within a range of ids.
     *
     * @param start The starting key (inclusive).
     * @param end The ending key (exclusive).
     * @return A mono which signals the completion of the deletion of values.
     */
    override fun deleteInRange(start: Long, end: Long): Mono<Void> = Mono.empty()

    /**
     * Deletes a value associated with the provided id.
     *
     * @param id The id of the value to delete.
     * @return A mono which signals the completion of the deletion of the value.
     */
    override fun delete(id: Long): Mono<Void> = Mono.empty()

    override fun delete(ids: Publisher<Long>): Mono<Void> = Mono.empty()
}