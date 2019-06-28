package org.abimon.hackweek.alterego.stores

import discord4j.core.`object`.data.stored.MessageBean
import discord4j.core.`object`.data.stored.UserBean
import discord4j.store.api.Store
import discord4j.store.api.primitive.LongObjStore
import discord4j.store.api.service.StoreService
import discord4j.store.api.util.StoreContext
import kotlinx.coroutines.ExperimentalCoroutinesApi
import reactor.core.publisher.Mono
import java.io.Serializable

@ExperimentalUnsignedTypes
@ExperimentalCoroutinesApi
class GrandCentralService(val gcs: GrandJdbcStation): StoreService {
    /**
     * This is called to provide a new store instance for the provided configuration.
     *
     * @param keyClass The class of the keys.
     * @param valueClass The class of the values.
     * @param <K> The key type which provides a 1:1 mapping to the value type. This type is also expected to be
     * [Comparable] in order to allow for range operations.
     * @param <V> The value type, these follow
     * [JavaBean](https://en.wikipedia.org/wiki/JavaBeans#JavaBean_conventions) conventions.
     * @return The instance of the store.
    </V></K> */
    override fun <K : Comparable<K>?, V : Serializable?> provideGenericStore(
        keyClass: Class<K>,
        valueClass: Class<V>
    ): Store<K, V>? = null

    /**
     * This is used to check if this service can provide long-object stores.
     *
     * @return True if possible, else false.
     * @see LongObjStore
     */
    override fun hasLongObjStores(): Boolean = true

    /**
     * This is called to provide a new store instance with a long key and object values.
     *
     * @param valueClass The class of the values.
     * @param <V> The value type, these follow
     * [JavaBean](https://en.wikipedia.org/wiki/JavaBeans#JavaBean_conventions) conventions.
     * @return The instance of the store.
    </V> */
    @Suppress("UNCHECKED_CAST")
    override fun <V : Serializable?> provideLongObjStore(valueClass: Class<V>): LongObjStore<V>? {
        if (valueClass == MessageBean::class.java) {
            return GrandJdbcMessageTrain(gcs) as LongObjStore<V>
        } else if (valueClass == UserBean::class.java) {
            return GrandJdbcUserTrain(gcs) as LongObjStore<V>
        }

        return null
    }

    /**
     * This is a lifecycle method called to signal that a store should allocate any necessary resources.
     *
     * @param context Some context about the environment which this service is being utilized in.
     */
    override fun init(context: StoreContext) {}

    /**
     * This is used to check if this service can provide generic stores.
     *
     * @return True if possible, else false.
     * @see Store
     */
    override fun hasGenericStores(): Boolean = false

    /**
     * This is a lifecycle method called to signal that a store should dispose of any resources due to
     * an abrupt close (hard reconnect or disconnect).
     *
     * @return A mono, whose completion signals that resources have been released successfully.
     */
    override fun dispose(): Mono<Void> = Mono.empty()
}