package org.abimon.hackweek.alterego

import discord4j.core.`object`.data.stored.MessageBean
import discord4j.core.`object`.data.stored.embed.EmbedFieldBean
import discord4j.core.`object`.entity.Message
import discord4j.core.util.EntityUtil
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.reactor.asMono
import reactor.core.publisher.Mono
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.Instant
import kotlin.coroutines.CoroutineContext

@ExperimentalUnsignedTypes
fun Long.toUString(): String = toULong().toString()

fun EmbedFieldBean(name: String, value: String, inline: Boolean): EmbedFieldBean {
    val bean = EmbedFieldBean()
    bean.name = name
    bean.value = value
    bean.isInline = inline
    return bean
}

inline fun <T, reified R> Array<T>.mapToArray(transform: (T) -> R): Array<R> =
    Array(size) { i -> transform(this[i]) }

@Suppress("REDUNDANT_INLINE_SUSPEND_FUNCTION_TYPE") //Without it it BREAKS
suspend inline fun <T, reified R> Array<T>.mapToArraySuspend(transform: suspend (T) -> R): Array<R> =
    Array(size) { i -> transform(this[i]) }

fun PreparedStatement.setIntOrNull(index: Int, int: Int?) {
    if (int != null)
        setInt(index, int)
    else
        setNull(index, Types.INTEGER)
}
fun ResultSet.getIntOrNull(columnName: String): Int? = getInt(columnName).takeUnless { wasNull() }

@ExperimentalCoroutinesApi
fun <T: Any> wrapMono(context: CoroutineContext = Dispatchers.Default, block: suspend () -> T?): Mono<T> = GlobalScope.async { block() }.asMono(context)

fun discordSnowflakeForTime(time: Instant): String = ((time.toEpochMilli() - EntityUtil.DISCORD_EPOCH) shl 22).toString()

val MESSAGE_DATA_FIELD = Message::class.java.getDeclaredField("data").apply { isAccessible = true }

val Message.bean: MessageBean
    get() = MESSAGE_DATA_FIELD[this] as MessageBean