package org.abimon.hackweek.alterego

import discord4j.core.ServiceMediator
import discord4j.core.`object`.data.stored.MessageBean
import discord4j.core.`object`.data.stored.embed.EmbedFieldBean
import discord4j.core.`object`.entity.*
import discord4j.core.`object`.reaction.ReactionEmoji
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
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
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
fun <T : Any> wrapMono(context: CoroutineContext = Dispatchers.Default, block: suspend () -> T?): Mono<T> =
    GlobalScope.async { block() }.asMono(context)

fun discordSnowflakeForTime(time: Instant): String =
    ((time.toEpochMilli() - EntityUtil.DISCORD_EPOCH) shl 22).toString()

val MESSAGE_DATA_FIELD = Message::class.java.getDeclaredField("data").apply { isAccessible = true }
val MESSAGE_SERVICE_MEDIATOR = Message::class.java.getDeclaredField("serviceMediator").apply { isAccessible = true }

val Message.bean: MessageBean
    get() = MESSAGE_DATA_FIELD[this] as MessageBean

val Message.serviceMediator: ServiceMediator
    get() = MESSAGE_SERVICE_MEDIATOR[this] as ServiceMediator

operator fun AtomicInteger.inc(): AtomicInteger {
    incrementAndGet()
    return this
}

operator fun AtomicInteger.dec(): AtomicInteger {
    decrementAndGet()
    return this
}

fun AtomicInteger.decrementButNotBelow(num: Int): Int {
    val result = decrementAndGet()
    if (result < num)
        return incrementAndGet()
    return result
}

fun createTableSql(tableName: String, vararg components: String): String = buildString {
    append("CREATE TABLE IF NOT EXISTS ")
    append(tableName)
    append(" (")
    append(components.joinToString(", "))
    append(");")
}

fun <T> MutableList<T>.popOrNull(): T? = if (isEmpty()) null else removeAt(0)

/**
 * Borrowed from https://stackoverflow.com/a/3366634
 */
val regex: Pattern = Pattern.compile("\"((?:\\\\\"|[^\"])*)\"|(\\S+)")

fun String.parameters(): List<String> {
    val m = regex.matcher(this)
    val results: MutableList<String> = ArrayList()
    while (m.find()) {
        if (m.group(1) != null) {
            results.add(m.group(1).replace("\\\"", "\""))
        } else {
            results.add(m.group(2))
        }
    }

    return results
}

fun String.parameters(limit: Int): List<String> {
    val m = regex.matcher(this)
    val results: MutableList<String> = ArrayList()
    try {
        while (m.find()) {
            if (m.group(1) != null) {
                results.add(m.group(1).replace("\\\"", "\""))
            } else {
                results.add(m.group(2))
            }

            if (results.size + 1 == limit) {
                results.add(this.substring(m.toMatchResult().end() + 1).trim('"'))
                break
            }
        }
    } catch (ignored: IndexOutOfBoundsException) {
    }

    return results
}

@ExperimentalUnsignedTypes
@ExperimentalCoroutinesApi
fun AlterEgo.prefixFor(channel: Channel): String =
    if (channel is GuildChannel) prefixFor(channel.guildId.asLong()) else defaultPrefix

val NON_NUMERIC_REGEX = "\\D".toRegex()
fun Guild.findRoleByIdentifier(identifier: String): Mono<Role> {
    val identifierNumeric = identifier.replace(NON_NUMERIC_REGEX, "")
    return roles.filter { role ->
        role.id.asString() == identifier || role.id.asString() == identifierNumeric || role.name.equals(
            identifier,
            true
        )
    }
        .next()
}

fun ReactionEmoji.asFormat(): String =
    when (this) {
        is ReactionEmoji.Custom -> '<' + (if (isAnimated) "a" else "") + ':' + name + ':' + id.asString() + '>'
        is ReactionEmoji.Unicode -> raw
        else -> toString()
    }