package org.abimon.hackweek.alterego.stores.routes

import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.abimon.hackweek.alterego.stores.GrandJdbcStation
import org.intellij.lang.annotations.Language
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
interface IGrandStationRoute {
    var station: GrandJdbcStation
}

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
open class GrandStationRoute : IGrandStationRoute {
    override lateinit var station: GrandJdbcStation

    val MESSAGE_TABLE_NAME
        get() = station.MESSAGE_TABLE_NAME
    val USER_TABLE_NAME
        get() = station.USER_TABLE_NAME
    val ATTACHMENT_TABLE_NAME
        get() = station.ATTACHMENT_TABLE_NAME
    val EMBED_TABLE_NAME
        get() = station.EMBED_TABLE_NAME
    val REACTION_TABLE_NAME
        get() = station.REACTION_TABLE_NAME

    val USER_MENTIONS_TABLE_NAME
        get() = station.USER_MENTIONS_TABLE_NAME
    val ROLE_MENTIONS_TABLE_NAME
        get() = station.ROLE_MENTIONS_TABLE_NAME

    val EMBED_FIELD_TABLE_NAME
        get() = station.EMBED_FIELD_TABLE_NAME

    val MESSAGE_AUTHOR_TABLE_NAME
        get() = station.MESSAGE_AUTHOR_TABLE_NAME

    val MESSAGE_ATTACHMENTS_TABLE_NAME
        get() = station.MESSAGE_ATTACHMENTS_TABLE_NAME
    val MESSAGE_EMBEDS_TABLE_NAME
        get() = station.MESSAGE_EMBEDS_TABLE_NAME
    val MESSAGE_REACTIONS_TABLE_NAME
        get() = station.MESSAGE_REACTIONS_TABLE_NAME

    public inline fun <R> use(block: (Connection) -> R): R = station.use(block)
    public inline fun <R> useStatement(block: (Statement) -> R): R = station.useStatement(block)
    public inline fun <R> useStatement(@Language("SQL")sql: String, block: (Statement) -> R): R = station.useStatement(sql, block)
    public inline fun <R> usePreparedStatement(@Language("SQL") sql: String, block: (PreparedStatement) -> R): R =
        station.usePreparedStatement(sql, block)

    public inline fun execute(@Language("SQL")sql: String) = station.execute(sql)

    fun createTableSql(tableName: String, vararg components: String): String = station.createTableSql(tableName, *components)

    suspend fun newID(): String = newIDLong().toString()
    suspend fun newIDLong(): Long = station.snowstorm.generate()

    open fun setup() {}
}