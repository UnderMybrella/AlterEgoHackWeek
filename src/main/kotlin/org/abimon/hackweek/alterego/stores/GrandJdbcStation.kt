package org.abimon.hackweek.alterego.stores

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import org.abimon.hackweek.alterego.SnowflakeGenerator
import org.abimon.hackweek.alterego.stores.routes.*
import org.intellij.lang.annotations.Language
import reactor.core.scheduler.Schedulers
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement
import javax.sql.DataSource

@ExperimentalUnsignedTypes
@ExperimentalCoroutinesApi
class GrandJdbcStation(
    val tablePrefix: String,
    val db: DataSource,
    val snowstorm: SnowflakeGenerator,
    val userRoute: IGrandUserRoute = GrandUserRoute(),
    val messageRoute: IGrandMessageRoute = GrandMessageRoute(),
    val attachmentRoute: IGrandAttachmentRoute = GrandAttachmentRoute(),
    val embedRoute: IGrandEmbedRoute = GrandEmbedRoute(),
    val reactionRoute: IGrandReactionRoute = GrandReactionRoute()
): IGrandUserRoute by userRoute, IGrandMessageRoute by messageRoute, IGrandAttachmentRoute by attachmentRoute, IGrandEmbedRoute by embedRoute, IGrandReactionRoute by reactionRoute {
    private val conductor = Schedulers.newElastic("gjs-scheduler")
    private val dispatcher = conductor.asCoroutineDispatcher()

    val MESSAGE_TABLE_NAME = table("messages")
    val USER_TABLE_NAME = table("users")
    val ATTACHMENT_TABLE_NAME = table("attachments")
    val EMBED_TABLE_NAME = table("embeds")
    val REACTION_TABLE_NAME = table("reactions")

    val USER_MENTIONS_TABLE_NAME = table("message_user_mentions")
    val ROLE_MENTIONS_TABLE_NAME = table("message_role_mentions")

    val EMBED_FIELD_TABLE_NAME = table("embed_fields")

    val MESSAGE_AUTHOR_TABLE_NAME = table("message_authors")

    val MESSAGE_ATTACHMENTS_TABLE_NAME = table("message_attachments")
    val MESSAGE_EMBEDS_TABLE_NAME = table("message_embeds")
    val MESSAGE_REACTIONS_TABLE_NAME = table("message_reactions")

    public inline fun <R> use(block: (Connection) -> R): R = db.connection.use(block)
    public inline fun <R> useStatement(block: (Statement) -> R): R =
        db.connection.use { it.createStatement().use(block) }

    public inline fun <R> useStatement(@Language("SQL") sql: String, block: (Statement) -> R): R =
        db.connection.use { it.createStatement().use { stmt -> execute(sql); block(stmt) } }

    public inline fun <R> usePreparedStatement(@Language("SQL") sql: String, block: (PreparedStatement) -> R): R =
        db.connection.use { it.prepareStatement(sql).use(block) }

    public inline fun execute(@Language("SQL") sql: String) =
        db.connection.use { it.createStatement().use { stmt -> stmt.execute(sql) } }

    fun table(name: String): String = "${tablePrefix}_$name"

    fun createTableSql(tableName: String, vararg components: String): String = buildString {
        append("CREATE TABLE IF NOT EXISTS ")
        append(tableName)
        append(" (")
        append(components.joinToString(", "))
        append(");")
    }

    fun stationRoute(route: Any) {
        if (route is GrandStationRoute) {
            route.station = this
            route.setup()
        }
    }

    init {
        db.connection.use { connection ->
            connection.createStatement().use { statement ->
                execute(
                    createTableSql(
                        USER_TABLE_NAME,
                        "id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "username VARCHAR(128) NOT NULL",
                        "discriminator VARCHAR(8) NOT NULL",
                        "avatar VARCHAR",
                        "is_bot BOOLEAN DEFAULT FALSE NOT NULL"
                    )
                )

                statement.execute(
                    createTableSql(
                        ATTACHMENT_TABLE_NAME,
                        "id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "file_name VARCHAR NOT NULL",
                        "size INT NOT NULL",
                        "url VARCHAR NOT NULL",
                        "proxy_url VARCHAR NOT NULL",
                        "height INT",
                        "width INT"
                    )
                )

                statement.execute(
                    createTableSql(
                        EMBED_TABLE_NAME,
                        "id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "title VARCHAR",
                        "type VARCHAR DEFAULT 'embed' NOT NULL",
                        "description VARCHAR",
                        "url VARCHAR",
                        "timestamp VARCHAR",
                        "color INT",
                        "footer_text VARCHAR",
                        "footer_icon_url VARCHAR",
                        "footer_proxy_icon_url VARCHAR",
                        "image_url VARCHAR",
                        "image_proxy_url VARCHAR",
                        "image_height INT",
                        "image_width INT",
                        "thumbnail_url VARCHAR",
                        "thumbnail_proxy_url VARCHAR",
                        "thumbnail_height INT",
                        "thumbnail_width INT",
                        "video_url VARCHAR",
                        "video_proxy_url VARCHAR",
                        "video_height INT",
                        "video_width INT",
                        "provider_name VARCHAR",
                        "provider_url VARCHAR",
                        "author_name VARCHAR",
                        "author_url VARCHAR",
                        "author_icon_url VARCHAR",
                        "author_proxy_icon_url VARCHAR"
                    )
                )

                statement.execute(
                    createTableSql(
                        EMBED_FIELD_TABLE_NAME,
                        "id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "embed_id VARCHAR(32) NOT NULL",
                        "name VARCHAR NOT NULL",
                        "value VARCHAR NOT NULL",
                        "inline BOOLEAN DEFAULT FALSE NOT NULL"
                    )
                )

                statement.execute(
                    createTableSql(
                        REACTION_TABLE_NAME,
                        "id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "count INT NOT NULL",
                        "me BOOLEAN NOT NULL",
                        "emoji_id VARCHAR(32)",
                        "emoji_name VARCHAR NOT NULL",
                        "emoji_animated BOOLEAN DEFAULT FALSE NOT NULL"
                    )
                )

                statement.execute(
                    createTableSql(
                        MESSAGE_AUTHOR_TABLE_NAME,
                        "message_id VARCHAR(32) NOT NULL PRIMARY KEY", //Each message can only have one author
                        "author_id VARCHAR(32) NOT NULL",
                        "username VARCHAR(128) NOT NULL",
                        "discriminator VARCHAR(8) NOT NULL",
                        "avatar VARCHAR",
                        "is_bot BOOLEAN DEFAULT FALSE NOT NULL"
                    )
                )

                statement.execute(
                    createTableSql(
                        USER_MENTIONS_TABLE_NAME,
                        "id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "message_id VARCHAR(32) NOT NULL",
                        "user_id VARCHAR(32) NOT NULL"
                    )
                )

                statement.execute(
                    createTableSql(
                        ROLE_MENTIONS_TABLE_NAME,
                        "id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "message_id VARCHAR(32) NOT NULL",
                        "role_id VARCHAR(32) NOT NULL"
                    )
                )

                statement.execute(
                    createTableSql(
                        MESSAGE_ATTACHMENTS_TABLE_NAME,
                        "attachment_id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "message_id VARCHAR(32) NOT NULL"
                    )
                )

                statement.execute(
                    createTableSql(
                        MESSAGE_EMBEDS_TABLE_NAME,
                        "embed_id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "message_id VARCHAR(32) NOT NULL"
                    )
                )

                statement.execute(
                    createTableSql(
                        MESSAGE_REACTIONS_TABLE_NAME,
                        "reaction_id VARCHAR(32) NOT NULL PRIMARY KEY",
                        "message_id VARCHAR(32) NOT NULL"
                    )
                )
            }
        }

        stationRoute(userRoute)
        stationRoute(messageRoute)
        stationRoute(attachmentRoute)
        stationRoute(embedRoute)
        stationRoute(reactionRoute)
    }
}