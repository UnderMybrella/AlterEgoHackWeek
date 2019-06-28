package org.abimon.hackweek.alterego.stores.routes

import discord4j.core.`object`.data.stored.MessageBean
import discord4j.core.`object`.data.stored.UserBean
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.isActive
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.abimon.hackweek.alterego.createTableSql
import org.abimon.hackweek.alterego.mapToArraySuspend
import org.abimon.hackweek.alterego.stores.MessageTableBean
import org.abimon.hackweek.alterego.toUString
import reactor.core.scheduler.Schedulers
import java.sql.ResultSet
import java.sql.SQLException

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
interface IGrandMessageRoute {
    suspend fun messageForID(id: String): MessageBean?
    suspend fun messagesInRange(offset: Int, limit: Int): List<MessageBean> =
        messagesInRange(offset, limit, ArrayList())

    suspend fun <T : MutableList<MessageBean>> messagesInRange(offset: Int, limit: Int, results: T): T
    suspend fun messagesInRange(offset: Int, limit: Int, minID: String, maxID: String): List<MessageBean> =
        messagesInRange(offset, limit, minID, maxID, ArrayList())

    suspend fun <T : MutableList<MessageBean>> messagesInRange(
        offset: Int,
        limit: Int,
        minID: String,
        maxID: String,
        results: T
    ): T

    suspend fun countMessages(): Long
    suspend fun messageKeys(): Set<String> {
        val set: MutableSet<String> = HashSet()
        messageKeys { set.add(it) }
        return set
    }
    suspend fun messageKeys(keyFunc: (String) -> Unit)
    suspend fun save(bean: MessageBean) = save(bean.id, bean)
    suspend fun save(id: Long, bean: MessageBean)

    suspend fun messageTableForID(id: String): MessageTableBean?
    suspend fun attachmentIDsForMessage(messageID: String): Array<String>
    suspend fun embedIDsForMessage(messageID: String): Array<String>
    suspend fun reactionIDsForMessage(messageID: String): Array<String>
    suspend fun webhookAuthorForMessage(messageID: String): UserBean?
    suspend fun userMentionsForMessage(messageID: String): LongArray
    suspend fun roleMentionsForMessage(messageID: String): LongArray

    suspend fun lastMessageIn(channelID: String): MessageTableBean?
}

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class GrandMessageRoute : GrandStationRoute(), IGrandMessageRoute {
    private val conductor = Schedulers.newElastic("grand-message-route-scheduler")
    private val dispatcher = conductor.asCoroutineDispatcher()

    override suspend fun messageForID(id: String): MessageBean? = messageTableForID(id)?.let { table -> completeBean(table) }

    suspend fun completeBean(messageTable: MessageTableBean): MessageBean {
        val bean = messageTable.toBean()
        bean.attachments = attachmentIDsForMessage(messageTable.id).mapToArraySuspend(station::attachmentForID)
        bean.embeds = embedIDsForMessage(messageTable.id).mapToArraySuspend(station::embedForID)
        bean.reactions = reactionIDsForMessage(messageTable.id).mapToArraySuspend(station::reactionForID)
        bean.mentions = userMentionsForMessage(messageTable.id)
        bean.mentionRoles = roleMentionsForMessage(messageTable.id)

        if (messageTable.webhookId == messageTable.authorId) {
            //TODO: Implement this once D4J doesn't just drop webhook authors
//            val webhookAuthor = webhookAuthorForMessage(id)
//            if (webhookAuthor != null) {
//                bean.author = webhookAuthor
//            }
        } else if (messageTable.authorId != null) {
            bean.author = station.userForID(messageTable.authorId)
        }

        return bean
    }

    override suspend fun <T : MutableList<MessageBean>> messagesInRange(offset: Int, limit: Int, results: T): T =
        withContext(dispatcher) {
            usePreparedStatement("SELECT id, channel_id, author_id, content, timestamp, edited_timestamp, tts, mentions_everyone, pinned, webhook_id, type FROM $MESSAGE_TABLE_NAME ORDER BY id OFFSET ? LIMIT ?;") { prepared ->
                prepared.setInt(1, offset)
                prepared.setInt(2, limit)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    results.clear()
                    while (rs.next()) results.add(completeBean(MessageTableBean(rs)))
                    results
                }
            }
        }

    override suspend fun <T : MutableList<MessageBean>> messagesInRange(
        offset: Int,
        limit: Int,
        minID: String,
        maxID: String,
        results: T
    ): T = withContext(dispatcher) {
        usePreparedStatement("SELECT id, channel_id, author_id, content, timestamp, edited_timestamp, tts, mentions_everyone, pinned, webhook_id, type FROM $MESSAGE_TABLE_NAME WHERE id > ? AND ID < ? ORDER BY id OFFSET ? LIMIT ?;") { prepared ->
            prepared.setString(1, minID)
            prepared.setString(2, maxID)
            prepared.setInt(3, offset)
            prepared.setInt(4, limit)
            prepared.execute()

            prepared.resultSet.use { rs ->
                results.clear()
                while (rs.next()) results.add(completeBean(MessageTableBean(rs)))
                results
            }
        }
    }

    override suspend fun countMessages(): Long =
        withContext(dispatcher) {
            useStatement("SELECT COUNT(id) FROM $MESSAGE_TABLE_NAME;") { statement ->
                statement.resultSet.use { rs -> rs.takeIf(ResultSet::next)?.getLong(1) ?: 0L }
            }
        }

    override suspend fun messageKeys(keyFunc: (String) -> Unit) =
        withContext(dispatcher) {
            var offset = 0
            val limit = 100
            var loop = true
            usePreparedStatement("SELECT id FROM $MESSAGE_TABLE_NAME SORT BY id OFFSET ? LIMIT ?;") { prepared ->
                while (loop && isActive) {
                    prepared.setInt(1, offset)
                    prepared.setInt(2, limit)
                    prepared.execute()

                    prepared.resultSet.use { rs ->
                        if (rs.next()) {
                            do {
                                keyFunc(rs.getString("id"))
                                offset++
                            } while (rs.next())
                        } else {
                            loop = false
                        }
                    }
                }
            }
        }

    override suspend fun save(id: Long, bean: MessageBean) {
        withContext(dispatcher) {
            val messageID = bean.id.toUString()
            if (messageTableForID(messageID) != null) {
                usePreparedStatement("UPDATE $MESSAGE_TABLE_NAME SET content = ?, edited_timestamp = ?, pinned = ? WHERE id = ?;") { prepared ->
                    prepared.setString(1, bean.content?.takeIf(String::isNotEmpty))
                    prepared.setString(2, bean.editedTimestamp)
                    prepared.setBoolean(3, bean.isPinned)
                    prepared.setString(4, id.toUString())
                    prepared.execute()
                }

                val existingEmbeds = embedIDsForMessage(messageID).map { id -> id to station.embedForID(id) }
                val existingReactions =
                    reactionIDsForMessage(messageID).map { id -> id to station.reactionForID(id) }

                existingEmbeds.forEach { (id, embed) ->
                    if (bean.embeds.none { newEmbed -> newEmbed == embed }) {
                        //Embed has been deleted or changed
                        station.deleteEmbed(id)
                    }
                }
                bean.embeds.forEach { newEmbed ->
                    if (existingEmbeds.none { (_, embed) -> newEmbed == embed }) {
                        //We got a new embed
                        station.save(newEmbed)
                    }
                }

                //TODO: Examine the efficiency of reaction events
                existingReactions.forEach { (id, reaction) ->
                    if (bean.reactions?.none { newReact -> newReact == reaction } == true) {
                        //Reaction has been deleted or updated
                        station.deleteReaction(id)
                    }
                }
                bean.reactions?.forEach { newReact ->
                    if (existingReactions.none { (_, reaction) -> newReact == reaction }) {
                        //We got a new reaction
                        station.save(newReact)
                    }
                }
            } else {
                try {
                    usePreparedStatement("INSERT INTO $MESSAGE_TABLE_NAME (id, channel_id, author_id, content, timestamp, edited_timestamp, tts, mentions_everyone, pinned, webhook_id, type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);") { prepared ->
                        prepared.setString(1, messageID)
                        prepared.setString(2, bean.channelId.toUString())
                        prepared.setString(3, bean.author?.id?.toUString())
                        prepared.setString(4, bean.content?.takeIf(String::isNotEmpty))
                        prepared.setString(5, bean.timestamp)
                        prepared.setString(6, bean.editedTimestamp)
                        prepared.setBoolean(7, bean.isTts)
                        prepared.setBoolean(8, bean.isMentionEveryone)
                        prepared.setBoolean(9, bean.isPinned)
                        prepared.setString(10, bean.webhookId?.toUString())
                        prepared.setInt(11, bean.type)
                        prepared.execute()
                    }
                } catch (sql: SQLException) {
                    println("ID: $messageID")
                    throw sql
                }

                if (bean.attachments.isNotEmpty()) {
                    usePreparedStatement("INSERT INTO $MESSAGE_ATTACHMENTS_TABLE_NAME (attachment_id, message_id) VALUES (?, ?);") { prepared ->
                        bean.attachments.forEach { attachment ->
                            prepared.setString(1, station.save(attachment))
                            prepared.setString(2, messageID)
                            prepared.addBatch()
                        }
                        prepared.executeBatch()
                    }
                }

                if (bean.embeds.isNotEmpty()) {
                    usePreparedStatement("INSERT INTO $MESSAGE_EMBEDS_TABLE_NAME (embed_id, message_id) VALUES (?, ?);") { prepared ->
                        bean.embeds.forEach { embed ->
                            prepared.setString(1, station.save(embed))
                            prepared.setString(2, messageID)
                            prepared.addBatch()
                        }
                        prepared.executeBatch()
                    }
                }

                if (bean.reactions?.isNotEmpty() == true) {
                    usePreparedStatement("INSERT INTO $MESSAGE_REACTIONS_TABLE_NAME (reaction_id, message_id) VALUES (?, ?);") { prepared ->
                        bean.reactions!!.forEach { reaction ->
                            prepared.setString(1, station.save(reaction))
                            prepared.setString(2, messageID)
                            prepared.addBatch()
                        }
                        prepared.executeBatch()
                    }
                }

                if (bean.mentions.isNotEmpty()) {
                    usePreparedStatement("INSERT INTO $USER_MENTIONS_TABLE_NAME (id, message_id, user_id) VALUES (?, ?, ?);") { prepared ->
                        bean.mentions.forEach { mentionID ->
                            prepared.setString(1, newID())
                            prepared.setString(2, messageID)
                            prepared.setString(3, mentionID.toUString())
                            prepared.addBatch()
                        }
                        prepared.executeBatch()
                    }
                }

                if (bean.mentionRoles.isNotEmpty()) {
                    usePreparedStatement("INSERT INTO $ROLE_MENTIONS_TABLE_NAME (id, message_id, role_id) VALUES (?, ?, ?);") { prepared ->
                        bean.mentionRoles.forEach { mentionID ->
                            prepared.setString(1, newID())
                            prepared.setString(2, messageID)
                            prepared.setString(3, mentionID.toUString())
                            prepared.addBatch()
                        }
                        prepared.executeBatch()
                    }
                } else {}
            }
        }
    }

    override suspend fun messageTableForID(id: String): MessageTableBean? =
        withContext(dispatcher) {
            usePreparedStatement("SELECT id, channel_id, author_id, content, timestamp, edited_timestamp, tts, mentions_everyone, pinned, webhook_id, type FROM $MESSAGE_TABLE_NAME WHERE id = ?;") { prepared ->
                prepared.setString(1, id)
                prepared.execute()

                prepared.resultSet.use { rs -> rs.takeIf(ResultSet::next)?.let(::MessageTableBean) }
            }
        }

    override suspend fun attachmentIDsForMessage(messageID: String): Array<String> =
        withContext(dispatcher) {
            usePreparedStatement("SELECT attachment_id FROM $MESSAGE_ATTACHMENTS_TABLE_NAME WHERE message_id = ? ORDER BY attachment_id;") { prepared ->
                prepared.setString(1, messageID)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    val results: MutableList<String> = ArrayList()
                    while (rs.next()) results.add(rs.getString("attachment_id"))
                    results.toTypedArray()
                }
            }
        }

    override suspend fun embedIDsForMessage(messageID: String): Array<String> =
        withContext(dispatcher) {
            usePreparedStatement("SELECT embed_id FROM $MESSAGE_EMBEDS_TABLE_NAME WHERE message_id = ? ORDER BY embed_id;") { prepared ->
                prepared.setString(1, messageID)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    val results: MutableList<String> = ArrayList()
                    while (rs.next()) results.add(rs.getString("embed_id"))
                    results.toTypedArray()
                }
            }
        }

    override suspend fun webhookAuthorForMessage(messageID: String): UserBean? =
        withContext(dispatcher) {
            usePreparedStatement("SELECT author_id, username, discriminator, avatar, is_bot FROM $MESSAGE_AUTHOR_TABLE_NAME WHERE message_id = ?;") { prepared ->
                prepared.setString(1, messageID)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    if (rs.next()) {
                        val bean = UserBean()
                        bean.id = rs.getString("author_id").toULong().toLong()
                        bean.username = rs.getString("username")
                        bean.discriminator = rs.getString("discriminator")
                        bean.avatar = rs.getString("avatar")
                        bean.isBot = rs.getBoolean("is_bot")

                        bean
                    } else {
                        null
                    }
                }
            }
        }

    override suspend fun reactionIDsForMessage(messageID: String): Array<String> =
        withContext(dispatcher) {
            usePreparedStatement("SELECT reaction_id FROM $MESSAGE_REACTIONS_TABLE_NAME WHERE message_id = ?;") { prepared ->
                prepared.setString(1, messageID)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    val results: MutableList<String> = ArrayList()
                    while (rs.next()) results.add(rs.getString("reaction_id"))
                    results.toTypedArray()
                }
            }
        }

    override suspend fun userMentionsForMessage(messageID: String): LongArray =
        withContext(dispatcher) {
            usePreparedStatement("SELECT user_id FROM $USER_MENTIONS_TABLE_NAME WHERE message_id = ? ORDER BY id;") { prepared ->
                prepared.setString(1, messageID)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    val results: MutableList<Long> = ArrayList()
                    while (rs.next()) results.add(rs.getString("user_id").toULong().toLong())
                    results.toLongArray()
                }
            }
        }

    override suspend fun roleMentionsForMessage(messageID: String): LongArray =
        withContext(dispatcher) {
            usePreparedStatement("SELECT role_id FROM $ROLE_MENTIONS_TABLE_NAME WHERE message_id = ? ORDER BY id;") { prepared ->
                prepared.setString(1, messageID)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    val results: MutableList<Long> = ArrayList()
                    while (rs.next()) results.add(rs.getString("role_id").toULong().toLong())
                    results.toLongArray()
                }
            }
        }

    override suspend fun lastMessageIn(channelID: String): MessageTableBean? =
        withContext(dispatcher) {
            usePreparedStatement("SELECT id, channel_id, author_id, content, timestamp, edited_timestamp, tts, mentions_everyone, pinned, webhook_id, type FROM $MESSAGE_TABLE_NAME WHERE channel_id = ? ORDER BY id DESC LIMIT 1;") { prepared ->
                prepared.setString(1, channelID)
                prepared.execute()

                prepared.resultSet.use { rs -> rs.takeIf(ResultSet::next)?.let(::MessageTableBean) }
            }
        }

    override fun setup() {
        useStatement { statement ->
            statement.execute(
                //TODO: Fix this once D4J does
                createTableSql(
                    MESSAGE_TABLE_NAME,
                    "id VARCHAR(32) PRIMARY KEY NOT NULL",
                    "channel_id VARCHAR(32) NOT NULL",
                    "author_id VARCHAR(32)",
                    "content VARCHAR(2000)",
                    "timestamp VARCHAR NOT NULL",
                    "edited_timestamp VARCHAR",
                    "tts BOOLEAN NOT NULL",
                    "mentions_everyone BOOLEAN NOT NULL",
                    "pinned BOOLEAN NOT NULL",
                    "webhook_id VARCHAR(32)",
                    "type INT NOT NULL"
                )
            )
        }
    }
}