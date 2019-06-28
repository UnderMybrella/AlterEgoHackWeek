package org.abimon.hackweek.alterego.stores.routes

import discord4j.core.`object`.data.stored.ReactionBean
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.abimon.hackweek.alterego.toUString
import reactor.core.scheduler.Schedulers

interface IGrandReactionRoute {
    suspend fun reactionForID(id: String): ReactionBean?
    suspend fun deleteReaction(id: String)
    suspend fun save(reaction: ReactionBean): String
}

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class GrandReactionRoute : GrandStationRoute(), IGrandReactionRoute {
    private val conductor = Schedulers.newElastic("grand-reaction-route-scheduler")
    private val dispatcher = conductor.asCoroutineDispatcher()

    override suspend fun reactionForID(id: String): ReactionBean? =
        withContext(dispatcher) {
            usePreparedStatement("SELECT id, count, me, emoji_id, emoji_name, emoji_animated FROM $REACTION_TABLE_NAME WHERE id = ?;") { prepared ->
                prepared.setString(1, id)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    if (rs.next()) {
                        val bean = ReactionBean()

                        bean.count = rs.getInt("count")
                        bean.isMe = rs.getBoolean("me")
                        bean.emojiId = rs.getString("emoji_id")?.toULong()?.toLong()
                        bean.emojiName = rs.getString("emoji_name")
                        bean.isEmojiAnimated = rs.getBoolean("emoji_animated")

                        bean
                    } else {
                        null
                    }
                }
            }
        }

    override suspend fun deleteReaction(id: String) {
        withContext(dispatcher) {
            usePreparedStatement("DELETE FROM $REACTION_TABLE_NAME WHERE id = ?;") { prepared ->
                prepared.setString(1, id)
                prepared.execute()
            }
        }
    }

    override suspend fun save(reaction: ReactionBean): String =
        withContext(dispatcher) {
            val id = newID()
            usePreparedStatement("INSERT INTO $REACTION_TABLE_NAME (id, count, me, emoji_id, emoji_name, emoji_animated) VALUES (?, ?, ?, ?, ?, ?);") { prepared ->
                prepared.setString(1, id)
                prepared.setInt(2, reaction.count)
                prepared.setBoolean(3, reaction.isMe)
                prepared.setString(4, reaction.emojiId?.toUString())
                prepared.setString(5, reaction.emojiName)
                prepared.setBoolean(6, reaction.isEmojiAnimated)
                prepared.execute()
            }
            return@withContext id
        }
}