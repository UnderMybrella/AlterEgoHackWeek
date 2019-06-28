package org.abimon.hackweek.alterego.stores.routes

import discord4j.core.`object`.data.stored.embed.EmbedBean
import discord4j.core.`object`.data.stored.embed.EmbedFieldBean
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.abimon.hackweek.alterego.EmbedFieldBean
import org.abimon.hackweek.alterego.setIntOrNull
import org.abimon.hackweek.alterego.stores.EmbedTableBean
import reactor.core.scheduler.Schedulers

interface IGrandEmbedRoute {
    suspend fun embedForID(id: String): EmbedBean?
    suspend fun fieldsForEmbed(embedID: String): Array<EmbedFieldBean>

    suspend fun deleteEmbed(id: String)
    suspend fun save(bean: EmbedBean): String
}

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class GrandEmbedRoute : GrandStationRoute(), IGrandEmbedRoute {
    private val conductor = Schedulers.newElastic("grand-embed-route-scheduler")
    private val dispatcher = conductor.asCoroutineDispatcher()

    override suspend fun embedForID(id: String): EmbedBean? {
        return withContext(dispatcher) {
            usePreparedStatement("SELECT id, title, type, description, url, timestamp, color, footer_text, footer_icon_url, footer_proxy_icon_url, image_url, image_proxy_url, image_height, image_width, thumbnail_url, thumbnail_proxy_url, thumbnail_height, thumbnail_width, video_url, video_proxy_url, video_height, video_width, provider_name, provider_url, author_name, author_url, author_icon_url, author_proxy_icon_url FROM $EMBED_TABLE_NAME WHERE id = ?;") { prepared ->
                prepared.setString(1, id)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    if (rs.next()) {
                        val bean = EmbedTableBean(rs).toBean()
                        bean.fields = fieldsForEmbed(id)
                        bean
                    } else {
                        null
                    }
                }
            }
        }
    }

    override suspend fun fieldsForEmbed(embedID: String): Array<EmbedFieldBean> =
        withContext(dispatcher) {
            usePreparedStatement("SELECT name, value, inline FROM $EMBED_FIELD_TABLE_NAME WHERE embed_id = ? ORDER BY id;") { prepared ->
                prepared.setString(1, embedID)
                prepared.execute()

                val results: MutableList<EmbedFieldBean> = ArrayList()
                prepared.resultSet.use { rs ->
                    while (rs.next()) results.add(
                        EmbedFieldBean(
                            rs.getString("name"),
                            rs.getString("value"),
                            rs.getBoolean("inline")
                        )
                    )
                }
                results.toTypedArray()
            }
        }

    override suspend fun deleteEmbed(id: String) {
        withContext(dispatcher) {
            usePreparedStatement("DELETE FROM $EMBED_TABLE_NAME WHERE id = ?;") { prepared ->
                prepared.setString(1, id)
                prepared.execute()
            }

            usePreparedStatement("DELETE FROM $EMBED_FIELD_TABLE_NAME WHERE embed_id = ?;") { prepared ->
                prepared.setString(1, id)
                prepared.execute()
            }
        }
    }

    override suspend fun save(bean: EmbedBean): String =
        withContext(dispatcher) {
            val id = newID()

            usePreparedStatement("INSERT INTO $EMBED_TABLE_NAME (id, title, type, description, url, timestamp, color, footer_text, footer_icon_url, footer_proxy_icon_url, image_url, image_proxy_url, image_height, image_width, thumbnail_url, thumbnail_proxy_url, thumbnail_height, thumbnail_width, video_url, video_proxy_url, video_height, video_width, provider_name, provider_url, author_name, author_url, author_icon_url, author_proxy_icon_url) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)") { prepared ->
                prepared.setString(1, id)
                prepared.setString(2, bean.title)
                prepared.setString(3, bean.type)
                prepared.setString(4, bean.description)
                prepared.setString(5, bean.url)
                prepared.setString(6, bean.timestamp)
                prepared.setIntOrNull(7, bean.color)
                prepared.setString(8, bean.footer?.text)
                prepared.setString(9, bean.footer?.iconUrl)
                prepared.setString(10, bean.footer?.proxyIconUrl)
                prepared.setString(11, bean.image?.url)
                prepared.setString(12, bean.image?.proxyUrl)
                prepared.setIntOrNull(13, bean.image?.height)
                prepared.setIntOrNull(14, bean.image?.width)
                prepared.setString(15, bean.thumbnail?.url)
                prepared.setString(16, bean.thumbnail?.proxyUrl)
                prepared.setIntOrNull(17, bean.thumbnail?.height)
                prepared.setIntOrNull(18, bean.thumbnail?.width)
                prepared.setString(19, bean.video?.url)
                prepared.setString(20, bean.video?.proxyUrl)
                prepared.setIntOrNull(21, bean.video?.height)
                prepared.setIntOrNull(22, bean.video?.width)
                prepared.setString(23, bean.provider?.name)
                prepared.setString(24, bean.provider?.url)
                prepared.setString(25, bean.author?.name)
                prepared.setString(26, bean.author?.url)
                prepared.setString(27, bean.author?.iconUrl)
                prepared.setString(28, bean.author?.proxyIconUrl)
                prepared.execute()

                id
            }

            if (bean.fields != null) {
                usePreparedStatement("INSERT INTO $EMBED_FIELD_TABLE_NAME (id, embed_id, name, value, inline) VALUES (?, ?, ?, ?, ?);") { prepared ->
                    bean.fields.forEach { field ->
                        prepared.setString(1, newID())
                        prepared.setString(2, id)
                        prepared.setString(3, field.name)
                        prepared.setString(4, field.value)
                        prepared.setBoolean(5, field.isInline)
                        prepared.addBatch()
                    }

                    prepared.executeBatch()
                }
            }

            return@withContext id
        }
}