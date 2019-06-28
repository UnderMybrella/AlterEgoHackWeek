package org.abimon.hackweek.alterego.stores.routes

import discord4j.core.`object`.data.stored.AttachmentBean
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.abimon.hackweek.alterego.setIntOrNull
import reactor.core.scheduler.Schedulers

interface IGrandAttachmentRoute {
    suspend fun attachmentForID(id: String): AttachmentBean?
    suspend fun save(bean: AttachmentBean): String
}

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class GrandAttachmentRoute: GrandStationRoute(), IGrandAttachmentRoute {
    private val conductor = Schedulers.newElastic("grand-attachment-route-scheduler")
    private val dispatcher = conductor.asCoroutineDispatcher()

    override suspend fun attachmentForID(id: String): AttachmentBean? =
        withContext(dispatcher) {
            usePreparedStatement("SELECT file_name, size, url, proxy_url, height, width FROM $ATTACHMENT_TABLE_NAME WHERE id = ?;") { prepared ->
                prepared.setString(1, id)
                    prepared.execute()

                    prepared.resultSet.use { rs ->
                        if (rs.next()) {
                            val bean = AttachmentBean()
                            bean.fileName = rs.getString("file_name")
                            bean.size = rs.getInt("size")
                            bean.url = rs.getString("url")
                            bean.proxyUrl = rs.getString("proxy_url")
                            bean.height = rs.getInt("height")
                            bean.width = rs.getInt("width")

                            bean
                        } else {
                            null
                        }
                    }
            }
        }

    override suspend fun save(bean: AttachmentBean): String =
        withContext(dispatcher) {
            val id = newID()
            usePreparedStatement("INSERT INTO $ATTACHMENT_TABLE_NAME (id,  file_name, size, url, proxy_url, height, width) VALUES (?, ?, ?, ?, ?, ?, ?);") { prepared ->
                prepared.setString(1, id)
                prepared.setString(2, bean.fileName)
                prepared.setInt(3, bean.size)
                prepared.setString(4, bean.url)
                prepared.setString(5, bean.proxyUrl)
                prepared.setIntOrNull(6, bean.height)
                prepared.setIntOrNull(7, bean.width)
                prepared.execute()
            }
            return@withContext id
        }
}