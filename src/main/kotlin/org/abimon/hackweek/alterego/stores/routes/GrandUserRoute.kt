package org.abimon.hackweek.alterego.stores.routes

import discord4j.core.`object`.data.stored.UserBean
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.isActive
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.abimon.hackweek.alterego.stores.GrandJdbcStation
import org.abimon.hackweek.alterego.toUString
import reactor.core.scheduler.Schedulers
import java.sql.ResultSet

interface IGrandUserRoute {
    suspend fun userForID(id: String): UserBean?
    suspend fun usersInRange(offset: Int, limit: Int): List<UserBean> = usersInRange(offset, limit, ArrayList())
    suspend fun <T : MutableList<UserBean>> usersInRange(offset: Int, limit: Int, results: T): T
    suspend fun usersInRange(offset: Int, limit: Int, minID: String, maxID: String): List<UserBean> =
        usersInRange(offset, limit, minID, maxID, ArrayList())

    suspend fun <T : MutableList<UserBean>> usersInRange(
        offset: Int,
        limit: Int,
        minID: String,
        maxID: String,
        results: T
    ): T

    suspend fun countUsers(): Long
    suspend fun userKeys(): Set<String> {
        val set: MutableSet<String> = HashSet()
        userKeys { set.add(it) }
        return set
    }
    suspend fun userKeys(keyFunc: (String) -> Unit)
    suspend fun save(bean: UserBean) = save(bean.id, bean)
    suspend fun save(id: Long, bean: UserBean)
    suspend fun saveAll(list: List<Pair<Long, UserBean>>)
}

@ExperimentalCoroutinesApi
@ExperimentalUnsignedTypes
class GrandUserRoute : GrandStationRoute(), IGrandUserRoute {
    private val conductor = Schedulers.newElastic("grand-user-route-scheduler")
    private val dispatcher = conductor.asCoroutineDispatcher()

    override lateinit var station: GrandJdbcStation

    override suspend fun userForID(id: String): UserBean? =
        withContext(dispatcher) {
            usePreparedStatement("SELECT id, username, discriminator, avatar, is_bot FROM $USER_TABLE_NAME WHERE id = ?;") { prepared ->
                prepared.setString(1, id)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    if (rs.next()) {
                        val bean = UserBean()
                        bean.id = id.toULong().toLong()
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

    override suspend fun <T : MutableList<UserBean>> usersInRange(offset: Int, limit: Int, results: T): T =
        withContext(dispatcher) {
            usePreparedStatement("SELECT id, username, discriminator, avatar, is_bot FROM $USER_TABLE_NAME ORDER BY id OFFSET ? LIMIT ?;") { prepared ->
                prepared.setInt(1, offset)
                prepared.setInt(2, limit)
                prepared.execute()

                prepared.resultSet.use { rs ->
                    results.clear()
                    while (rs.next()) {
                        val bean = UserBean()
                        bean.id = rs.getString("id").toULong().toLong()
                        bean.username = rs.getString("username")
                        bean.discriminator = rs.getString("discriminator")
                        bean.avatar = rs.getString("avatar")
                        bean.isBot = rs.getBoolean("is_bot")
                        results.add(bean)
                    }
                    results
                }
            }
        }

    override suspend fun <T : MutableList<UserBean>> usersInRange(
        offset: Int,
        limit: Int,
        minID: String,
        maxID: String,
        results: T
    ): T = withContext(dispatcher) {
        usePreparedStatement("SELECT id, username, discriminator, avatar, is_bot FROM $USER_TABLE_NAME WHERE id > ? AND ID < ? ORDER BY id OFFSET ? LIMIT ?;") { prepared ->
            prepared.setString(1, minID)
            prepared.setString(2, maxID)
            prepared.setInt(3, offset)
            prepared.setInt(4, limit)
            prepared.execute()

            prepared.resultSet.use { rs ->
                results.clear()
                while (rs.next()) {
                    val bean = UserBean()
                    bean.id = rs.getString("id").toULong().toLong()
                    bean.username = rs.getString("username")
                    bean.discriminator = rs.getString("discriminator")
                    bean.avatar = rs.getString("avatar")
                    bean.isBot = rs.getBoolean("is_bot")
                    results.add(bean)
                }
                results
            }
        }
    }

    override suspend fun countUsers(): Long =
        withContext(dispatcher) {
            useStatement("SELECT COUNT(id) FROM $USER_TABLE_NAME;") { statement ->
                statement.resultSet.use { rs -> rs.takeIf(ResultSet::next)?.getLong(1) ?: 0L }
            }
        }

    override suspend fun userKeys(keyFunc: (String) -> Unit) =
        withContext(dispatcher) {
            var offset = 0
            val limit = 100
            var loop = true
            usePreparedStatement("SELECT id FROM $USER_TABLE_NAME SORT BY id OFFSET ? LIMIT ?;") { prepared ->
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

    override suspend fun save(id: Long, bean: UserBean) {
        withContext(dispatcher) {
            if (userForID(id.toUString()) != null) {
                usePreparedStatement("UPDATE $USER_TABLE_NAME SET username=?, discriminator=?, avatar=?, is_bot=? WHERE id=?;") { prepared ->
                    prepared.setString(1, bean.username)
                    prepared.setString(2, bean.discriminator)
                    prepared.setString(3, bean.avatar)
                    prepared.setBoolean(4, bean.isBot)
                    prepared.setString(5, bean.id.toUString())
                    prepared.execute()
                }
            } else {
                usePreparedStatement("INSERT INTO $USER_TABLE_NAME (id, username, discriminator, avatar, is_bot) VALUES (?, ?, ?, ?, ?);") { prepared ->
                    prepared.setString(1, bean.id.toUString())
                    prepared.setString(2, bean.username)
                    prepared.setString(3, bean.discriminator)
                    prepared.setString(4, bean.avatar)
                    prepared.setBoolean(5, bean.isBot)
                    prepared.execute()
                }
            }
        }
    }

    override suspend fun saveAll(list: List<Pair<Long, UserBean>>) {
        withContext(dispatcher) {
            val selects =
                usePreparedStatement("SELECT id FROM $USER_TABLE_NAME WHERE id = ?;") { select ->
                    list.groupBy { (key) ->
                        select.setString(1, key.toUString())
                        select.execute()

                        return@groupBy select.resultSet.use(ResultSet::next)
                    }
                }

            station.usePreparedStatement("INSERT INTO $USER_TABLE_NAME (id, username, discriminator, avatar, is_bot) VALUES (?, ?, ?, ?, ?);") { insert ->
                selects[false]?.forEach { (id, bean) ->
                    insert.setString(1, id.toUString())
                    insert.setString(2, bean.username)
                    insert.setString(3, bean.discriminator)
                    insert.setString(4, bean.avatar)
                    insert.setBoolean(5, bean.isBot)
                    insert.addBatch()
                }

                insert.executeBatch()
            }

            station.usePreparedStatement("UPDATE ${station.USER_TABLE_NAME} SET username=?, discriminator=?, avatar=?, is_bot=? WHERE id=?;") { update ->
                selects[true]?.forEach { (id, bean) ->
                    update.setString(1, bean.username)
                    update.setString(2, bean.discriminator)
                    update.setString(3, bean.avatar)
                    update.setBoolean(4, bean.isBot)
                    update.setString(5, id.toUString())
                    update.addBatch()
                }

                update.executeBatch()
            }
        }
    }
}