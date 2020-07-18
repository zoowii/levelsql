package com.zoowii.levelsql.oss.test


import com.zoowii.levelsql.engine.IntColumnType
import com.zoowii.levelsql.engine.LevelSqlEngine
import com.zoowii.levelsql.engine.TextColumnType
import com.zoowii.levelsql.engine.VarCharColumnType
import com.zoowii.levelsql.engine.store.DummyStore
import com.zoowii.levelsql.oss.*
import org.junit.Test
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import com.zoowii.levelsql.engine.utils.logger

class OssPlannerTests {
    private val baseOssUrl = "https://levelsqldemo-1251116499.cos.ap-nanjing.myqcloud.com"

    private val log = logger()

    init {
        val root: Logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
        root.setLevel(Level.INFO)
    }

    @Test
    fun testSelectPlanner() {

        val tableFileRules = { tableName: String, seq: Int, type: OssFileType ->
            if (type == OssFileType.Meta) {
                "${tableName}/meta.json"
            } else {
                "${tableName}/small${tableName}${seq.toString().padStart(2, '0')}.csv"
            }
        }
        val ossDbBaseDefinition = OssDbBaseDefinition(baseOssUrl,
                listOf("taxi"), tableFileRules, true)
        val ossDb = OssDatabaseDefinition("test",
                listOf(OssTableDefinition(
                        "taxi",
                        listOf(OssColumnDefinition("vendor_id", VarCharColumnType(100)),
                                OssColumnDefinition("rate_code", IntColumnType()),
                                OssColumnDefinition("passenger_count", IntColumnType()),
                                OssColumnDefinition("trip_time_in_secs", IntColumnType()),
                                OssColumnDefinition("trip_distance", TextColumnType()),
                                OssColumnDefinition("payment_type", VarCharColumnType(50)),
                                OssColumnDefinition("fare_amount", TextColumnType())),
                        ossDbBaseDefinition)),
                ossDbBaseDefinition)
        val store = DummyStore()
        val engine = LevelSqlEngine(store)
        engine.loadMeta()
        val session = OssDbSession(ossDb)
        engine.bindExternalSession(session)
        session.useDb("test")
//        val sql1 = "select * from taxi limit 100"
        val sql1 = "select count(vendor_id) from taxi"
        val result = engine.executeSQL(session, sql1)
        log.info("sql result: \n{}", result)
        engine.shutdown()
    }
}