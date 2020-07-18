package com.zoowii.levelsql.test.server

import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.protocol.mysql.MysqlServer
import org.junit.Test
import java.sql.DriverManager

class ProtocolTests {
    private val log = logger()

    private val host = "127.0.0.1"
    private val port = 3000
    @Test fun testStartMysqlServer() {
        if(true) {
            return // only start server manually
        }
        val server = MysqlServer()
        server.initServer("./planner_tests_local")
        log.debug("starting mysql protocol at localhost:$port")
        server.startLoop(host, port)
    }

    @Test fun testConnectToMysql() {
        if(true) {
            return // only start client manually
        }
        Class.forName("com.mysql.cj.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:$port/test?useSSL=false&serverTimezone=UTC",
                "root","123456")
        val stmt = conn.createStatement()
        val resultSet = stmt.executeQuery("select * from employee")
        log.debug("result set ${resultSet}")
        conn.close()
    }
}