package com.zoowii.levelsql.test.server

import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.protocol.mysql.MysqlServer
import org.junit.Test
import java.sql.DriverManager

class ProtocolTests {
    private val log = logger()

    private val port = 3000
    @Test fun testStartMysqlServer() {
        val server = MysqlServer()
        log.debug("starting mysql protocol at localhost:$port")
        server.startLoop(port)
    }

    @Test fun testConnectToMysql() {
        Class.forName("com.mysql.cj.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:$port/test?useSSL=false&serverTimezone=UTC",
                "root","123456")
        val stmt = conn.createStatement()
        val resultSet = stmt.executeQuery("select * from employee")
        log.debug("result set ${resultSet}")
        conn.close()
    }
}