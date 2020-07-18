package com.zoowii.levelsql.protocol.mysql

import com.zoowii.levelsql.engine.LevelSqlEngine
import com.zoowii.levelsql.engine.store.LocalFileStore
import com.zoowii.levelsql.protocol.mysql.exceptions.ServerException
import com.zoowii.levelsql.engine.utils.logger
import java.io.File
import java.lang.Exception
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.SocketAddress
import java.util.concurrent.Executors

// 实现mysql的基于TCP的接口协议
class MysqlServer {
    private val log = logger()

    private val serverExecutor = Executors.newCachedThreadPool()

    private val dispatcher = CommandDispatcher(this)

    fun getDispatcher() = dispatcher

    private var dbPath: String = ""
    private var engine: LevelSqlEngine? = null

    fun initServer(dbPath: String) {
        this.dbPath = dbPath
        val localDbFile = File(dbPath)
        val existedBefore = localDbFile.exists()
        val store = LocalFileStore.openStore(localDbFile)
        this.engine = LevelSqlEngine(store)
        if(existedBefore) {
           this.engine!!.loadMeta()
        } else {
            this.engine!!.saveMeta()
        }
    }

    fun getEngine() = engine ?: throw ServerException("db engine not init")

    fun startLoop(host: String, port: Int) {
        engine ?: throw ServerException("db engine not init")
        log.info("starting levelsql server on $host:$port")

        // 为简化实现，目前这里用阻塞连接的方式，以后可以改用nio或者netty
        val serverSocket = ServerSocket()
        serverSocket.bind(InetSocketAddress(host, port))
        while(true) {
            val socket = serverSocket.accept()
            serverExecutor.submit({
                try {
                    val handler = ConnectionHandler(this, socket.getInputStream(), socket.getOutputStream())
                    handler.start()
                } catch(e: Exception) {
                    e.printStackTrace()
                } finally {
                    try {
                        socket.close()
                    } catch (_: Exception) {}
                }
            })
        }
    }

    fun shutdown() {
        engine?.shutdown()
        serverExecutor.shutdown()
    }
}