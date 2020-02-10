package com.zoowii.levelsql.protocol.mysql

import java.lang.Exception
import java.net.ServerSocket
import java.util.concurrent.Executors

// 实现mysql的基于TCP的接口协议
class MysqlServer {
    private val serverExecutor = Executors.newCachedThreadPool()

    fun startLoop(port: Int) {
        // 为简化实现，目前这里用阻塞连接的方式，以后可以改用nio或者netty
        val serverSocket = ServerSocket(port)
        while(true) {
            val socket = serverSocket.accept()
            serverExecutor.submit({
                try {
                    val handler = ConnectionHandler(socket.getInputStream(), socket.getOutputStream())
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
        serverExecutor.shutdown()
    }
}