package com.zoowii.levelsql.protocol.mysql

import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.protocol.mysql.packet.HandshakePacket
import com.zoowii.levelsql.protocol.mysql.packet.HandshakeResponsePacket
import com.zoowii.levelsql.protocol.mysql.packet.OkPacket
import com.zoowii.levelsql.protocol.mysql.packet.QuitPacket
import java.io.InputStream
import java.io.OutputStream

class ConnectionHandler(val server: MysqlServer, val inputStream: InputStream, val outputStream: OutputStream) {
    private val log = logger()

    private val context = Context(inputStream, outputStream)

    // 开始处理来自mysql客户端的连接
    fun start() {
        // start connection phase
        val handshake = context.create(HandshakePacket::class.java, context.nextSeqId())
        handshake.serverVersion = "5.5.2-m2"
        handshake.protocolVersion = 10 // always 10
        handshake.connectionId = 0 // unique connection id at same time assigned by server
        handshake.authPluginData = "dvH@I-CJ*4d|cZwk4^]:." // what's this?
        context.setCapabilityFlag((0xf5ff.toLong() or Capabilities.PLUGIN_AUTH or Capabilities.SECURE_CONNECTION or Capabilities.CLIENT_MYSQL).toInt())

        handshake.capabilities = context.capabilities
        handshake.characterSet = 0x08
        handshake.serverStatus = ServerStatus.AUTOCOMMIT
        handshake.authPluginName = "mysql_native_password"

        context.send(handshake)

        val handshakeResponse = context.receive(HandshakeResponsePacket::class.java, false)
        handshakeResponse as HandshakeResponsePacket
        log.debug("handshake response $handshakeResponse")

        context.setLastSeqId(handshakeResponse.sequenceId)

        // TODO: authenticate
        val authRes = context.create(OkPacket::class.java, context.nextSeqId())
        authRes.info = "Login Successfully"
        context.send(authRes)
        // end connection phase

        context.currentDb = handshakeResponse.database

        // start command phase
        while(true) {
            val cmdPacket = context.receiveCmdType()
            log.debug("received packet type ${cmdPacket.javaClass.canonicalName}")
            if(cmdPacket.javaClass == QuitPacket::class.java) {
                log.debug("got quit packet to close socket")
                break
            }
            context.setLastSeqId(cmdPacket.sequenceId)
            val respPacket = server.getDispatcher().dispatchCommand(context, cmdPacket)
            context.send(respPacket)
        }
        // TODO: 权限认证和依次处理请求的packet直到连接关闭
    }
}