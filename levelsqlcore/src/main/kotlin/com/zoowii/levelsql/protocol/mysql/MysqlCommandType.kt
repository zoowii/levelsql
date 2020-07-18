package com.zoowii.levelsql.protocol.mysql

abstract class MysqlCommandType : MysqlPacket() {
    var commandType: Byte = 0 // command type
}