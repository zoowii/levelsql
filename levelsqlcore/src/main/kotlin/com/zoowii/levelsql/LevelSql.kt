package com.zoowii.levelsql

import org.apache.commons.cli.*
import com.zoowii.levelsql.protocol.mysql.MysqlServer
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val options = Options()

    val dataDirOption = Option("d", "data", true, "data dir path")
    dataDirOption.isRequired = true
    options.addOption(dataDirOption)

    val hostOption = Option("h", "host", true, "bind address")
    options.addOption(hostOption)

    val portOption = Option("p", "port", true, "bind port")
    options.addOption(portOption)

    val cmdLineParser = DefaultParser()
    val cmdFormatter = HelpFormatter()
    val cmd: CommandLine
    try {
        cmd = cmdLineParser.parse(options, args)
    } catch(e: ParseException) {
        println(e.message)
        cmdFormatter.printHelp("levelsql-help", options)
        exitProcess(1)
        return
    }

    val host = cmd.getOptionValue("host", "127.0.0.1")
    val port = Integer.parseInt(cmd.getOptionValue("port", "3000"))
    val dataDir = cmd.getOptionValue("data")
    if(dataDir.isNullOrBlank()) {
        println("please set -data option")
        exitProcess(1)
        return
    }

    val server = MysqlServer()
    server.initServer(dataDir)
    try {
        server.startLoop(host, port)
    } finally {
        server.shutdown()
    }
    println("levelsql server shutdown gracefully")
}