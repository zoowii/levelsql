package com.zoowii.levelsql.engine.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory

inline fun <reified T> T.logger(): Logger {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug")
    return LoggerFactory.getLogger(T::class.java)
}