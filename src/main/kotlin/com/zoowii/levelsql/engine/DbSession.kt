package com.zoowii.levelsql.engine

import java.util.concurrent.atomic.AtomicLong

// 一次db会话的上下文
class DbSession {
    companion object {
        private val idGen = AtomicLong()
    }
    val id = idGen.getAndIncrement()
}