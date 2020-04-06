package com.zoowii.levelsql.engine.store

import java.io.Closeable

interface IStore : Closeable {
     fun put(key: StoreKey, value: ByteArray)
     fun get(key: StoreKey): ByteArray?
 }