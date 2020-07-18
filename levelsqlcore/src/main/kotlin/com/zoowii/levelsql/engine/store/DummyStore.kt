package com.zoowii.levelsql.engine.store

class DummyStore : IStore {
    override fun put(key: StoreKey, value: ByteArray) {
        TODO("Not yet implemented")
    }

    override fun get(key: StoreKey): ByteArray? {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}