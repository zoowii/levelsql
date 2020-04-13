package com.zoowii.levelsql.engine.tx

enum class TransactionState {
    Active, PartiallyCommitted, Failed, Aborted, Committed
}