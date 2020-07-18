package com.zoowii.levelsql.protocol.mysql

object ServerStatus {
    const val IN_TRANS = 1
    const val AUTOCOMMIT = 1 shl 1
    const val MORE_RESULTS_EXISTS = 1 shl 3
    const val QUERY_NO_GOOD_INDEX_USED = 1 shl 4
    const val NO_GOOD_INDEX_USED = QUERY_NO_GOOD_INDEX_USED
    const val QUERY_NO_INDEX_USED = 1 shl 5
    const val NO_INDEX_USED = QUERY_NO_INDEX_USED
    const val CURSOR_EXISTS = 1 shl 6
    const val LAST_ROW_SENT = 1 shl 7
    const val DB_DROPPED = 1 shl 8
    const val NO_BACKSLASH_ESCAPES = 1 shl 9
    const val METADATA_CHANGED = 1 shl 10
    const val QUERY_WAS_SLOW = 1 shl 11
    const val PS_OUT_PARAMS = 1 shl 12
    const val IN_TRANS_READONLY = 1 shl 13
    const val SESSION_STATE_CHANGED = 1 shl 14
}