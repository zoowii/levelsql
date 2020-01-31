package com.zoowii.levelsql.engine.planner

import com.zoowii.levelsql.engine.DbSession

interface Planner {
    // 本次执行的db session
    fun getSession(): DbSession
    // 输出各列的名称
    fun getOutputNames(): List<String>
    fun setOutputNames(names: List<String>)
}

abstract class LogicalPlanner(private val sess: DbSession) : Planner {
    var children = mutableListOf<LogicalPlanner>()
    fun setChild(index: Int, planner: LogicalPlanner) {
        children[index] = planner
    }
    fun addChild(planner: LogicalPlanner) {
        children.add(planner)
    }
    private var outputNames = listOf<String>()
    override fun getOutputNames(): List<String> {
        return outputNames
    }
    override fun setOutputNames(names: List<String>) {
        outputNames = names
    }
    override fun getSession(): DbSession {
        return sess
    }
    fun maxOneRow(): Boolean = false

    protected fun childrenToString(): String {
        if(children.isEmpty()) {
            return ""
        }
        return "\n${children.joinToString(",\t")}"
    }
}

abstract class PhysicalPlanner(private val sess: DbSession) : Planner {
    var children = mutableListOf<PhysicalPlanner>()
    fun setChild(index: Int, planner: PhysicalPlanner) {
        children[index] = planner
    }
    fun addChild(planner: PhysicalPlanner) {
        children.add(planner)
    }
    private var outputNames = listOf<String>()
    override fun getOutputNames(): List<String> {
        return outputNames
    }
    override fun getSession(): DbSession {
        return sess
    }
    override fun setOutputNames(names: List<String>) {
        outputNames = names
    }
}