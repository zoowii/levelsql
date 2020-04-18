package com.zoowii.levelsql.engine.planner

object PlannerConfig {
    val executeEachPlannerTimeoutSeconds: Long = 300 // executor执行planner的每次fetch的超时时间
    val executeChildPlannerTimeoutSeconds: Long = 300 // planner算子等待下一个child的输出的超时时间
}