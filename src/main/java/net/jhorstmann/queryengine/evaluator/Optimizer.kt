package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.ColumnExpression
import net.jhorstmann.queryengine.data.Schema

fun removeUnnededProjections(plan: LogicalNode) : LogicalNode {
    return when (plan) {
        is LogicalScanNode -> plan
        is LogicalFilterNode -> {
            val source = removeUnnededProjections(plan.source)
            LogicalFilterNode(source, plan.filter)
        }
        is LogicalAggregationNode -> {
            val source = removeUnnededProjections(plan.source)
            LogicalAggregationNode(source, plan.groupCount, plan.aggregateFunctions)
        }
        is LogicalOrderByNode -> {
            val source = removeUnnededProjections(plan.source)
            LogicalOrderByNode(source, plan.index)
        }
        is LogicalProjectionNode -> {
            val source = removeUnnededProjections(plan.source)

            val indices = plan.expressions.mapNotNull { expr ->
                if (expr is ColumnExpression) {
                    expr.index
                } else {
                    null
                }
            }

            if (indices.size == plan.expressions.size) {
                val isIdentity = indices.withIndex().all { (i, j) -> i == j }
                if (isIdentity) {
                    source
                } else if (source is LogicalScanNode) {
                    // TODO: might not be an optimization if the same field is selected more than once from csv file
                    val pushdown = Schema(indices.map { source.schema.fields[it] })
                    LogicalScanNode(source.table, pushdown)
                } else {
                    LogicalProjectionNode(source, plan.expressions)
                }
            } else {
                LogicalProjectionNode(source, plan.expressions)
            }
        }
    }
}