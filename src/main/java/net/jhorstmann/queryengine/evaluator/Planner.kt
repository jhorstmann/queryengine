package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.Query
import net.jhorstmann.queryengine.data.TableRegistry
import net.jhorstmann.queryengine.operator.FilterOperator
import net.jhorstmann.queryengine.operator.GlobalAggregationOperator
import net.jhorstmann.queryengine.operator.Operator
import net.jhorstmann.queryengine.operator.ProjectionOperator
import java.lang.IllegalStateException

private fun initialPlan(tableRegistry: TableRegistry, query: Query): LogicalNode {
    val tableName = query.from
    val schema = tableRegistry.getSchema(tableName)
    val scan = LogicalScanNode(tableName, schema)
    val filter = query.filter?.let { LogicalFilterNode(scan, it) } ?: scan
    val projection = LogicalProjectionNode(filter, query.select)

    return projection
}


fun buildLogicalPlan(tableRegistry: TableRegistry, query: Query): LogicalNode {
    val plan = initialPlan(tableRegistry, query)

    val resolvedPlan = resolveSchema(plan)
    val checkedPlan = typeCheck(resolvedPlan)
    val aggregatedPlan = rewriteAggregates(checkedPlan)

    return aggregatedPlan
}

fun buildPhysicalPlan(tableRegistry: TableRegistry, plan: LogicalNode, mode: Mode = Mode.INTERPRETER): Operator {
    return when (plan) {
        is LogicalScanNode -> tableRegistry.getTable(plan.table).getScanOperator(plan.schema)
        is LogicalFilterNode -> {
            val source = buildPhysicalPlan(tableRegistry, plan.source, mode)
            FilterOperator(source, compileExpression(plan.filter, mode))
        }
        is LogicalProjectionNode -> {
            val source = buildPhysicalPlan(tableRegistry, plan.source, mode)
            ProjectionOperator(source, plan.expressions.map { compileExpression(it, mode) })
        }
        is LogicalAggregationNode -> {
            val source = buildPhysicalPlan(tableRegistry, plan.source, mode)
            if (plan.groupBy.isNotEmpty()) {
                throw IllegalStateException("Group by operator not yet supported")
            }
            val initAccumulators = { initAccumulators(plan.aggregateFunctions) }
            GlobalAggregationOperator(source, plan.aggregate.map { compileExpression(it, mode) }, initAccumulators)
        }
    }
}