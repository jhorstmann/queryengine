package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.Query
import net.jhorstmann.queryengine.data.TableRegistry
import net.jhorstmann.queryengine.operator.*

private fun initialPlan(tableRegistry: TableRegistry, query: Query): LogicalNode {
    val tableName = query.from
    val schema = tableRegistry.getSchema(tableName)
    val scan = LogicalScanNode(tableName, schema)
    val filter = query.filter?.let { LogicalFilterNode(scan, it) } ?: scan
    val projection = LogicalProjectionNode(filter, query.select)
    val orderBy = query.orderByColumn?.let { LogicalOrderByNode(projection, it) } ?: projection

    return orderBy
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
        is LogicalScanNode -> tableRegistry.getTable(plan.table).getScanOperator(plan.schema.fields.map { it.name })
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
            val initAccumulators = { initAccumulators(plan.aggregateFunctions) }

            val aggregates = plan.aggregate.map { compileExpression(it, mode) }

            if (plan.groupBy.isNotEmpty()) {
                val groupBy = plan.groupBy.map { compileExpression(it, mode) }
                GroupByAggregationOperator(source, groupBy, aggregates, initAccumulators)
            } else {
                GlobalAggregationOperator(source, aggregates, initAccumulators)
            }
        }
        is LogicalOrderByNode -> {
            val source = buildPhysicalPlan(tableRegistry, plan.source, mode)
            OrderByOperator(source, plan.index - 1)
        }
    }
}