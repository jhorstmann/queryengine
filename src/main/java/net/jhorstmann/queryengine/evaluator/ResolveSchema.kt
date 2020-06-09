package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.data.Field
import net.jhorstmann.queryengine.data.Schema
import java.lang.IllegalStateException
import java.lang.RuntimeException

class SchemaException(message: String) : RuntimeException(message)

private fun findScanNode(plan: LogicalNode): LogicalScanNode {
    return when (plan) {
        is LogicalScanNode -> plan
        is LogicalProjectionNode -> findScanNode(plan.source)
        is LogicalFilterNode -> findScanNode(plan.source)
        is LogicalAggregationNode -> throw IllegalStateException("Unexpected aggregation node in this stage")
    }
}

private fun rewritePlan(plan: LogicalNode, resolver: ResolveSchema): LogicalNode {
    return when (plan) {
        is LogicalScanNode -> LogicalScanNode(plan.table, Schema(resolver.fields))
        is LogicalProjectionNode -> {
            val expressions = plan.expressions.map { it.accept(resolver) }
            val source = rewritePlan(plan.source, resolver)
            LogicalProjectionNode(source, expressions)
        }
        is LogicalFilterNode -> {
            val filter = plan.filter.accept(resolver)
            val source = rewritePlan(plan.source, resolver)
            LogicalFilterNode(source, filter)
        }
        is LogicalAggregationNode -> throw IllegalStateException("Unexpected aggregation node in this stage")
    }
}

fun resolveSchema(plan: LogicalNode): LogicalNode {
    val schema = findScanNode(plan).schema
    val resolver = ResolveSchema(schema)

    return rewritePlan(plan, resolver)
}

private class ResolveSchema(private val schema: Schema) : DefaultExpressionVisitor() {

    internal val fields = mutableListOf<Field>()

    private fun lookupField(name: String): Pair<Int, Field> {

        val idx = fields.indexOfFirst { it.name == name }
        if (idx >= 0) {
            return idx to fields[idx]
        } else {
            val field = schema[name] ?: throw SchemaException("Could not find field $name")
            fields.add(field)
            return fields.size - 1 to field
        }
    }

    override fun visitIdentifier(expr: IdentifierExpression): Expression {
        val (idx, field) = lookupField(expr.name)
        return ColumnExpression(field.name, idx, dataType = field.type)
    }

}

