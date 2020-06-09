package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import java.lang.IllegalStateException
import java.lang.RuntimeException

class InvalidAggregatesException(msg: String) : RuntimeException(msg)

internal fun rewriteAggregates(plan: LogicalNode): LogicalNode {
    return when (plan) {
        is LogicalScanNode -> plan
        is LogicalFilterNode -> {
            if (plan.filter.accept(DetectAggregates)) {
                throw InvalidAggregatesException("Aggregate expressions not allowed in where clause")
            }
            plan
        }
        is LogicalProjectionNode -> {
            val source = rewriteAggregates(plan.source)

            val (inputAggregates, inputGroups) = plan.expressions.partition { it.accept(DetectAggregates) }

            if (inputAggregates.isEmpty()) {
                LogicalProjectionNode(source, inputGroups)
            } else {
                val visitor = RewriteAggregates(inputGroups.size)
                val expressions = inputAggregates.map { it.accept(visitor) }
                val aggregateExpressions = visitor.aggregateFunctions
                val functions = aggregateExpressions.map { it.function }
                val groupByExpressions = inputGroups.mapIndexed { i, e -> ColumnExpression("_$i", i, e.dataType) }

                val aggregateNode = LogicalAggregationNode(source, groupByExpressions, aggregateExpressions, functions)

                LogicalProjectionNode(aggregateNode, expressions)
            }
        }
        is LogicalAggregationNode -> throw IllegalStateException("Unexpected aggregation node")
    }
}

private object DetectAggregates : ExpressionVisitor<Boolean> {
    override fun visitIdentifier(expr: IdentifierExpression): Boolean = false

    override fun visitNumericLiteral(expr: NumericLiteralExpression): Boolean = false

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression): Boolean = false

    override fun visitStringLiteral(expr: StringLiteralExpression): Boolean = false

    override fun visitColumn(expr: ColumnExpression): Boolean = false

    override fun visitFunction(expr: FunctionExpression): Boolean = false

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Boolean {
        val containsAggregates = expr.operands.any { it.accept(this) }
        if (containsAggregates) {
            throw InvalidAggregatesException("Nested aggregates are not supported")
        }
        return true
    }

}

private class RewriteAggregates(val groupExpressionCount: Int = 0) : DefaultExpressionVisitor() {

    internal val aggregateFunctions = mutableListOf<AggregationFunctionExpression>()

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Expression {
        val ops = visitOperands(expr.operands)
        val idx = groupExpressionCount + aggregateFunctions.size
        val res = expr.with(operands = ops, resultIndex = idx)
        aggregateFunctions.add(res)
        return ColumnExpression(res.function.name, idx, res.dataType)
    }
}
