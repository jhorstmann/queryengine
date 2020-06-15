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

            val input = plan.expressions.map { it to it.accept(DetectAggregates) }

            val aggregateCount = input.count { it.second }
            val groupCount = input.count { !it.second }

            if (aggregateCount == 0) {
                LogicalProjectionNode(source, plan.expressions)
            } else {
                val rewriteAggregates = RewriteAggregates(groupCount)

                val groupByExpressions = ArrayList<Expression>(groupCount)
                val projection = input.map { (expr, isAggregate) ->
                    if (isAggregate) {
                        expr.accept(rewriteAggregates)
                    } else {
                        val index = groupByExpressions.size
                        groupByExpressions.add(expr)
                        ColumnExpression("_$index", index, expr.dataType)
                    }
                }

                val aggregateExpressions = rewriteAggregates.aggregateFunctions
                val aggregateFunctions = aggregateExpressions.map { it.function }

                val aggregateNode = LogicalAggregationNode(source, groupByExpressions, aggregateExpressions, aggregateFunctions)

                LogicalProjectionNode(aggregateNode, projection)
            }
        }
        is LogicalOrderByNode -> {
            val source = rewriteAggregates(plan.source)
            LogicalOrderByNode(source, plan.index)
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

    override fun visitFunction(expr: FunctionExpression): Boolean  {
        // validate all args first
        val args = expr.operands.map { it.accept(this) }
        return args.any { it }
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Boolean {
        val containsAggregates = expr.operands.any { it.accept(this) }
        if (containsAggregates) {
            throw InvalidAggregatesException("Nested aggregates are not allowed")
        }
        return true
    }

}

private class RewriteAggregates(val groupExpressionCount: Int) : DefaultExpressionVisitor() {

    internal val aggregateFunctions = mutableListOf<AggregationFunctionExpression>()

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Expression {
        val ops = visitOperands(expr.operands)
        val idx = aggregateFunctions.size
        val res = expr.with(operands = ops, accumulatorIndex = idx)
        aggregateFunctions.add(res)
        return ColumnExpression(res.function.name, groupExpressionCount + idx, res.dataType)
    }
}
