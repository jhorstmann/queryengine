package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import java.lang.IllegalStateException
import java.lang.RuntimeException

class InvalidAggregatesException(msg: String) : RuntimeException(msg)

internal fun rewriteAggregates(plan: LogicalNode): LogicalNode {
    return when (plan) {
        is LogicalScanNode -> plan
        is LogicalFilterNode -> {
            if (plan.filter.accept(CountAggregates) > 0) {
                throw InvalidAggregatesException("Aggregate expressions not allowed in where clause")
            }
            plan
        }
        is LogicalProjectionNode -> {
            val source = rewriteAggregates(plan.source)

            val classifiedExpressions = plan.expressions.map { it to it.accept(CountAggregates) }

            val aggregateCount = classifiedExpressions.sumBy { it.second }
            val groupCount = classifiedExpressions.count { it.second == 0 }

            if (aggregateCount == 0) {
                LogicalProjectionNode(source, plan.expressions)
            } else {
                val rewriteAggregates = RewriteAggregates(groupCount)

                val aggregateInput = ArrayList<Expression>(groupCount + aggregateCount)
                val groupByFinish = classifiedExpressions.map { (expr, count) ->
                    if (count > 0) {
                        expr.accept(rewriteAggregates)
                    } else {
                        val index = aggregateInput.size
                        aggregateInput.add(expr)
                        ColumnExpression("_$index", index, expr.dataType)
                    }
                }
                aggregateInput.addAll(rewriteAggregates.aggregateInputs)
                val aggregateFunctions = rewriteAggregates.aggregateFunctions

                val input = LogicalProjectionNode(source, aggregateInput)
                val aggregate = LogicalAggregationNode(input, groupCount, aggregateFunctions)

                LogicalProjectionNode(aggregate, groupByFinish)
            }
        }
        is LogicalOrderByNode -> {
            val source = rewriteAggregates(plan.source)
            LogicalOrderByNode(source, plan.index)
        }
        is LogicalAggregationNode -> throw IllegalStateException("Unexpected aggregation node")
    }
}

private object CountAggregates : ExpressionVisitor<Int> {
    override fun visitIdentifier(expr: IdentifierExpression): Int = 0

    override fun visitNumericLiteral(expr: NumericLiteralExpression): Int = 0

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression): Int = 0

    override fun visitStringLiteral(expr: StringLiteralExpression): Int = 0

    override fun visitColumn(expr: ColumnExpression): Int = 0

    override fun visitFunction(expr: FunctionExpression): Int  {
        // validate all args first
        val args = expr.operands.map { it.accept(this) }
        return args.sum()
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Int {
        val containsAggregates = expr.operands.any { it.accept(this) > 0 }
        if (containsAggregates) {
            throw InvalidAggregatesException("Nested aggregates are not allowed")
        }
        return 1
    }

}

private class RewriteAggregates(val groupExpressionCount: Int) : DefaultExpressionVisitor() {

    internal val aggregateInputs = mutableListOf<Expression>()
    internal val aggregateFunctions = mutableListOf<AggregationFunction>()

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Expression {
        val ops = visitOperands(expr.operands)
        val idx = aggregateFunctions.size
        aggregateFunctions.add(expr.function)
        aggregateInputs.add(ops[0])
        return ColumnExpression(expr.function.name, groupExpressionCount + idx, expr.dataType)
    }
}
