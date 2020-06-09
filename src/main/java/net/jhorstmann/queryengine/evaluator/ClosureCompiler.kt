package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import java.lang.IllegalStateException


internal class ClosureCompiler : ExpressionVisitor<RowCallable> {
    companion object {
        private inline fun callable(crossinline closure: (Array<Any?>, Array<Accumulator>) -> Any?): RowCallable {
            return object : RowCallable() {
                override fun invoke(row: Array<Any?>, accumulators: Array<Accumulator>): Any? {
                    return closure(row, accumulators)
                }
            }
        }

        private inline fun aggregate(idx: Int, input: RowCallable): RowCallable {
            return callable { row, acc ->
                val res = input(row, acc)
                if (res != null) {
                    acc[idx].accumulate(res)
                }
                null
            }
        }

        private inline fun aggregateColumn(idx: Int): RowCallable {
            return callable { row, acc ->
                val res = row[idx]
                if (res != null) {
                    acc[idx].accumulate(res)
                }
                null
            }
        }

    }

    override fun visitIdentifier(expr: IdentifierExpression): RowCallable {
        throw IllegalStateException("Identifier not expected during evaluation")
    }

    override fun visitNumericLiteral(expr: NumericLiteralExpression): RowCallable {
        val value = expr.value
        return callable { _, _ -> value }
    }

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression): RowCallable {
        val value = expr.value
        return callable { _, _ -> value }
    }

    override fun visitStringLiteral(expr: StringLiteralExpression): RowCallable {
        val value = expr.value
        return callable { _, _ -> value }
    }

    override fun visitColumn(expr: ColumnExpression): RowCallable {
        val idx = expr.index
        return callable { row, _ -> row[idx] }
    }

    override fun visitFunction(expr: FunctionExpression): RowCallable {

        val ops = expr.operands.map { it.accept(this) }

        return when (expr.function) {
            Function.IF -> callable { row, acc -> if (ops[0](row, acc) as Boolean) ops[1](row, acc) else ops[2](row, acc) }

            Function.AND -> callable { row, acc -> ops[0](row, acc) as Boolean && ops[1](row, acc) as Boolean }
            Function.OR -> callable { row, acc -> ops[0](row, acc) as Boolean || ops[1](row, acc) as Boolean }
            Function.NOT -> callable { row, acc -> !(ops[0](row, acc) as Boolean) }

            Function.UNARY_PLUS -> ops[0]
            Function.UNARY_MINUS -> callable { row, acc -> -(ops[0](row, acc) as Double) }
            Function.ADD -> callable { row, acc -> ops[0](row, acc) as Double + ops[1](row, acc) as Double }
            Function.SUB -> callable { row, acc -> ops[0](row, acc) as Double - ops[1](row, acc) as Double }
            Function.MUL -> callable { row, acc -> ops[0](row, acc) as Double * ops[1](row, acc) as Double }
            Function.DIV -> callable { row, acc -> ops[0](row, acc) as Double / ops[1](row, acc) as Double }
            Function.MOD -> callable { row, acc -> ops[0](row, acc) as Double % ops[1](row, acc) as Double }

            Function.CMP_EQ -> callable { row, acc -> ops[0](row, acc) as Double == ops[1](row, acc) as Double }
            Function.CMP_NE -> callable { row, acc -> ops[0](row, acc) as Double != ops[1](row, acc) as Double }
            Function.CMP_LT -> callable { row, acc -> (ops[0](row, acc) as Double) < (ops[1](row, acc) as Double) }
            Function.CMP_LE -> callable { row, acc -> ops[0](row, acc) as Double <= ops[1](row, acc) as Double }
            Function.CMP_GE -> callable { row, acc -> ops[0](row, acc) as Double >= ops[1](row, acc) as Double }
            Function.CMP_GT -> callable { row, acc -> ops[0](row, acc) as Double > ops[1](row, acc) as Double }


        }
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): RowCallable {

        // all aggregation functions take only a single argument
        val op = expr.operands[0]

        if (op is ColumnExpression) {
            return when (expr.function) {
                AggregationFunction.COUNT -> aggregateColumn(expr.resultIndex)
                AggregationFunction.SUM -> aggregateColumn(expr.resultIndex)
                AggregationFunction.MIN -> aggregateColumn(expr.resultIndex)
                AggregationFunction.MAX -> aggregateColumn(expr.resultIndex)
                AggregationFunction.ANY -> aggregateColumn(expr.resultIndex)
                AggregationFunction.ALL -> aggregateColumn(expr.resultIndex)
            }
        } else {
            val input = op.accept(this)
            return when (expr.function) {
                AggregationFunction.COUNT -> aggregate(expr.resultIndex, input)
                AggregationFunction.SUM -> aggregate(expr.resultIndex, input)
                AggregationFunction.MIN -> aggregate(expr.resultIndex, input)
                AggregationFunction.MAX -> aggregate(expr.resultIndex, input)
                AggregationFunction.ANY -> aggregate(expr.resultIndex, input)
                AggregationFunction.ALL -> aggregate(expr.resultIndex, input)
            }
        }
    }
}
