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

        private inline fun aggregateColumn(rowidx: Int, accidx: Int): RowCallable {
            return callable { row, acc ->
                val res = row[rowidx]
                if (res != null) {
                    acc[accidx].accumulate(res)
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

        // list all functions separately to inline separate closure implementations
        if (op is ColumnExpression) {
            val rowidx = op.index
            val accidx = expr.accumulatorIndex
            return when (expr.function) {
                AggregationFunction.COUNT -> aggregateColumn(rowidx, accidx)
                AggregationFunction.SUM -> aggregateColumn(rowidx, accidx)
                AggregationFunction.AVG -> aggregateColumn(rowidx, accidx)
                AggregationFunction.MIN -> aggregateColumn(rowidx, accidx)
                AggregationFunction.MAX -> aggregateColumn(rowidx, accidx)
                AggregationFunction.ANY -> aggregateColumn(rowidx, accidx)
                AggregationFunction.ALL -> aggregateColumn(rowidx, accidx)
            }
        } else {
            val input = op.accept(this)
            return when (expr.function) {
                AggregationFunction.COUNT -> aggregate(expr.accumulatorIndex, input)
                AggregationFunction.SUM -> aggregate(expr.accumulatorIndex, input)
                AggregationFunction.AVG -> aggregate(expr.accumulatorIndex, input)
                AggregationFunction.MIN -> aggregate(expr.accumulatorIndex, input)
                AggregationFunction.MAX -> aggregate(expr.accumulatorIndex, input)
                AggregationFunction.ANY -> aggregate(expr.accumulatorIndex, input)
                AggregationFunction.ALL -> aggregate(expr.accumulatorIndex, input)
            }
        }
    }
}
