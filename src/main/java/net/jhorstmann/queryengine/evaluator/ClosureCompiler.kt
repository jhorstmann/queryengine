package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import java.lang.IllegalStateException


internal class ClosureCompiler : ExpressionVisitor<RowCallable> {
    companion object {
        private inline fun callable(crossinline closure: (Array<Any?>, Array<Accumulator>) -> Any?): RowCallable {
            return object : RowCallable() {
                override fun invoke(row: Array<Any?>, acc: Array<Accumulator>): Any? {
                    return closure(row, acc)
                }
            }
        }

        private inline fun unary(ops: List<RowCallable>, crossinline closure: (Any) -> Any?): RowCallable {
            val op = ops[0]
            return object : RowCallable() {
                override fun invoke(row: Array<Any?>, acc: Array<Accumulator>): Any? {
                    val a = op(row, acc) ?: return null
                    return closure(a)
                }
            }
        }

        private inline fun binary(ops: List<RowCallable>, crossinline closure: (Any, Any) -> Any?): RowCallable {
            val op1 = ops[0]
            val op2 = ops[1]
            return object : RowCallable() {
                override fun invoke(row: Array<Any?>, acc: Array<Accumulator>): Any? {
                    val a = op1(row, acc) ?: return null
                    val b = op2(row, acc) ?: return null
                    return closure(a, b)
                }
            }
        }

        private inline fun aggregate(idx: Int, input: RowCallable): RowCallable {
            return object : RowCallable() {
                override fun invoke(row: Array<Any?>, acc: Array<Accumulator>): Any? {
                    val res = input(row, acc)
                    if (res != null) {
                        acc[idx].accumulate(res)
                    }
                    return null
                }
            }
        }

        private inline fun aggregateColumn(rowidx: Int, accidx: Int): RowCallable {
            return object : RowCallable() {
                override fun invoke(row: Array<Any?>, acc: Array<Accumulator>): Any? {
                    val res = row[rowidx]
                    if (res != null) {
                        acc[accidx].accumulate(res)
                    }
                    return null
                }
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
            Function.IF -> callable { row, acc ->
                val cond = ops[0](row, acc) as Boolean?
                when {
                    cond == null -> null
                    cond -> ops[1](row, acc)
                    else -> ops[2](row, acc)
                }
            }

            Function.AND -> callable { row, acc ->
                val p = ops[0](row, acc) as Boolean?
                when {
                    p == null -> {
                        val q = ops[1](row, acc) as Boolean?
                        if (q == null) {
                            null
                        } else if (!q) {
                            false
                        } else {
                            null
                        }
                    }
                    p -> {
                        ops[1](row, acc) as Boolean?
                    }
                    else -> false
                }
            }
            Function.OR -> callable { row, acc ->
                val p = ops[0](row, acc) as Boolean?
                when {
                    p == null -> {
                        val q = ops[1](row, acc) as Boolean?
                        if (q == null) {
                            null
                        } else if (q) {
                            true
                        } else {
                            null
                        }
                    }
                    p -> true
                    else -> ops[1](row, acc) as Boolean?
                }
            }
            Function.NOT -> unary(ops) { a -> !(a as Boolean) }

            Function.UNARY_PLUS -> unary(ops) { a -> a as Double }
            Function.UNARY_MINUS -> unary(ops) { a -> -(a as Double) }
            Function.ADD -> binary(ops) { a, b -> (a as Double) + (b as Double) }
            Function.SUB -> binary(ops) { a, b -> (a as Double) - (b as Double) }
            Function.MUL -> binary(ops) { a, b -> (a as Double) * (b as Double) }
            Function.DIV -> binary(ops) { a, b -> (a as Double) / (b as Double) }
            Function.MOD -> binary(ops) { a, b -> (a as Double) % (b as Double) }

            Function.CMP_EQ -> binary(ops) { a, b -> a == b }
            Function.CMP_NE -> binary(ops) { a, b -> a != b }
            Function.CMP_LT -> binary(ops) { a, b -> (a as Double) < (b as Double) }
            Function.CMP_LE -> binary(ops) { a, b -> (a as Double) <= (b as Double) }
            Function.CMP_GE -> binary(ops) { a, b -> (a as Double) >= (b as Double) }
            Function.CMP_GT -> binary(ops) { a, b -> (a as Double) > (b as Double) }
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
