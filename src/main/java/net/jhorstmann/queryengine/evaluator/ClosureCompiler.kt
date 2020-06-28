package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import java.lang.IllegalStateException


internal class ClosureCompiler : ExpressionVisitor<RowCallable> {
    companion object {
        private inline fun callable(crossinline closure: (Array<Any?>) -> Any?): RowCallable {
            return object : RowCallable() {
                override fun invoke(row: Array<Any?>): Any? {
                    return closure(row)
                }
            }
        }

        private inline fun unary(ops: List<RowCallable>, crossinline closure: (Any) -> Any?): RowCallable {
            val op = ops[0]
            return object : RowCallable() {
                override fun invoke(row: Array<Any?>): Any? {
                    val a = op(row) ?: return null
                    return closure(a)
                }
            }
        }

        private inline fun binary(ops: List<RowCallable>, crossinline closure: (Any, Any) -> Any?): RowCallable {
            val op1 = ops[0]
            val op2 = ops[1]
            return object : RowCallable() {
                override fun invoke(row: Array<Any?>): Any? {
                    val a = op1(row) ?: return null
                    val b = op2(row) ?: return null
                    return closure(a, b)
                }
            }
        }
    }

    override fun visitIdentifier(expr: IdentifierExpression): RowCallable {
        throw IllegalStateException("Identifier not expected during evaluation")
    }

    override fun visitNumericLiteral(expr: NumericLiteralExpression): RowCallable {
        val value = expr.value
        return callable { value }
    }

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression): RowCallable {
        val value = expr.value
        return callable { value }
    }

    override fun visitStringLiteral(expr: StringLiteralExpression): RowCallable {
        val value = expr.value
        return callable { value }
    }

    override fun visitColumn(expr: ColumnExpression): RowCallable {
        val idx = expr.index
        return callable { row -> row[idx] }
    }

    override fun visitFunction(expr: FunctionExpression): RowCallable {

        val ops = expr.operands.map { it.accept(this) }

        return when (expr.function) {
            Function.IF -> callable { row ->
                val cond = ops[0](row) as Boolean?
                when {
                    cond == null -> null
                    cond -> ops[1](row)
                    else -> ops[2](row)
                }
            }

            Function.AND -> callable { row ->
                val p = ops[0](row) as Boolean?
                when {
                    p == null -> {
                        val q = ops[1](row) as Boolean?
                        if (q == null) {
                            null
                        } else if (!q) {
                            false
                        } else {
                            null
                        }
                    }
                    p -> {
                        ops[1](row) as Boolean?
                    }
                    else -> false
                }
            }
            Function.OR -> callable { row ->
                val p = ops[0](row) as Boolean?
                when {
                    p == null -> {
                        val q = ops[1](row) as Boolean?
                        if (q == null) {
                            null
                        } else if (q) {
                            true
                        } else {
                            null
                        }
                    }
                    p -> true
                    else -> ops[1](row) as Boolean?
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
        throw IllegalStateException("Unexpected aggregation expression in expression compiler")
    }
}
