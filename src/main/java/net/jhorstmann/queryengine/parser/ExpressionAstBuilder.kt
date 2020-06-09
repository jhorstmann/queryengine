package net.jhorstmann.queryengine.parser

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import java.lang.IllegalStateException
import java.util.*

internal object ExpressionAstBuilder : QueryBaseVisitor<Expression>() {
    private val functions = Function.values().associateBy { it.name }
    private val aggregationFunctions = AggregationFunction.values().associateBy { it.name }
    private val comparisons = mapOf(
            "="  to Function.CMP_EQ,
            "==" to Function.CMP_EQ,
            "!=" to Function.CMP_NE,
            "<>" to Function.CMP_NE,
            "<"  to Function.CMP_LT,
            "<=" to Function.CMP_LE,
            ">=" to Function.CMP_GE,
            ">"  to Function.CMP_GT
    )

    override fun visitSingleExpression(ctx: QueryParser.SingleExpressionContext): Expression {
        return ctx.expression().accept(this)
    }

    override fun visitNumericLiteralExpression(ctx: QueryParser.NumericLiteralExpressionContext): Expression {
        return NumericLiteralExpression(ctx.literal.text.toDouble())
    }

    override fun visitStringLiteralExpression(ctx: QueryParser.StringLiteralExpressionContext): Expression {
        return StringLiteralExpression(unquoteSingle(ctx.literal.text))
    }

    override fun visitBooleanLiteralExpression(ctx: QueryParser.BooleanLiteralExpressionContext): Expression {
        val ch = ctx.literal.text[0]
        return BooleanLiteralExpression(ch == 't' || ch == 'T')
    }

    override fun visitColumnNameExpression(ctx: QueryParser.ColumnNameExpressionContext): Expression {
        val identifier = ctx.identifier()

        return IdentifierExpression(unquote(identifier))
    }

    override fun visitIfExpression(ctx: QueryParser.IfExpressionContext): Expression {
        val operands = ctx.expression().map { it.accept(this) }
        return FunctionExpression(Function.IF, operands)
    }

    override fun visitFunctionExpression(ctx: QueryParser.FunctionExpressionContext): Expression {
        val operands = ctx.expression().map { it.accept(this) }

        val name = ctx.functionName.IDENTIFIER().text.toUpperCase(Locale.ROOT)
        val function = functions[name]
        if (function != null) {
            return FunctionExpression(function, operands)
        } else {
            val aggFunction = aggregationFunctions[name]
            if (aggFunction != null) {
                return AggregationFunctionExpression(aggFunction, operands)
            } else {
                throw IllegalStateException("Unsupported function $name")
            }
        }
    }

    override fun visitCompareExpression(ctx: QueryParser.CompareExpressionContext): Expression {
        val operator = ctx.operator.text
        val function = comparisons[operator] ?: throw SyntaxException("Unexpected operator $operator")
        val operands = ctx.expression().map { it.accept(this) }

        return FunctionExpression(function, operands)
    }

    override fun visitOrExpression(ctx: QueryParser.OrExpressionContext): Expression {
        val operands = ctx.expression().map { it.accept(this) }
        return FunctionExpression(Function.OR, operands)
    }

    override fun visitAndExpression(ctx: QueryParser.AndExpressionContext): Expression {
        val operands = ctx.expression().map { it.accept(this) }
        return FunctionExpression(Function.AND, operands)
    }

    override fun visitAddExpression(ctx: QueryParser.AddExpressionContext): Expression {
        val function = when (ctx.operator.text) {
            "+" -> Function.ADD
            "-" -> Function.SUB
            else -> throw SyntaxException("Unexpected operator")
        }
        val operands = ctx.expression().map { it.accept(this) }
        return FunctionExpression(function, operands)
    }

    override fun visitUnaryExpression(ctx: QueryParser.UnaryExpressionContext): Expression {
        val function = when (ctx.operator.text[0]) {
            '+' -> Function.UNARY_PLUS
            '-' -> Function.UNARY_MINUS
            'n', 'N' -> Function.NOT
            else -> throw SyntaxException("Unexpected operator")
        }
        val operand = ctx.expression().accept(this)
        // TODO: maybe move to separate optimization phase
        if (operand is NumericLiteralExpression) {
            if (function == Function.UNARY_MINUS) {
                return NumericLiteralExpression(-operand.value)
            } else if (function == Function.UNARY_PLUS) {
                return operand
            }
        }
        return FunctionExpression(function, listOf(operand))
    }

    override fun visitMulExpression(ctx: QueryParser.MulExpressionContext): Expression {
        val function = when (ctx.operator.text) {
            "*" -> Function.MUL
            "/" -> Function.DIV
            "%" -> Function.MOD
            else -> throw SyntaxException("Unexpected operator")
        }
        val operands = ctx.expression().map { it.accept(this) }
        return FunctionExpression(function, operands)
    }

    override fun visitNestedExpression(ctx: QueryParser.NestedExpressionContext): Expression {
        return ctx.expression().accept(this)
    }

    override fun visitIdentifier(ctx: QueryParser.IdentifierContext): Expression {
        return visitChildren(ctx)
    }
}

