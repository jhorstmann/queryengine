package net.jhorstmann.queryengine.ast

abstract class DefaultExpressionVisitor : ExpressionVisitor<Expression> {
    protected fun visitOperands(expressions: List<Expression>): List<Expression> {
        return expressions.map { it.accept(this) }
    }

    override fun visitFunction(expr: FunctionExpression): Expression {
        return expr.copy(operands = visitOperands(expr.operands))
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Expression {
        return expr.copy(operands = visitOperands(expr.operands))
    }

    override fun visitIdentifier(expr: IdentifierExpression): Expression = expr

    override fun visitNumericLiteral(expr: NumericLiteralExpression): Expression = expr

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression): Expression = expr

    override fun visitStringLiteral(expr: StringLiteralExpression): Expression = expr

    override fun visitColumn(expr: ColumnExpression): Expression = expr
}