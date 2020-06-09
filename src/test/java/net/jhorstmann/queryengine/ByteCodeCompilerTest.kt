package net.jhorstmann.queryengine

import net.jhorstmann.queryengine.ast.FunctionExpression
import net.jhorstmann.queryengine.ast.Function
import net.jhorstmann.queryengine.ast.NumericLiteralExpression
import net.jhorstmann.queryengine.evaluator.Mode
import net.jhorstmann.queryengine.evaluator.compileExpression

fun main() {
    val callable = compileExpression(FunctionExpression(Function.ADD,
            FunctionExpression(Function.ADD, NumericLiteralExpression(1.0), NumericLiteralExpression(2.0)), NumericLiteralExpression(3.0)),


            Mode.BYTECODE_COMPILER)

    val res = callable(emptyArray(), emptyArray())

    println(res)
}