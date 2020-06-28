package net.jhorstmann.queryengine

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import net.jhorstmann.queryengine.data.DataType
import net.jhorstmann.queryengine.evaluator.Accumulator
import net.jhorstmann.queryengine.evaluator.Mode
import net.jhorstmann.queryengine.evaluator.SumAccumulator
import net.jhorstmann.queryengine.evaluator.compileExpression

fun main() {

    val callable = compileExpression(
            FunctionExpression(Function.IF,
                    listOf(FunctionExpression(Function.CMP_EQ, listOf(NumericLiteralExpression(1.0), NumericLiteralExpression(1.0)), DataType.BOOLEAN),
                            StringLiteralExpression("YES"),
                            StringLiteralExpression("NO")), DataType.BOOLEAN),


            Mode.BYTECODE_COMPILER)

    val row: Array<Any?> = arrayOf<Any?>()
    val res = callable(row)

    println(res)
    //println(row.toList())
    //println(acc.map { it.finish() })
}