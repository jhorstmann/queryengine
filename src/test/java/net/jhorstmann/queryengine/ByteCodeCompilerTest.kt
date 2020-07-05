package net.jhorstmann.queryengine

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import net.jhorstmann.queryengine.data.DataType
import net.jhorstmann.queryengine.data.Field
import net.jhorstmann.queryengine.data.Schema
import net.jhorstmann.queryengine.evaluator.*
import net.jhorstmann.queryengine.operator.MemorySourceOperator
import net.jhorstmann.queryengine.operator.map

fun main() {

    val scan = MemorySourceOperator(
            intArrayOf(0, 1, 2),
            listOf(
                    listOf(10.0, 11.0, 12.0),
                    listOf(20.0, 21.0, 22.0),
                    listOf(30.0, 31.0, 32.0)
            ))

    val projection = LogicalProjectionNode(
            LogicalScanNode("table", Schema(listOf(
                    Field("foo", DataType.DOUBLE),
                    Field("bar", DataType.DOUBLE),
                    Field("baz", DataType.DOUBLE)))),
            listOf(
                    FunctionExpression(Function.ADD, listOf(ColumnExpression("foo", 0, DataType.DOUBLE), ColumnExpression("bar", 1, DataType.DOUBLE))),
                    FunctionExpression(Function.ADD, listOf(ColumnExpression("bar", 1, DataType.DOUBLE), ColumnExpression("baz", 2, DataType.DOUBLE)))))

    val projectionOperator = compileProjection(projection, scan)


    val result = projectionOperator.map { it.toList() }


    println(result)
    //println(row.toList())
    //println(acc.map { it.finish() })
}