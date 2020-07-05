package net.jhorstmann.queryengine

import net.jhorstmann.queryengine.data.*
import net.jhorstmann.queryengine.evaluator.Mode
import net.jhorstmann.queryengine.evaluator.buildLogicalPlan
import net.jhorstmann.queryengine.evaluator.buildPhysicalPlan
import net.jhorstmann.queryengine.operator.map
import net.jhorstmann.queryengine.parser.parseQuery
import java.util.*

fun query(registry: TableRegistry, query: String, mode: Mode = Mode.INTERPRETER): List<Array<Any?>> {
    val ast = parseQuery(query)

    val logicalPlan = buildLogicalPlan(registry, ast)

    val physicalPlan = buildPhysicalPlan(registry, logicalPlan, mode)

    return physicalPlan.map { it }
}

fun query(tableName: String, table: Table, query: String, mode: Mode = Mode.INTERPRETER): List<Array<Any?>> {
    val registry = TableRegistry()
    registry.register(tableName, table)

    return query(registry, query, mode)
}

fun main() {
    val schema = Schema(listOf(
            Field("id", DataType.STRING),
            Field("country", DataType.STRING),
            Field("net_price", DataType.DOUBLE),
            Field("net_shipping_cost", DataType.DOUBLE)))

    val table = MemoryTable(schema, listOf(
            listOf("1", "DE", 100.0, 5.0),
            listOf("2", "AT", 100.0, 5.0),
            listOf("3", "CH", 50.0, 0.0),
            listOf("4", "DE", 50.0, 0.0),
            listOf("5", "DE", 200.0, 10.0)
    ))

    val registry = TableRegistry()
    registry.register("orders", table)

    val query = """
        SELECT SUM(net_price + net_shipping_cost) * 1.25, country
          FROM orders
         ORDER BY 1
    """

    val rows = query(registry, query, Mode.BYTECODE_COMPILER)

    for (row in rows) {
        println(Arrays.toString(row))
    }
}