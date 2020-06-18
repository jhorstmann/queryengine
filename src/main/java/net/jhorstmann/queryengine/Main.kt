package net.jhorstmann.queryengine

import net.jhorstmann.queryengine.data.*
import net.jhorstmann.queryengine.evaluator.buildLogicalPlan
import net.jhorstmann.queryengine.evaluator.buildPhysicalPlan
import net.jhorstmann.queryengine.parser.parseQuery

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

    val query = parseQuery("""
        SELECT SUM(net_price + net_shipping_cost) * 1.25
          FROM orders
         WHERE country = 'DE'
    """)

    val logicalPlan = buildLogicalPlan(registry, query)

    val physicalPlan = buildPhysicalPlan(registry, logicalPlan)

    physicalPlan.open()
    while (true) {
        val row = physicalPlan.next() ?: break
        println(row.asList())
    }
    physicalPlan.close()
}