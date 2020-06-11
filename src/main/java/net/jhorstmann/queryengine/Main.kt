package net.jhorstmann.queryengine

import net.jhorstmann.queryengine.data.*
import net.jhorstmann.queryengine.evaluator.Mode
import net.jhorstmann.queryengine.evaluator.buildLogicalPlan
import net.jhorstmann.queryengine.evaluator.buildPhysicalPlan
import net.jhorstmann.queryengine.operator.forEach
import net.jhorstmann.queryengine.parser.parseQuery

fun main() {
    val schema = Schema(listOf(Field("foo", DataType.DOUBLE), Field("bar", DataType.DOUBLE)))

    val table = MemoryTable(schema, (0 until 1_000_000).map {
        listOf((it / 1000).toDouble(), (it / 1000).toDouble())
    })

    val registry = TableRegistry()
    registry.register("table", table)

    val query = parseQuery("SELECT SUM(foo + bar) / 2 FROM table")

    val logicalPlan = buildLogicalPlan(registry, query)

    println(logicalPlan)

    val physicalPlan = buildPhysicalPlan(registry, logicalPlan, Mode.BYTECODE_COMPILER)

    physicalPlan.forEach {
        println(it.asList())
    }
}