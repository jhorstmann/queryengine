package net.jhorstmann.queryengine

import net.jhorstmann.queryengine.data.*
import net.jhorstmann.queryengine.evaluator.Mode
import net.jhorstmann.queryengine.evaluator.buildLogicalPlan
import net.jhorstmann.queryengine.evaluator.buildPhysicalPlan
import net.jhorstmann.queryengine.operator.forEach
import net.jhorstmann.queryengine.parser.parseQuery
import java.io.File
import kotlin.system.measureTimeMillis

fun main() {
    val schema = Schema(listOf(
            Field("tip_amount", DataType.DOUBLE)))

    val table = UnivocityCsvTable(File("/home/jhorstmann/Downloads/yellow_tripdata_2019-01.csv"), schema)

    val registry = TableRegistry()
    registry.register("tripdata", table)

    val query = parseQuery("""
        SELECT MIN(tip_amount), MAX(tip_amount) FROM tripdata
    """)

    val logicalPlan = buildLogicalPlan(registry, query)

    val physicalPlan = buildPhysicalPlan(registry, logicalPlan, Mode.BYTECODE_COMPILER)

    do {
        val t = measureTimeMillis {
            physicalPlan.forEach { println(it.toList()) }
        }

        println(t / 1000.0)
    } while (true)
}