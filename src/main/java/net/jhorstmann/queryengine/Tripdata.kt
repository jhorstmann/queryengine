package net.jhorstmann.queryengine

import net.jhorstmann.queryengine.data.*
import java.io.File
import kotlin.system.measureTimeMillis

fun main() {
    val schema = Schema(listOf(
            Field("tip_amount", DataType.DOUBLE),
            Field("fare_amount", DataType.DOUBLE),
            Field("passenger_count", DataType.DOUBLE)))

    val table = UnivocityCsvTable(File("/home/jhorstmann/Downloads/yellow_tripdata_2019-01.csv"), schema)

    val registry = TableRegistry()
    registry.register("tripdata", table)

    // Queries from https://andygrove.io/2019/04/datafusion-0.13.0-benchmarks/

    val t1 = measureTimeMillis {
        val query = query(registry, "SELECT MIN(tip_amount), MAX(tip_amount) FROM tripdata")
        println(query.map { it.contentToString() })
    }

    println(t1 / 1000.0)

    val t2 = measureTimeMillis {
        val query = query(registry, "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM tripdata")
        println(query.map { it.contentToString() })
    }

    println(t2 / 1000.0)

}