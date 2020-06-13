package net.jhorstmann.queryengine.operator

import net.jhorstmann.queryengine.data.DataType
import net.jhorstmann.queryengine.data.Schema
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import java.util.function.Supplier


class CsvSourceOperator(private val parserSupplier: Supplier<CSVParser>, private val schema: Schema, private val projection: List<String>) : Operator() {
    private var parser: CSVParser? = null
    private var fields: Array<Pair<Int, DataType>>? = null
    private var iterator: Iterator<CSVRecord>? = null

    override fun open() {

        val parser = parserSupplier.get()

        val headers = parser.headerMap
        val fields = schema.fields.associateBy { it.name }

        this.fields = projection.map { name ->
            val field = fields[name]
                    ?: throw java.lang.IllegalStateException("projected field $name not found in schema")
            val idx = headers[name]
                    ?: throw java.lang.IllegalStateException("projected field $name not found in csv headers")

            idx to field.type
        }.toTypedArray()

        this.iterator = parser.iterator()

        this.parser = parser
    }

    override fun close() {
        val parser = this.parser
        if (parser != null) {
            parser.close()
            this.parser = null
            this.iterator = null
            this.fields = null
        }
    }

    override fun next(): Array<Any?>? {
        val iterator = this.iterator ?: throw IllegalStateException("Operator not initialized")
        val fields = this.fields ?: throw java.lang.IllegalStateException("Operator not initialized")

        if (iterator.hasNext()) {
            val record = iterator.next()

            val row = Array(fields.size) {
                val (i, type) = fields[it]

                @Suppress("IMPLICIT_CAST_TO_ANY")
                if (i < record.size()) {
                    val stringValue: String? = record[i]

                    if (stringValue.isNullOrEmpty()) {
                        null
                    } else {
                        when (type) {
                            DataType.STRING -> stringValue
                            DataType.BOOLEAN -> stringValue.toBoolean()
                            DataType.DOUBLE -> stringValue.toDouble()
                        }
                    }
                } else {
                    null
                }
            }

            return row
        } else {
            return null
        }
    }
}