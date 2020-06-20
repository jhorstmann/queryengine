package net.jhorstmann.queryengine.data

import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.csv.CsvParserSettings
import net.jhorstmann.queryengine.operator.Operator
import java.io.File
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException

class UnivocityCsvTable(private val file: File, override val schema: Schema) : Table() {
    override fun getScanOperator(projection: List<String>): Operator {
        val settings = CsvParserSettings()
        settings.format.delimiter = ','
        settings.isHeaderExtractionEnabled = true;
        settings.selectFields(*projection.toTypedArray());

        val types = projection.map { name ->
            val field = schema.fields.find { it.name == name }
                    ?: throw IllegalArgumentException("Field $name not found in schema")
            field.type
        }.toTypedArray()

        return UnivocityCsvScanOperator(file, settings, types)
    }
}

class UnivocityCsvScanOperator(private val file: File, private val settings: CsvParserSettings, private val types: Array<DataType>) : Operator() {
    private var parser: CsvParser? = null


    override fun open() {

        val parser = CsvParser(settings)
        parser.beginParsing(file)

        this.parser = parser

    }

    override fun close() {
        val parser = this.parser
        if (parser != null) {
            parser.stopParsing()
            this.parser = null
        }
    }

    override fun next(): Array<Any?>? {
        val parser = this.parser ?: throw IllegalStateException("Operator not initialized")
        val types = this.types

        val row = parser.parseNext() ?: return null

        return Array(row.size) { i ->
            val stringValue: String? = row[i]
            if (stringValue.isNullOrEmpty()) {
                null
            } else {
                when (types[i]) {
                    DataType.STRING -> stringValue
                    DataType.BOOLEAN -> stringValue.toBoolean()
                    DataType.DOUBLE -> stringValue.toDouble()
                }
            }
        }
    }

}