package net.jhorstmann.queryengine.data

import net.jhorstmann.queryengine.operator.CsvSourceOperator
import net.jhorstmann.queryengine.operator.Operator
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.File
import java.io.FileReader
import java.nio.charset.StandardCharsets
import java.util.function.Supplier

class CsvTable(private val file: File, override val schema: Schema): Table() {

    private val format = CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .withDelimiter(',')
            .withIgnoreEmptyLines(true)

    override fun getScanOperator(projection: List<String>): Operator {
        val parserSupplier = Supplier<CSVParser>() {
            val reader = FileReader(file, StandardCharsets.UTF_8)
            val parser = format.parse(reader)

            parser
        }

        return CsvSourceOperator(parserSupplier, schema, projection)
    }
}