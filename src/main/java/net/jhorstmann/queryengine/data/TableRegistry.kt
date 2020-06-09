package net.jhorstmann.queryengine.data

import java.lang.IllegalArgumentException

class TableRegistry {
    private val tables: MutableMap<String, Table> = HashMap()

    fun register(name: String, table: Table) {
        tables[name] = table
    }

    fun drop(name: String) {
        tables.remove(name)
    }

    fun getTable(name: String) = tables[name] ?:  throw IllegalArgumentException("Unknown table $name")

    fun getSchema(name: String) = getTable(name).schema
}