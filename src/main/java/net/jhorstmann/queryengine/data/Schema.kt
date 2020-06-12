package net.jhorstmann.queryengine.data

enum class DataType {
    STRING, DOUBLE, BOOLEAN
}

data class Field(val name: String, val type: DataType)

data class Schema(val fields: List<Field>) {
    private val byName = fields.associateBy { it.name }

    operator fun get(name: String): Field? = byName[name]
}
