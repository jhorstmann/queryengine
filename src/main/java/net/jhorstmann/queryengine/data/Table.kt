package net.jhorstmann.queryengine.data

import net.jhorstmann.queryengine.operator.Operator


abstract class Table() {
    abstract val schema: Schema
    abstract fun getScanOperator(projectedSchema: Schema): Operator

}


