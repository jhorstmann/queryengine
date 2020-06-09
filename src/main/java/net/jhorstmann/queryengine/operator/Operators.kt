package net.jhorstmann.queryengine.operator

import java.io.Closeable

abstract class Operator: Closeable {
    abstract fun open()

    abstract override fun close()

    abstract fun next(): Array<Any?>?
}

inline fun Operator.forEach(consumer:(Array<Any?>) -> Unit) {
    this.open()

    this.use {
        while (true) {
            val row = next() ?: break
            consumer(row)
        }
    }

}

fun <T> Operator.map(mapper: (Array<Any?>) -> T): List<T> {
    val result = ArrayList<T>()
    forEach { result.add(mapper(it)) }
    return result
}

