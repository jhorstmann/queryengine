package net.jhorstmann.queryengine.ast

data class Query(val select: List<Expression>, val from: String, val filter: Expression?)