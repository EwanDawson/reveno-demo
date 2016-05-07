package net.lazygun.experiment.reveno.demo.kotlin

import java.util.*

data class Entity<T> private constructor(val identifier: String, val version: Long, val deleted: Boolean, val value: T, val type: String) {
    constructor(value: T, type: String) : this("$type:${UUID.randomUUID()}", 0L, false, value, type)
    fun update(mutator: (T) -> T) : Entity<T> {
        check(!deleted)
        return copy(value = mutator(value), version = version + 1)
    }
    fun delete() : Entity<T> {
        check(!deleted)
        return copy(version = version + 1, deleted = true)
    }
}

