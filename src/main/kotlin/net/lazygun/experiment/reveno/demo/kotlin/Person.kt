package net.lazygun.experiment.reveno.demo.kotlin

data class Person private constructor(val name: String, val age: Int, private val e: VersionedEntityDelegate) : VersionedEntity by e {
    constructor(name: String, age: Int) : this(name, age, VersionedEntityDelegate("Person"))
    override fun toString() = e.toString("name=$name,age=$age")
}

