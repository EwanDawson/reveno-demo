package net.lazygun.experiment.reveno.demo.kotlin

data class Person(val name: String, val age: Int) {
    data class Entity private constructor(val entity: net.lazygun.experiment.reveno.demo.kotlin.Entity<Person>) {
        constructor(name: String, age: Int) : this(Entity(Person(name, age), "Person"))
        fun update(mutator: (Person) -> Person) : Entity = Entity(entity.update(mutator))
        fun delete() : Entity = Entity(entity.delete())
    }
}

