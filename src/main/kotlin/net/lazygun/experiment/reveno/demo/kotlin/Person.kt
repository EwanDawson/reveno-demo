package net.lazygun.experiment.reveno.demo.kotlin

data class Person(val name: String, val age: Int) {
    data class Entity private constructor(val versionedEntity: VersionedEntity<Person>) {
        constructor(name: String, age: Int) : this(VersionedEntity(Person(name, age), "Person"))
        fun update(mutator: (Person) -> Person) : Entity = Entity(versionedEntity.update(mutator))
        fun delete() : Entity = Entity(versionedEntity.delete())
    }
}

