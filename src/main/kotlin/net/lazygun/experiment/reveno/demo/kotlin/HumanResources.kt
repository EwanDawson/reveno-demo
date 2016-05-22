package net.lazygun.experiment.reveno.demo.kotlin

data class Employee private constructor(val name: String, val salary: Int, private val e: VersionedEntityDelegate) : VersionedEntity by e {
    constructor(name: String, salary: Int) : this(name, salary, VersionedEntityDelegate("Employee"))
    fun raise(amount: Int) = copy(salary = salary + amount, e = e.update())
    fun retire() = copy(e = e.delete())
    override fun toString() = e.toString("name=$name,salary=$salary")
}

fun main(args: Array<String>) {
    val employee = Employee("Me", 123)
    println(employee)
    val bonusTime = employee.raise(100)
    println(bonusTime)
    val fired = bonusTime.retire()
    println(fired)
}