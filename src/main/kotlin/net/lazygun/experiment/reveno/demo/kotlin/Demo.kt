package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.core.Engine
import org.reveno.atp.utils.MapUtils.map
import java.time.Instant
import java.util.*

interface Identified { val identifier: UUID }

interface Versioned { val version: Long }

interface Entity : Identified, Versioned {
    val deleted: Boolean
    fun delete() : Entity
}

data class AbstractEntity<T> private constructor(val identifier: String, val version: Long, val deleted: Boolean, val value: T) {
    constructor(value: T, type: String) : this("$type:${UUID.randomUUID()}", 0L, false, value)
    fun update(mutator: (T) -> T) : AbstractEntity<T> {
        check(!deleted)
        return copy(value = mutator(value), version = version + 1)
    }
    fun delete() : AbstractEntity<T> {
        check(!deleted)
        return copy(version = version + 1, deleted = true)
    }
}

data class Person(val name: String, val age: Int) {
    companion object {
        fun entity(name: String, age: Int) : AbstractEntity<Person> = AbstractEntity(Person(name, age), "Person")
    }
}

data class Account private constructor (val name: String, val balance: Int, override val identifier: UUID, override val version: Long, override val deleted: Boolean) : Entity {
    constructor(name: String, balance: Int) : this (name, balance, UUID.randomUUID(), 0L, false)
    fun add(amount: Int) : Account {
        check(!deleted)
        return copy(balance = balance + amount, version = version + 1)
    }
    override fun delete() : Account {
        check(!deleted)
        return copy(deleted = true, version = version + 1)
    }
}

data class AccountView(val identifier: UUID, val name: String, val balance: Int, val version: Long)

data class Snapshot private constructor (val id: Long, val ancestors: List<Long>, val creatorBranch: Long, val entityChanges: List<Long>) {
    constructor(id: Long, creatorBranch: Long) : this(id, listOf(), creatorBranch, listOf())
    operator fun plus(entityChange: EntityChange) = this.copy(entityChanges = listOf(entityChange.id) + this.entityChanges)
}

data class SnapshotDescription(val description: String)

//data class SnapshotEntityChangeHistoryView(val id: Long, val entityChanges: List<EntityChange>)

//data class Branch (val id: Long, val parent: Long, val tip: Long)

enum class EntityChangeType { CREATE, UPDATE, DELETE }

data class EntityChange(val id: Long, val type: EntityChangeType, val entityClass: Class<Entity>, val identifier: UUID, val before: Long?, val after: Long?, val snapshot: Long) {
    constructor(id: Long, event: EntityChangedEvent, snapshot: Long) : this(id, event.type, event.entityClass, event.entityIdentifier, event.before?.version, event.after.version, snapshot)

}

data class EntityChangedEvent(val before: Entity?, val after: Entity) {
    val type: EntityChangeType = if (before == null) EntityChangeType.CREATE else if (after.deleted) EntityChangeType.DELETE else EntityChangeType.UPDATE
    val entityClass: Class<Entity> = after.javaClass
    val entityIdentifier: UUID = after.identifier
    init {
        if (before != null) {
            check(before.javaClass == after.javaClass)
            check(before.identifier == after.identifier)
            check(before.version + 1 == after.version)
        }
    }
}

//data class EntityHistory(val identifier: UUID, val upToSnapshot: Long, val id: List<Long>)

val account = Account::class.java
val accountView = AccountView::class.java
val SnapshotDomain = Snapshot::class.java
val SnapshotDescriptionView = SnapshotDescription::class.java
//val branch = Branch::class.java
val EntityChangeDomain = EntityChange::class.java
val entityChangedEvent = EntityChangedEvent::class.java

fun init(folder: String): Reveno {
    val reveno = Engine(folder)
    reveno.domain()
            .transaction("createAccount") { txn, ctx ->
                val newAccount = ctx.repo().store(txn.id(), Account(txn.arg(), 0))
                ctx.eventBus().publishEvent(EntityChangedEvent(null, newAccount))
            }
            .uniqueIdFor(account)
            .command()

    reveno.domain()
            .transaction("changeBalance") { txn, ctx ->
                val before = ctx.repo().get(account, txn.arg())
                val after = ctx.repo().store(txn.id(), before.add(txn.intArg("inc")))
                ctx.eventBus().publishEvent(EntityChangedEvent(before, after))
            }
            .uniqueIdFor(account)
            .conditionalCommand { cmd, ctx ->
                val accountToChange = ctx.repo().get(account, cmd.arg())
                reveno.query()
                        .select(accountView) { acc ->
                            acc.identifier == accountToChange.identifier && acc.version == accountToChange.version + 1
                        }
                        .isEmpty()
            }
            .command()

    reveno.domain()
            .transaction("deleteAccount") { txn, ctx ->
                val before = ctx.repo().get(account, txn.arg())
                val after = ctx.repo().store(txn.id(), before.delete())
                ctx.eventBus().publishEvent(EntityChangedEvent(before, after))
            }
            .uniqueIdFor(account)
            .conditionalCommand { cmd, ctx ->
                val accountToDelete = ctx.repo().get(account, cmd.arg())
                reveno.query()
                        .select(accountView) { acc ->
                            acc.identifier == accountToDelete.identifier && acc.version == accountToDelete.version + 1
                        }
                        .isEmpty()
            }
            .command()

    reveno.domain().viewMapper(account, accountView) { id, acc, ctx ->
        AccountView(acc.identifier, acc.name, acc.balance, acc.version)
    }

    reveno.domain().viewMapper(EntityChangeDomain, EntityChangeDomain) { id, ec, ctx ->  ec }

    reveno.domain()
            .transaction("createSnapshot") { txn, ctx ->
                ctx.repo().store(txn.id(), Snapshot(txn.id(), 0L))
            }
            .uniqueIdFor(SnapshotDomain)
            .command()

    reveno.domain().viewMapper(SnapshotDomain, SnapshotDescriptionView) { id, s, ctx ->
        SnapshotDescription("id $id, changes ${s.entityChanges.size}")
    }

    reveno.domain().viewMapper(SnapshotDomain, SnapshotDomain) { id, instance, ctx -> instance }

//    reveno.domain().viewMapper(SnapshotDomain, SnapshotEntityChangeHistoryView::class.java) { id, s, ctx ->
//        val history = s.ancestors.fold(s.entityChanges) { allChanges, ancestor -> allChanges + ctx.get(SnapshotDomain, ancestor).entityChanges }
//        SnapshotEntityChangeHistoryView(id, ctx.link(history.toLongArray(), EntityChangeDomain))
//        SnapshotEntityChangeHistoryView(1L, listOf())
//    }

    reveno.domain()
            .transaction("createEntityChange") { txn, ctx ->
                val snapshotId : Long = txn.longArg("snapshot")
                val entityChange = ctx.repo().store(txn.id(), EntityChange(txn.id(), txn.arg("event"), snapshotId))
                ctx.repo().remap(snapshotId, SnapshotDomain) { id, s -> s + entityChange }
                println(entityChange)
            }
            .uniqueIdFor(EntityChangeDomain)
            .command()

    reveno.events().eventHandler(entityChangedEvent) { event, metadata ->
        val snapshotId = reveno.query().select(SnapshotDomain).first().id
        reveno.executeSync("createEntityChange", map("event", event, "snapshot", snapshotId))
    }

    return reveno
}

fun main(args: Array<String>) {
    val reveno = init("data/reveno-sample-${Instant.now().epochSecond}")
    reveno.startup()
    try {
        val snapshotId: Long = reveno.executeSync("createSnapshot")
        println(snapshotId)
        println(reveno.query().find(SnapshotDomain, snapshotId))
        val account = Stack<Long>()
        account.push(reveno.executeSync("createAccount", map("name", "John")))
        reveno.query().select(accountView).forEach { println(it) }
//        val identifier = reveno.query().find(accountView, accountId).identifier
        account.push(reveno.executeSync("changeBalance", map("id", account.pop(), "inc", 10000)))
        reveno.query().select(accountView).forEach { println(it) }
//        var latestAccountId = reveno.query().find(SnapshotEntityChangeHistoryView::class.java, snapshotId).entityChanges
//                .filter { e -> e.entityClass == account && e.identifier == identifier }
//                .first()
//                .after
//        if (latestAccountId != null) {
//            println(reveno.query().find(accountView, latestAccountId))
//        }
        account.push(reveno.executeSync("changeBalance", map("id", account.pop(), "inc", 10000)))
        reveno.query().select(accountView).forEach { println(it) }
//        latestAccountId = reveno.query().find(SnapshotEntityChangeHistoryView::class.java, snapshotId).entityChanges
//                .filter { e -> e.entityClass == account && e.identifier == identifier }
//                .first()
//                .after
//        if (latestAccountId != null) {
//            println(reveno.query().find(accountView, latestAccountId))
//        }
        account.push(reveno.executeSync("deleteAccount", map("id", account.pop())))
        reveno.query().select(accountView).forEach { println(it) }
    } finally {
        Thread.sleep(1000)
        reveno.shutdown()
    }
}