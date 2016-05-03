package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.core.Engine
import org.reveno.atp.utils.MapUtils.map
import java.time.Instant
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

data class Person(val name: String, val age: Int)

data class PersonEntity private constructor(val entity: Entity<Person>) {
    constructor(name: String, age: Int) : this(Entity(Person(name, age), "Person"))
    fun update(mutator: (Person) -> Person) : PersonEntity = PersonEntity(entity.update(mutator))
    fun delete() : PersonEntity = PersonEntity(entity.delete())
}

data class Account(val name: String, val balance: Int)

data class AccountEntity private constructor(val entity: Entity<Account>) {
    constructor(name: String, balance: Int) : this(Entity(Account(name, balance), "Account"))
    fun update(mutator: (Account) -> Account) : AccountEntity = AccountEntity(entity.update(mutator))
    fun delete() : AccountEntity = AccountEntity(entity.delete())
}

data class AccountView(val identifier: String, val name: String, val balance: Int, val version: Long, val deleted: Boolean)

data class Snapshot private constructor (val id: Long, val ancestors: List<Long>, val creatorBranch: Long, val entityChanges: List<Long>) {
    constructor(id: Long, creatorBranch: Long) : this(id, listOf(), creatorBranch, listOf())
    operator fun plus(entityChange: EntityChange) = this.copy(entityChanges = listOf(entityChange.id) + this.entityChanges)
}

data class SnapshotDescription(val description: String)

//data class SnapshotEntityChangeHistoryView(val id: Long, val entityChanges: List<EntityChange>)

//data class Branch (val id: Long, val parent: Long, val tip: Long)

enum class EntityChangeType { CREATE, UPDATE, DELETE }

data class EntityChange(val id: Long, val changeType: EntityChangeType, val entityType: String, val identifier: String, val before: Long?, val after: Long?, val snapshot: Long) {
    constructor(id: Long, event: EntityChangedEvent<*>, snapshot: Long) : this(id, event.type, event.entityType, event.entityIdentifier, event.before?.version, event.after.version, snapshot)

}

data class EntityChangedEvent<T>(val before: Entity<T>?, val after: Entity<T>) {
    val type: EntityChangeType = if (before == null) EntityChangeType.CREATE else if (after.deleted) EntityChangeType.DELETE else EntityChangeType.UPDATE
    val entityType: String = after.type
    val entityIdentifier: String = after.identifier
    init {
        if (before != null) {
            check(before.type == after.type)
            check(before.identifier == after.identifier)
            check(before.version + 1 == after.version)
        }
    }
}

//data class EntityHistory(val identifier: UUID, val upToSnapshot: Long, val id: List<Long>)

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
                val newAccount = ctx.repo().store(txn.id(), AccountEntity(txn.arg(), 0))
                ctx.eventBus().publishEvent(EntityChangedEvent(null, newAccount.entity))
            }
            .uniqueIdFor(AccountEntity::class.java)
            .command()

    reveno.domain()
            .transaction("changeBalance") { txn, ctx ->
                val before = ctx.repo().get(AccountEntity::class.java, txn.arg())
                val after = ctx.repo().store(txn.id(), before.update {
                    it.copy(balance = it.balance + txn.intArg("inc"))
                })
                ctx.eventBus().publishEvent(EntityChangedEvent(before.entity, after.entity))
            }
            .uniqueIdFor(AccountEntity::class.java)
            .conditionalCommand { cmd, ctx ->
                val accountToChange = ctx.repo().get(AccountEntity::class.java, cmd.arg()).entity
                reveno.query()
                        .select(accountView) { acc ->
                            acc.identifier == accountToChange.identifier && acc.version == accountToChange.version + 1
                        }
                        .isEmpty()
            }
            .command()

    reveno.domain()
            .transaction("deleteAccount") { txn, ctx ->
                val before = ctx.repo().get(AccountEntity::class.java, txn.arg())
                val after = ctx.repo().store(txn.id(), before.delete())
                ctx.eventBus().publishEvent(EntityChangedEvent(before.entity, after.entity))
            }
            .uniqueIdFor(AccountEntity::class.java)
            .conditionalCommand { cmd, ctx ->
                val accountToDelete = ctx.repo().get(AccountEntity::class.java, cmd.arg()).entity
                reveno.query()
                        .select(accountView) { acc ->
                            acc.identifier == accountToDelete.identifier && acc.version == accountToDelete.version + 1
                        }
                        .isEmpty()
            }
            .command()

    reveno.domain().viewMapper(AccountEntity::class.java, accountView) { id, account, ctx ->
        account.entity.let {
            AccountView(it.identifier, it.value.name, it.value.balance, it.version, it.deleted)
        }
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