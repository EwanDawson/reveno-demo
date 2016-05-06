package net.lazygun.experiment.reveno.demo.kotlin

import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.longs.LongList
import it.unimi.dsi.fastutil.longs.LongLists
import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager
import org.reveno.atp.api.query.MappingContext
import org.reveno.atp.core.Engine
import org.reveno.atp.utils.MapUtils.map
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier

fun <T> Class<T>.identityViewsMapper() = { id: Long, entity: T, repository: MappingContext -> entity }

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

data class Account(val name: String, val balance: Int) {
    operator fun plus(amount: Int) : Account = copy(balance = balance + amount)
    data class Entity private constructor(val entity: net.lazygun.experiment.reveno.demo.kotlin.Entity<Account>) {
        constructor(name: String, balance: Int) : this(Entity(Account(name, balance), "Account"))
        private fun update(mutator: (Account) -> Account) : Entity = Entity(entity.update(mutator))
        fun delete() : Entity = Entity(entity.delete())
        operator fun plus(amount: Int) : Entity = update { it.plus(amount) }
    }
    data class View(val identifier: String, val name: String, val balance: Int, val version: Long, val deleted: Boolean)
    companion object {
        val domain = Entity::class.java
        val view = View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(e.entity.identifier, e.entity.value.name, e.entity.value.balance, e.entity.version, e.entity.deleted)
        }}
    }
}

fun initAccount(reveno: Reveno) {
    reveno.domain().apply {

        transaction("createAccount") { txn, ctx ->
            val newAccount = ctx.repo().store(txn.id(), Account.Entity(txn.arg(), 0))
            println("Created $newAccount")
            ctx.eventBus().publishEvent(EntityChangedEvent(null, newAccount.entity))
        }
        .uniqueIdFor(Account.domain)
        .command()

        transaction("changeBalance") { txn, ctx ->
            val before = ctx.repo().get(Account.domain, txn.arg())
            val after = ctx.repo().store(txn.id(), before + txn.intArg("inc"))
            println("Changed balance of account ${before.entity.value.name} from ${before.entity.value.balance} to ${after.entity.value.balance}")
            ctx.eventBus().publishEvent(EntityChangedEvent(before.entity, after.entity))
        }
        .uniqueIdFor(Account.domain)
        .conditionalCommand { cmd, ctx ->
            val accountToChange = ctx.repo().get(Account.domain, cmd.arg()).entity
            reveno.query().select(Account.view) { acc ->
                acc.identifier == accountToChange.identifier && acc.version == accountToChange.version + 1
            }
            .isEmpty()
        }
        .command()

        transaction("deleteAccount") { txn, ctx ->
            val before = ctx.repo().get(Account.domain, txn.arg())
            val after = ctx.repo().store(txn.id(), before.delete())
            println("Deleted $after")
            ctx.eventBus().publishEvent(EntityChangedEvent(before.entity, after.entity))
        }
        .uniqueIdFor(Account.domain)
        .conditionalCommand { cmd, ctx ->
            val accountToDelete = ctx.repo().get(Account.domain, cmd.arg()).entity
            reveno.query()
            .select(Account.view) { acc ->
                acc.identifier == accountToDelete.identifier && acc.version == accountToDelete.version + 1
            }
            .isEmpty()
        }
        .command()

        Account.map(reveno.domain())
    }
}

fun list(head: Long, tail: Collection<Long>) = LongArrayList().apply { add(head); addAll(tail) }
fun list(head: Long) = LongLists.singleton(head)
fun list() = LongLists.EMPTY_LIST

data class Snapshot private constructor (val ancestors: LongList, val creatorBranch: Long, val committed: Boolean, val commitMessage: String, val changes: LongList) {
    constructor(id: Long, creatorBranch: Long) : this(list(id), creatorBranch, false, "", list())
    fun commit(message: String) = copy(committed = true, commitMessage = message)
    fun branch(snapshotId: Long, branchId: Long) : Snapshot {
        check(committed)
        return Snapshot(list(snapshotId, ancestors), branchId, false, "", list())
    }
    operator fun plus(change: EntityChange) = copy(changes = list(change.id, changes))
    data class View(val id: Long, val ancestors: List<View>, val creatorBranch: Supplier<Branch.View>, val committed: Boolean, val commitMessage: String, val changes: List<EntityChange.View>)
    companion object {
        val domain = Snapshot::class.java
        val view = View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(id, r.link(e.ancestors, view), r.link(Branch.view, e.creatorBranch), e.committed, e.commitMessage, r.link(e.changes, EntityChange.view))
        }}
    }
}

data class Branch (val id: Long, val parent: Long, val tip: Long) {
    data class View(val id: Long, val parent: Supplier<View>, val tip: Supplier<Snapshot.View>)
    companion object {
        val domain = Branch::class.java
        val view = View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(id, r.link(view, e.parent), r.link(Snapshot.view, e.tip))
        }}
    }
}

fun initVersioning(reveno: Reveno) {
    reveno.domain().apply {
        transaction("createSnapshot") { txn, ctx ->
            val snapshot = ctx.repo().store(txn.id(), Snapshot(txn.id(), 0L))
            println("Created $snapshot")
        }
        .uniqueIdFor(Snapshot.domain)
        .command()

        Snapshot.map(reveno.domain())

        transaction("createBranch") { txn, ctx ->
            val branch = ctx.repo().store(txn.id(), Branch)
            println("Created $branch")
        }

        Branch.map(reveno.domain())

        transaction("initVersioning") { txn, ctx ->
            val snapshot = ctx.repo().store(0L, Snapshot(0, 0))
            val branch = ctx.repo().store(0L, Branch(0, -1, 0))
            println("Initialised versioning: $branch, $snapshot")
        }
        .conditionalCommand { command, context ->
            !context.repo().has(Branch.domain, 0)
        }
        .command()
    }
}

enum class EntityChangeType { CREATE, UPDATE, DELETE }

data class EntityChange(val id: Long, val changeType: EntityChangeType, val entityType: String, val identifier: String, val before: Long?, val after: Long, val snapshot: Long) {
    constructor(id: Long, event: EntityChangedEvent<*>, snapshot: Long) : this(id, event.type, event.entityType, event.entityIdentifier, event.before?.version, event.after.version, snapshot)
    data class View(val id: Long, val changeType: EntityChangeType, val entityType: String, val identifier: String, val before: Long?, val after: Long, val snapshot: Supplier<Snapshot.View>)
    companion object {
        val domain = EntityChange::class.java
        val view = EntityChange.View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(id, e.changeType, e.entityType, e.identifier, e.before, e.after, r.link(Snapshot.view, e.snapshot))
        }}
    }
}

fun initEntityChange(reveno: Reveno) {
    reveno.domain().apply {
        transaction("createEntityChange") { txn, ctx ->
            val snapshotId : Long = txn.longArg("snapshot")
            val entityChange = ctx.repo().store(txn.id(), EntityChange(txn.id(), txn.arg("event"), snapshotId))
            ctx.repo().remap(snapshotId, Snapshot.domain) { id, s -> s + entityChange }
            System.err.println("Created $entityChange")
        }
        .uniqueIdFor(EntityChange.domain)
        .command()

        EntityChange.map(reveno.domain())
    }
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
    companion object {
        val event = EntityChangedEvent::class.java
    }
}

fun initEntityChangesEvent(reveno: Reveno) {
    reveno.apply {
        events().eventHandler(EntityChangedEvent.event) { event, metadata ->
            System.err.println("Handling $event")
            executeSync("createEntityChange", map("event", event, "snapshot", currentSnapshot.get()))
        }
    }
}

fun init(folder: String): Reveno {
    return Engine(folder).apply {
        initAccount(this)
        initEntityChange(this)
        initVersioning(this)
        initEntityChangesEvent(this)
    }
}

fun bootstrap(reveno: Reveno) {
    reveno.apply {
        executeSync<Unit>("initVersioning")
        val branch = query().select(Branch.view).sortedByDescending { it.id }.first()
        currentBranch.set(branch.id)
        currentSnapshot.set(branch.tip.get().id)
    }
}

val currentBranch = AtomicLong(0)
val currentSnapshot = AtomicLong(0)

fun main(args: Array<String>) {
    val reveno = init("data/reveno-sample-${Instant.now().epochSecond}").apply {
        startup()
        bootstrap(this)
    }

    fun printAccountHistory() = reveno.query().select(Account.view).forEach { println(it) }

//    fun changeHistory(snapshot: Long) = reveno.query().find(SnapshotChangeHistory.view, snapshot)

    try {
        val account = Stack<Long>()
        account.push(reveno.executeSync("createAccount", map("name", "John")))
        printAccountHistory()

        account.push(reveno.executeSync("changeBalance", map("id", account.pop(), "inc", 10000)))
        printAccountHistory()

        account.push(reveno.executeSync("changeBalance", map("id", account.pop(), "inc", 10000)))
        printAccountHistory()

        account.push(reveno.executeSync("deleteAccount", map("id", account.pop())))
        printAccountHistory()

        Thread.sleep(1000)

        println("Current snapshot: ${currentSnapshot.get()}")
        println(reveno.query().find(Snapshot.view, currentSnapshot.get()))

    } finally {
        reveno.shutdown()
    }
}