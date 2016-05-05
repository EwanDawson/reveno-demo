package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.api.query.MappingContext
import org.reveno.atp.core.Engine
import org.reveno.atp.utils.MapUtils.map
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicLong

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
    companion object {
        val domain = AccountEntity::class.java
        val view = AccountView::class.java
    }
}

data class AccountEntity private constructor(val entity: Entity<Account>) {
    constructor(name: String, balance: Int) : this(Entity(Account(name, balance), "Account"))
    private fun update(mutator: (Account) -> Account) : AccountEntity = AccountEntity(entity.update(mutator))
    fun delete() : AccountEntity = AccountEntity(entity.delete())
    operator fun plus(amount: Int) : AccountEntity = update { it.plus(amount) }
}

data class AccountView(val identifier: String, val name: String, val balance: Int, val version: Long, val deleted: Boolean)

fun initAccount(reveno: Reveno) {
    reveno.domain().apply {

        transaction("createAccount") { txn, ctx ->
            val newAccount = ctx.repo().store(txn.id(), AccountEntity(txn.arg(), 0))
            println("Created $newAccount")
            ctx.eventBus().publishEvent(EntityChangedEvent(null, newAccount.entity))
        }
        .uniqueIdFor(Account.domain)
        .command()

        transaction("changeBalance") { txn, ctx ->
            val before = ctx.repo().get(AccountEntity::class.java, txn.arg())
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

        viewMapper(Account.domain, Account.view) { id, account, ctx ->
            account.entity.let {
                AccountView(it.identifier, it.value.name, it.value.balance, it.version, it.deleted)
            }
        }
    }
}

data class Snapshot private constructor (val id: Long, val ancestors: List<Long>, val creatorBranch: Long, val committed: Boolean, val commitMessage: String) {
    constructor(id: Long, creatorBranch: Long) : this(id, listOf(id), creatorBranch, false, "")
    fun commit(message: String) = copy(committed = true, commitMessage = message)
    fun branch(snapshotId: Long, branchId: Long) : Snapshot {
        check(committed)
        return Snapshot(snapshotId, listOf(snapshotId) + ancestors, branchId, false, "")
    }
    companion object {
        val domain = Snapshot::class.java
    }
}

data class Branch (val id: Long, val parent: Long, val tip: Long) {
    companion object {
        val domain = Branch::class.java
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

        viewMapper(Snapshot.domain, Snapshot.domain, Snapshot.domain.identityViewsMapper())

        transaction("createBranch") { txn, ctx ->
            val branch = ctx.repo().store(txn.id(), Branch)
            println("Created $branch")
        }

        viewMapper(Branch.domain, Branch.domain, Branch.domain.identityViewsMapper())

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

data class EntityChange(val id: Long, val changeType: EntityChangeType, val entityType: String, val identifier: String, val before: Long?, val after: Long?, val snapshot: Long) {
    constructor(id: Long, event: EntityChangedEvent<*>, snapshot: Long) : this(id, event.type, event.entityType, event.entityIdentifier, event.before?.version, event.after.version, snapshot)
    companion object {
        val domain = EntityChange::class.java
    }
}

fun initEntityChange(reveno: Reveno) {
    reveno.domain().apply {
        transaction("createEntityChange") { txn, ctx ->
            val snapshotId : Long = txn.longArg("snapshot")
            val entityChange = ctx.repo().store(txn.id(), EntityChange(txn.id(), txn.arg("event"), snapshotId))
            //ctx.repo().remap(snapshotId, SnapshotDomain) { id, s -> s + entityChange }
            System.err.println("Created $entityChange")
        }
        .uniqueIdFor(EntityChange.domain)
        .command()

        viewMapper(EntityChange.domain, EntityChange.domain, EntityChange.domain.identityViewsMapper())
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
            val snapshotId = query().select(Snapshot.domain).first().id
            executeSync("createEntityChange", map("event", event, "snapshot", snapshotId))
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
        val branch = query().select(Branch.domain).sortedByDescending { it.id }.first()
        currentBranch.set(branch.id)
        currentSnapshot.set(branch.tip)
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

    } finally {
        Thread.sleep(1000)
        reveno.shutdown()
    }
}