package net.lazygun.experiment.reveno.demo.kotlin

import it.unimi.dsi.fastutil.longs.LongList
import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager
import org.reveno.atp.api.domain.WriteableRepository
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Predicate
import java.util.function.Supplier

interface EntityView {
    val id: Long
}

interface VersionedEntityView : EntityView {
    val identifier: String
}

interface VersionedEntity {
    val identifier: String
    val type: String
    val version: Long
    val deleted: Boolean
}

data class VersionedEntityDelegate private constructor(override val identifier: String, override val type: String,
                                                       override val version: Long, override val deleted: Boolean = false): VersionedEntity {
    constructor(type: String) : this("$type:${UUID.randomUUID()}", type, 0)
    fun update(): VersionedEntityDelegate = copy(version = version + 1)
    fun delete(): VersionedEntityDelegate = copy(version = version + 1, deleted = true)
    private val abbrevId: String by lazy {
        identifier.substringBefore(":") + ":" + identifier.substringAfterLast("-").substring(8)
    }
    private val toString: String by lazy {
        "$abbrevId:$version"
    }
    fun toString(contents: String) = toString + "(" + (if (deleted) "" else contents) + ")"
}

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

data class Snapshot private constructor (val id: Long, val ancestors: LongList, val creatorBranch: Long, val committed: Boolean, val commitMessage: String, val changes: LongList) {
    internal constructor(id: Long, creatorBranch: Long) : this(id, list(id), creatorBranch, false, "", list())
    fun commit(message: String) = copy(committed = true, commitMessage = message)
    fun branch(branchId: Long, snapshotId: Long) : Snapshot {
        check(committed)
        return Snapshot(snapshotId, list(snapshotId, ancestors), branchId, false, "", list())
    }
    operator fun plus(change: EntityChange) = copy(changes = list(change.id, changes))
    data class View(val id: Long, val ancestors: List<Long>, val creatorBranch: Long, val committed: Boolean, val commitMessage: String, val changes: List<EntityChange.View>)
    companion object {
        val domain = Snapshot::class.java
        val view = View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(id, e.ancestors, e.creatorBranch, e.committed, e.commitMessage, r.link(e.changes, EntityChange.view))
        }}
    }
}

data class Branch private  constructor (val id: Long, val tip: Long) {
    constructor(id: Long, tip: Snapshot) : this (id, tip.id)
    data class View(val id: Long, val tip: Supplier<Snapshot.View>)
    companion object {
        val domain = Branch::class.java
        val view = View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(id, r.link(Snapshot.view, e.tip))
        }}
    }
}

enum class EntityChangeType { CREATE, UPDATE, DELETE }

data class EntityChange(val id: Long, val changeType: EntityChangeType, val entityType: String, val identifier: String, val before: Long, val after: Long, val snapshot: Long) {

    constructor(id: Long, event: EntityChangedEvent, snapshot: Long) : this(id, event.type, event.entityType, event.entityIdentifier, event.before, event.after, snapshot)
    data class View(val id: Long, val changeType: EntityChangeType, val entityType: String, val identifier: String, val before: Long?, val after: Long, val snapshot: Long)
    companion object {
        val domain = EntityChange::class.java
        val view = EntityChange.View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(id, e.changeType, e.entityType, e.identifier, e.before, e.after, e.snapshot)
        }}
    }
}
data class EntityChangedEvent(val entity: VersionedEntity, val before: Long, val after: Long) {
    constructor(entity: VersionedEntity, after: Long) : this(entity, -1, after)
    val type: EntityChangeType = if (before == -1L) EntityChangeType.CREATE else if (entity.deleted) EntityChangeType.DELETE else EntityChangeType.UPDATE
    val entityType: String = entity.type
    val entityIdentifier: String = entity.identifier
    companion object {
        val event = EntityChangedEvent::class.java
    }
}

internal fun initVersionControlDomain(reveno: Reveno) {
    reveno.domain().apply {
        transaction("commitSnapshot") { txn, ctx ->
            val snapshot = ctx.repo().remap(txn.longArg(), Snapshot.domain, { id, s ->
                s.commit(txn.arg<String>("message"))}
            )
            println("Committed $snapshot")
            val uncommitted = ctx.repo().store(txn.id(), snapshot.branch(snapshot.creatorBranch, txn.id()))
            println("Created new uncommitted snapshot $uncommitted")
        }
        .uniqueIdFor(Snapshot.domain)
        .command()

        Snapshot.map(reveno.domain())

        transaction("createBranch") { txn, ctx ->
            val baseSnapshot = ctx.repo().get(Snapshot.domain, txn.longArg())
            val tipSnapshot = baseSnapshot.branch(txn.id(Branch.domain), txn.id(Snapshot.domain))
            ctx.repo().store(txn.id(Snapshot.domain), tipSnapshot)
            val branch = Branch(txn.id(Branch.domain), tipSnapshot)
            ctx.repo().store(txn.id(Branch.domain), branch)
            println("Created $branch")
        }
        .uniqueIdFor(Branch.domain, Snapshot.domain)
        .command()

        Branch.map(reveno.domain())

        transaction("initVersioning") { txn, ctx ->
            val snapshot = ctx.repo().store(0L, Snapshot(0, 0))
            val branch = ctx.repo().store(0L, Branch(0, snapshot))
            println("Initialised versioning: $branch, $snapshot")
        }
        .conditionalCommand { command, context ->
            !context.repo().has(Branch.domain, 0)
        }
        .command()

        EntityChange.map(reveno.domain())
    }

    reveno.apply {
        events().eventHandler(EntityChangedEvent.event) { event, metadata ->
            System.err.println("Handling $event")
        }
    }
}

internal val currentSnapshot = AtomicLong(0)

internal fun entityChange(id: Long, entityChangeEvent: EntityChangedEvent, repo: WriteableRepository) {
    val snapshotId = currentSnapshot.get()
    val entityChange = repo.store(id, EntityChange(id, entityChangeEvent, snapshotId))
    repo.remap(snapshotId, Snapshot.domain) { id, s -> s + entityChange }
    System.err.println("Created $entityChange")
}

class VersionedEntityQuery(val snapshotId: Long) {
    private val db = database.get()
    private fun snapshot(id: Long) = db.query().find(Snapshot.view, id)
    private val currentSnapshot = snapshot(snapshotId).ancestors.firstOrNull { snapshot(it).changes.isNotEmpty() }?.run { snapshot(this) }
    val currentChange = currentSnapshot?.changes?.first()?.id
    private fun mostRecentSnapshot(identifier: String) : Snapshot.View {
        if (currentSnapshot == null) throw NoSuchElementException("No entity with identifier $identifier exists")
        return snapshot(currentSnapshot.ancestors.first { snapshot(it).changes.any { it.identifier == identifier } })
    }
    private fun entityId(identifier: String) = mostRecentSnapshot(identifier).changes.first { it.id <= currentChange?:0L && it.identifier == identifier }.after
    fun find(identifier: String, view: Class<out VersionedEntityView>) = db.query().find(view, entityId(identifier))
    fun select(view: Class<VersionedEntityView>, predicate: Predicate<VersionedEntityView>) = db.query().select(view, predicate).filter { it.id == entityId(it.identifier) }
}