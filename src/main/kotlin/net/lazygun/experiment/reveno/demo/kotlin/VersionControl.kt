package net.lazygun.experiment.reveno.demo.kotlin

import it.unimi.dsi.fastutil.longs.LongList
import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager
import org.reveno.atp.api.domain.WriteableRepository
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier

data class VersionedEntity<T> private constructor(val identifier: String, val version: Long, val deleted: Boolean, val value: T, val type: String) {
    constructor(value: T, type: String) : this("$type:${UUID.randomUUID()}", 0L, false, value, type)
    fun update(mutator: (T) -> T) : VersionedEntity<T> {
        check(!deleted)
        return copy(value = mutator(value), version = version + 1)
    }
    fun delete() : VersionedEntity<T> {
        check(!deleted)
        return copy(version = version + 1, deleted = true)
    }
}

data class Snapshot private constructor (val ancestors: LongList, val creatorBranch: Long, val committed: Boolean, val commitMessage: String, val changes: LongList) {
    constructor(id: Long, creatorBranch: Long) : this(list(id), creatorBranch, false, "", list())
    fun commit(message: String) = copy(committed = true, commitMessage = message)
    fun branch(snapshotId: Long, branchId: Long) : Snapshot {
        check(committed)
        return Snapshot(list(snapshotId, ancestors), branchId, false, "", list())
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

enum class EntityChangeType { CREATE, UPDATE, DELETE }

data class EntityChange(val id: Long, val changeType: EntityChangeType, val entityType: String, val identifier: String, val before: Long, val after: Long, val snapshot: Long) {

    constructor(id: Long, event: EntityChangedEvent<*>, snapshot: Long) : this(id, event.type, event.entityType, event.entityIdentifier, event.before, event.after, snapshot)
    data class View(val id: Long, val changeType: EntityChangeType, val entityType: String, val identifier: String, val before: Long?, val after: Long, val snapshot: Long)
    companion object {
        val domain = EntityChange::class.java
        val view = EntityChange.View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(id, e.changeType, e.entityType, e.identifier, e.before, e.after, e.snapshot)
        }}
    }
}
data class EntityChangedEvent<T>(val entity: VersionedEntity<T>, val before: Long, val after: Long) {
    constructor(entity: VersionedEntity<T>, after: Long) : this(entity, -1, after)
    val type: EntityChangeType = if (before == -1L) EntityChangeType.CREATE else if (entity.deleted) EntityChangeType.DELETE else EntityChangeType.UPDATE
    val entityType: String = entity.type
    val entityIdentifier: String = entity.identifier
    companion object {
        val event = EntityChangedEvent::class.java
    }
}

internal fun initVersionControlDomain(reveno: Reveno) {
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

        EntityChange.map(reveno.domain())
    }

    reveno.apply {
        events().eventHandler(EntityChangedEvent.event) { event, metadata ->
            System.err.println("Handling $event")
        }
    }
}

internal fun <T> entityChange(id: Long, entityChangeEvent: EntityChangedEvent<T>, repo: WriteableRepository) {
    val snapshotId = currentSnapshot.get()
    val entityChange = repo.store(id, EntityChange(id, entityChangeEvent, snapshotId))
    repo.remap(snapshotId, Snapshot.domain) { id, s -> s + entityChange }
    System.err.println("Created $entityChange")
}

internal val currentBranch = AtomicLong(0)
internal val currentSnapshot = AtomicLong(0)

fun <T> latestVersion(identifier: String, view: Class<T>) : T {
    val db = database.get()
    val snapshotId = currentSnapshot.get()
    val snapshot = db.query().find(Snapshot.view, snapshotId)
    val snapshotChangeHistory = db.query().select(Snapshot.view, { snapshot.ancestors.contains(it.id)} ).map { it.changes }.flatten()
    val mostRecentChangeForEntityInstance = snapshotChangeHistory.firstOrNull { it.identifier == identifier }
    ?: throw NoSuchElementException("No entity with identifier $identifier exists in the history of snapshot $snapshotId")
    val entityId = mostRecentChangeForEntityInstance.after
    val entity = db.query().find(view, entityId)
    return entity
}