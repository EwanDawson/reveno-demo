package net.lazygun.experiment.reveno.demo.kotlin

import it.unimi.dsi.fastutil.longs.LongList
import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager
import org.reveno.atp.utils.MapUtils.map
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier

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

    reveno.apply {
        events().eventHandler(EntityChangedEvent.event) { event, metadata ->
            System.err.println("Handling $event")
            executeSync("createEntityChange", map("event", event, "snapshot", currentSnapshot.get()))
        }
    }
}

internal val currentBranch = AtomicLong(0)
internal val currentSnapshot = AtomicLong(0)
