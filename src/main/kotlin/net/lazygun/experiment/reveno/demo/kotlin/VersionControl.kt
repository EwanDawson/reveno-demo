package net.lazygun.experiment.reveno.demo.kotlin

import it.unimi.dsi.fastutil.longs.LongList
import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Predicate
import java.util.function.Supplier

interface EntityView {
    val versionId: Long
}

interface VersionedEntityView : EntityView {
    val entityId: String
}

interface VersionedEntity {
    val entityId: String
    val type: String
    val version: Long
    val deleted: Boolean
}

data class VersionedEntityDelegate private constructor(override val entityId: String, override val type: String,
                                                       override val version: Long, override val deleted: Boolean = false): VersionedEntity {
    constructor(type: String) : this("$type:${UUID.randomUUID()}", type, 0)
    fun update(): VersionedEntityDelegate = copy(version = version + 1)
    fun delete(): VersionedEntityDelegate = copy(version = version + 1, deleted = true)
    fun abbrevId() = entityId.substringBefore(":") + ":" + entityId.substringAfterLast("-").substring(8)
    fun toString(contents: String) = "${abbrevId()}:$version(${(if (deleted) "" else contents)})"
}

data class Snapshot private constructor (val id: Long, val ancestors: LongList, val creatorBranch: Long, val committed: Boolean, val commitMessage: String, val changes: LongList) {
    internal constructor(id: Long, creatorBranch: Long) : this(id, list(id), creatorBranch, false, "", list())
    fun commit(message: String) = copy(committed = true, commitMessage = message)
    fun branch(branchId: Long, snapshotId: Long) : Snapshot {
        check(committed)
        return Snapshot(snapshotId, list(snapshotId, ancestors), branchId, false, "", list())
    }
    operator fun plus(change: EntityChange) = copy(changes = list(change.id, changes))
    data class View(val id: Long, val ancestors: List<Long>, val creatorBranch: Long, val committed: Boolean, val commitMessage: String, val changes: List<EntityChange>)
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

data class EntityChange private constructor(val id: Long, val changeType: EntityChangeType, val entityType: String, val identifier: String, val beforeVersionId: Long?, val afterVersionId: Long, val snapshotId: Long) {
    companion object {
        val domain = EntityChange::class.java
        val view = EntityChange::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            EntityChange(id, e.changeType, e.entityType, e.identifier, e.beforeVersionId, e.afterVersionId, e.snapshotId)
        }}
        fun create(id: Long, entity: VersionedEntity, entityId: Long) = EntityChange(id, EntityChangeType.CREATE, entity.type, entity.entityId, null, entityId, currentSnapshot.get())
        fun update(id: Long, entity: VersionedEntity, entityBeforeId: Long, entityAfterId: Long) = EntityChange(id, EntityChangeType.UPDATE, entity.type, entity.entityId, entityBeforeId, entityAfterId, currentSnapshot.get())
        fun delete(id: Long, entity: VersionedEntity, entityBeforeId: Long, entityAfterId: Long) = EntityChange(id, EntityChangeType.DELETE, entity.type, entity.entityId, entityBeforeId, entityAfterId, currentSnapshot.get())
    }
}

data class CommitSnapshotCommand(val snapshotId: Long, val message: String)
private data class CommitSnapshotTransaction(val committed: Snapshot, val newUncommitted: Snapshot)
data class CreateBranchCommand(val baseSnapshotId: Long)
private data class CreateBranchTransaction(val newBranch: Branch, val newBranchTip: Snapshot)
data class InitVersionControl(val branchId: Long)

internal fun initVersionControlDomain(reveno: Reveno) {
    reveno.domain().apply {

        command(CommitSnapshotCommand::class.java, Long::class.java) { command, context ->
            val committed = context.repo().get(Snapshot.domain, command.snapshotId).commit(command.message)
            val newUncommitted = committed.branch(committed.creatorBranch, context.id(Snapshot.domain))
            context.executeTxAction(CommitSnapshotTransaction(committed, newUncommitted))
            newUncommitted.id
        }
        transactionAction(CommitSnapshotTransaction::class.java) { transaction, context ->
            context.repo().remap(transaction.committed.id, Snapshot.domain) { id, snapshot ->
                transaction.committed
            }
            context.repo().store(transaction.newUncommitted.id, transaction.newUncommitted)
        }
        Snapshot.map(reveno.domain())

        command(CreateBranchCommand::class.java, Long::class.java) { command, context ->
            val baseSnapshot = context.repo().get(Snapshot.domain, command.baseSnapshotId)
            val tipSnapshot = baseSnapshot.branch(context.id(Branch.domain), context.id(Snapshot.domain))
            context.executeTxAction(CreateBranchTransaction(Branch(tipSnapshot.creatorBranch, tipSnapshot), tipSnapshot))
            tipSnapshot.creatorBranch
        }
        transactionAction(CreateBranchTransaction::class.java) { transaction, context ->
            context.repo().store(transaction.newBranch.id, transaction.newBranch)
            context.repo().store(transaction.newBranchTip.id, transaction.newBranchTip)
        }
        Branch.map(reveno.domain())

        command(InitVersionControl::class.java) { command, context ->
            if (!context.repo().has(Branch.domain, command.branchId)) context.executeTxAction(command)
            else throw IllegalStateException("Version Control already initialised")
        }
        transactionAction(InitVersionControl::class.java) { transaction, context ->
            val snapshot = context.repo().store(0, Snapshot(0, transaction.branchId))
            context.repo().store(transaction.branchId, Branch(transaction.branchId, snapshot))
        }

        EntityChange.map(reveno.domain())
    }

    reveno.apply {
        events().eventHandler(EntityChange.domain) { event, metadata ->
            System.err.println("Handling $event")
        }
    }
}

internal fun bootstrapVersionControl(reveno: Reveno) : Long {
    reveno.run {
        executeSync<Unit>(InitVersionControl(0))
        val snapshot = query().select(Snapshot.view).sortedByDescending { it.id }.first().id
        database.set(this)
        return snapshot
    }
}

internal val currentSnapshot = AtomicLong(0)

class VersionedEntityQuery(val snapshotId: Long) {
    private val db = database.get()
    private fun snapshot(id: Long) = db.query().find(Snapshot.view, id)
    private val currentSnapshot = snapshot(snapshotId).ancestors.firstOrNull { snapshot(it).changes.isNotEmpty() }?.run { snapshot(this) }
    val currentChange = currentSnapshot?.changes?.first()?.id
    private fun mostRecentSnapshot(identifier: String) : Snapshot.View {
        if (currentSnapshot == null) throw NoSuchElementException("No entity with identifier $identifier exists")
        return snapshot(currentSnapshot.ancestors.first { snapshot(it).changes.any { it.identifier == identifier } })
    }
    private fun entityId(identifier: String) = mostRecentSnapshot(identifier).changes.first { it.id <= currentChange?:0L && it.identifier == identifier }.afterVersionId
    fun find(identifier: String, view: Class<out VersionedEntityView>) = db.query().find(view, entityId(identifier))
    fun select(view: Class<VersionedEntityView>, predicate: Predicate<VersionedEntityView>) = db.query().select(view, predicate).filter { it.versionId == entityId(it.entityId) }
}