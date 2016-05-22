package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.core.Engine
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

internal fun init(folder: String): Reveno {
    return Engine(folder).apply {
        initAccountDomain(this)
        initVersionControlDomain(this)
    }
}

internal val database = AtomicReference<Reveno>()

fun main(args: Array<String>) {
    init("data/reveno-sample-${Instant.now().epochSecond}").apply {
        startup()
        currentSnapshot.set(bootstrapVersionControl(this))
    }

    val db = database.get()
    fun printAccountHistory() {
        println("---- Account history -----")
        db.query().select(Account.view).forEach { println(it) }
        println("--------------------------")
    }

    fun updateCurrentSnapshotToBranchTip(branchId: Long) {
        val branch = db.query().find(Branch.view, branchId)
        val tipId = branch.tip.get().id
        currentSnapshot.set(tipId)
        println("Current snapshot set to ${db.query().find(Snapshot.view, tipId)}")
    }

    try {
        val initialInstanceId: Long = db.executeSync(CreateAccountCommand("Ewan"))
        val identifier = db.query().find(Account.view, initialInstanceId).entityId
        println("Account identifier: $identifier")
        fun printLatestVersion() {
            println("Latest version: " + VersionedEntityQuery(currentSnapshot.get()).find(identifier, Account.view))
        }

        printAccountHistory()
        printLatestVersion()

        val rootBranchUpdateId: Long = db.executeSync(UpdateAccountBalanceCommand(initialInstanceId, 10000))
        printAccountHistory()
        printLatestVersion()

        val forkSnapshot = currentSnapshot.getAndUpdate {
            val newSnapshot: Long = db.executeSync(CommitSnapshotCommand(it, "First commit!"))
            println("Current snapshot set to ${db.query().find(Snapshot.view, newSnapshot)}")
            return@getAndUpdate newSnapshot
        }

        val branchA: Long = db.executeSync(CreateBranchCommand(forkSnapshot))
        println(db.query().find(Branch.view, branchA))
        val branchB: Long = db.executeSync(CreateBranchCommand(forkSnapshot))
        println(db.query().find(Branch.view, branchB))

        updateCurrentSnapshotToBranchTip(branchA)
        val branchAUpdateId: Long = db.executeSync(UpdateAccountBalanceCommand(rootBranchUpdateId, -5000))
        printAccountHistory()
        printLatestVersion()

        updateCurrentSnapshotToBranchTip(branchB)
        val branchBUpdateId: Long = db.executeSync(CloseAccountCommand(rootBranchUpdateId))
        printAccountHistory()
        printLatestVersion()

    } finally {
        Thread.sleep(1000)
        db.shutdown()
    }
}