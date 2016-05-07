package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.core.Engine
import org.reveno.atp.utils.MapUtils.map
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicReference

internal fun init(folder: String): Reveno {
    return Engine(folder).apply {
        initAccountDomain(this)
        initVersionControlDomain(this)
    }
}

internal fun bootstrap(reveno: Reveno) : Long {
    reveno.run {
        executeSync<Unit>("initVersioning")
        val snapshot = query().select(Snapshot.view).sortedByDescending { it.id }.first().id
        database.set(this)
        return snapshot
    }
}

internal val database = AtomicReference<Reveno>()

fun main(args: Array<String>) {
    init("data/reveno-sample-${Instant.now().epochSecond}").apply {
        startup()
        currentSnapshot.set(bootstrap(this))
    }

    val db = database.get()
    fun printAccountHistory() {
        println("---- Account history -----")
        db.query().select(Account.view).forEach { println(it) }
        println("--------------------------")
    }

    try {
        val account = Stack<Long>()
        account.push(db.executeSync("createAccount", map("account", Account("Ewan"))))
        val identifier = db.query().find(Account.view, account.peek()).identifier
        println("Account identifier: $identifier")
        fun printLatestVersion() {
            println("Latest version: " + latestVersion(currentSnapshot.get(), identifier, Account.view))
        }

        printAccountHistory()
        printLatestVersion()

        account.push(db.executeSync("changeBalance", map("id", account.pop(), "inc", 10000)))
        printAccountHistory()
        printLatestVersion()

        val forkSnapshot = currentSnapshot.getAndUpdate {
            db.executeSync("commitSnapshot", map("id", it, "message", "First commit!"))
        }
        val branchA: Long = db.executeSync("createBranch", map("baseSnapshot", forkSnapshot))
        val branchB: Long = db.executeSync("createBranch", map("baseSnapshot", forkSnapshot))

        currentSnapshot.set(db.query().find(Branch.view, branchA).tip.get().id)
        account.push(db.executeSync("changeBalance", map("id", account.pop(), "inc", 10000)))
        printAccountHistory()
        printLatestVersion()

        currentSnapshot.set(db.query().find(Branch.view, branchB).tip.get().id)
        account.push(db.executeSync("deleteAccount", map("id", account.pop())))
        printAccountHistory()
        printLatestVersion()

    } finally {
        Thread.sleep(1000)
        db.shutdown()
    }
}