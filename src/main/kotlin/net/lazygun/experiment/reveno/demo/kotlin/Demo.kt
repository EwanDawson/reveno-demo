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

internal fun bootstrap(reveno: Reveno) {
    reveno.apply {
        executeSync<Unit>("initVersioning")
        val branch = query().select(Branch.view).sortedByDescending { it.id }.first()
        currentBranch.set(branch.id)
        currentSnapshot.set(branch.tip.get().id)
        database.set(this)
    }
}

internal val database = AtomicReference<Reveno>()

fun main(args: Array<String>) {
    init("data/reveno-sample-${Instant.now().epochSecond}").apply {
        startup()
        bootstrap(this)
    }

    val db = database.get()
    fun printAccountHistory() {
        println("---- Account history -----")
        db.query().select(Account.view).forEach { println(it) }
        println("--------------------------")
    }

    try {
        val account = Stack<Long>()
        account.push(db.executeSync("createAccount", map("name", "John")))
        val identifier = db.query().find(Account.view, account.peek()).identifier
        println("Account identifier: $identifier")
        fun printLatestVersion() {
            println("Latest version: " + latestVersion(identifier, Account.view))
        }

        printAccountHistory()
        printLatestVersion()

        account.push(db.executeSync("changeBalance", map("id", account.pop(), "inc", 10000)))
        printAccountHistory()
        printLatestVersion()

        account.push(db.executeSync("changeBalance", map("id", account.pop(), "inc", 10000)))
        printAccountHistory()
        printLatestVersion()

        account.push(db.executeSync("deleteAccount", map("id", account.pop())))
        printAccountHistory()
        printLatestVersion()

        Thread.sleep(1000)

        println("Current snapshot: ${currentSnapshot.get()}")
        println(db.query().find(Snapshot.view, currentSnapshot.get()))

    } finally {
        db.shutdown()
    }
}