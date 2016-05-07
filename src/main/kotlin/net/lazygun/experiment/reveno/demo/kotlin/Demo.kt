package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.core.Engine
import org.reveno.atp.utils.MapUtils.map
import java.time.Instant
import java.util.*

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
    }
}

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

        Thread.sleep(1000)

        println("Current snapshot: ${currentSnapshot.get()}")
        println(reveno.query().find(Snapshot.view, currentSnapshot.get()))

    } finally {
        reveno.shutdown()
    }
}