package net.lazygun.experiment.reveno.demo.kotlin

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.reveno.atp.api.Reveno
import org.reveno.atp.core.Engine

import org.reveno.atp.utils.MapUtils.map;

data class Account(val name: String, val balance: Int) {
    fun add(amount: Int) = Account(name, balance + amount)
}

data class AccountView(val id: Long, val name: String, val balance: Int)

val account = Account::class.java
val accountView = AccountView::class.java

fun init(folder: String): Reveno {
    val reveno = Engine(folder)
    reveno.domain()
            .transaction("createAccount") { t, c -> c.repo().store(t.id(), Account(t.arg(), 0)) }
            .uniqueIdFor(account)
            .command()
    reveno.domain()
            .transaction("changeBalance") { t, c -> c.repo().store(t.longArg(), c.repo().get(account, t.arg()).add(t.intArg("inc")))}
            .command()
    reveno.domain().viewMapper(account, accountView,
            { id, e, r -> AccountView(id, e.name, e.balance) })
    return reveno
}

fun checkState(reveno: Reveno, accountId: Long): Unit {
    assertNotNull(reveno.query().find(accountView, accountId))
    assertEquals("John", reveno.query().find(accountView, accountId).name)
    assertEquals(10000, reveno.query().find(accountView, accountId).balance)
}

fun main(args: Array<String>) {
    var reveno = init("data/reveno-sample")
    reveno.startup()

    val accountId: Long = reveno.executeSync("createAccount", map("name", "John"))
    reveno.executeSync<Account>("changeBalance", map("id", accountId, "inc", 10000))

    checkState(reveno, accountId)
    reveno.shutdown()

    reveno = init("data/reveno-sample")
    reveno.startup()

    checkState(reveno, accountId)
    reveno.shutdown()
}