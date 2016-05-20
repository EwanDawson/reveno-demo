package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager
import org.reveno.atp.api.commands.CommandContext
import org.reveno.atp.api.dynamic.AbstractDynamicCommand

data class Account private constructor (val name: String, val balance: Int, val childAccounts: List<String>, private val e: VersionedEntityDelegate) : VersionedEntity by e {
    constructor(name: String) : this (name, 0, listOf(), VersionedEntityDelegate(Account.type))
    operator fun plus(amount: Int) = copy(balance = balance + amount, e = e.update())
    operator fun minus(amount: Int) = copy(balance = balance - amount, e = e.update())
    fun close() = copy(e = e.delete())
    fun addChild(child: Account) = copy(childAccounts = childAccounts + child.identifier)
    override fun toString() = e.toString("name=$name, balance=$balance, childAccounts=$childAccounts")
    data class View(override val id: Long, override val identifier: String, val name: String, val balance: Int, val version: Long, val deleted: Boolean, val childAccounts: List<String>) : VersionedEntityView
    companion object {
        val type = "Account"
        val domain = Account::class.java
        val view = View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, account, r -> account.run {
            View(id, identifier, name, balance, version, deleted, childAccounts)
        }}}
    }
}

internal fun initAccountDomain(reveno: Reveno) {
    reveno.domain().apply {

        val accountUpdatePrecondition: (AbstractDynamicCommand, CommandContext) -> Boolean = { command, context ->
            val account = context.repo().get(Account.domain, command.longArg("id"))
            command.longArg("id") == VersionedEntityQuery(currentSnapshot.get()).find(account.identifier, Account.view).id
        }

        transaction("createAccount") { txn, ctx ->
            val id = txn.id(Account.domain)
            val newAccount = ctx.repo().store(id, Account(txn.arg<String>("name")))
            val entityChangedEvent = EntityChangedEvent(newAccount, id)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Created $newAccount")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .command()

        transaction("updateAccountBalance") { txn, ctx ->
            val beforeId = txn.longArg("id")
            val before = ctx.repo().get(Account.domain, beforeId)
            val afterId = txn.id(Account.domain)
            val after = ctx.repo().store(afterId, before.plus(txn.arg("amount")))
            val entityChangedEvent = EntityChangedEvent(after, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Updated balance of account ${before.identifier} from ${before.balance} to ${after.balance}")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand(accountUpdatePrecondition)
        .command()

        transaction("addChildAccount") { txn, ctx ->
            val beforeId = txn.longArg("id")
            val before = ctx.repo().get(Account.domain, beforeId)
            val afterId = txn.id(Account.domain)
            val after = ctx.repo().store(afterId, before.addChild(txn.arg("child")))
            val entityChangedEvent = EntityChangedEvent(after, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Added child to account ${before.identifier} from $before to $after")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand(accountUpdatePrecondition)
        .command()

        transaction("deleteAccount") { txn, ctx ->
            val beforeId = txn.longArg("id")
            val before = ctx.repo().get(Account.domain, beforeId)
            val afterId = txn.id(Account.domain)
            val after = ctx.repo().store(afterId, before.close())
            val entityChangedEvent = EntityChangedEvent(after, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Deleted $after")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand(accountUpdatePrecondition)
        .command()

        Account.map(reveno.domain())
    }
}