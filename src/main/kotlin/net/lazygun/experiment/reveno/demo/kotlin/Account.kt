package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager
import org.reveno.atp.api.commands.CommandContext
import org.reveno.atp.api.dynamic.AbstractDynamicCommand

data class Account private constructor (val name: String, val balance: Int, val childAccounts: List<String>) {
    constructor(name: String) : this (name, 0, listOf())
    operator fun plus(amount: Int) : Account = copy(balance = balance + amount)
    operator fun minus(amount: Int) : Account = copy(balance = balance - amount)
    fun addChild(child: Entity) : Account = copy(childAccounts = childAccounts + child.entity.identifier)
    data class Entity private constructor(val entity: VersionedEntity<Account>) {
        constructor(account: Account) : this(VersionedEntity(account, type))
        fun update(mutator: (Account) -> Account) : Entity = Entity(entity.update(mutator))
        fun delete() : Entity = Entity(entity.delete())
    }
    data class View(override val id: Long, override val identifier: String, val name: String, val balance: Int, val version: Long, val deleted: Boolean, val childAccounts: List<String>) : VersionedEntityView
    companion object {
        val type = "Account"
        val domain = Entity::class.java
        val view = View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(id, e.entity.identifier, e.entity.value.name, e.entity.value.balance, e.entity.version, e.entity.deleted, e.entity.value.childAccounts)
        }}
    }
}

internal fun initAccountDomain(reveno: Reveno) {
    reveno.domain().apply {

        val accountUpdatePrecondition: (AbstractDynamicCommand, CommandContext) -> Boolean = { command, context ->
            val account = context.repo().get(Account.domain, command.longArg("id"))
            command.longArg("id") == VersionedEntityQuery(currentSnapshot.get()).find(account.entity.identifier, Account.view).id
        }

        transaction("createAccount") { txn, ctx ->
            val id = txn.id(Account.domain)
            val newAccount = ctx.repo().store(id, Account.Entity(txn.arg()))
            val entityChangedEvent = EntityChangedEvent(newAccount.entity, id)
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
            val after = ctx.repo().store(afterId, before.update({ a: Account -> a.plus(txn.arg("amount"))}))
            val entityChangedEvent = EntityChangedEvent(after.entity, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Updated balance of account ${before.entity.identifier} from ${before.entity.value.balance} to ${after.entity.value.balance}")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand(accountUpdatePrecondition)
        .command()

        transaction("addChildAccount") { txn, ctx ->
            val beforeId = txn.longArg("id")
            val before = ctx.repo().get(Account.domain, beforeId)
            val afterId = txn.id(Account.domain)
            val after = ctx.repo().store(afterId, before.update({ a: Account -> a.addChild(txn.arg("child")) }))
            val entityChangedEvent = EntityChangedEvent(after.entity, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Added child to account ${before.entity.identifier} from ${before.entity.value} to ${after.entity.value}")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand(accountUpdatePrecondition)
        .command()

        transaction("deleteAccount") { txn, ctx ->
            val beforeId = txn.longArg("id")
            val before = ctx.repo().get(Account.domain, beforeId)
            val afterId = txn.id(Account.domain)
            val after = ctx.repo().store(afterId, before.delete())
            val entityChangedEvent = EntityChangedEvent(after.entity, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Deleted ${after.entity}")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand(accountUpdatePrecondition)
        .command()

        Account.map(reveno.domain())
    }
}