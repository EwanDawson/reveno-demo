package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager

data class Account private constructor (val name: String, val balance: Int) {
    constructor(name: String) : this (name, 0)
    operator fun plus(amount: Int) : Account = copy(balance = balance + amount)
    data class Entity private constructor(val entity: VersionedEntity<Account>) {
        constructor(account: Account) : this(VersionedEntity(account, type))
        fun update(mutator: (Account) -> Account) : Entity = Entity(entity.update(mutator))
        fun delete() : Entity = Entity(entity.delete())
    }
    data class View(val id: Long, val identifier: String, val name: String, val balance: Int, val version: Long, val deleted: Boolean)
    companion object {
        val type = "Account"
        val domain = Entity::class.java
        val view = View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(id, e.entity.identifier, e.entity.value.name, e.entity.value.balance, e.entity.version, e.entity.deleted)
        }}
    }
}

internal fun initAccountDomain(reveno: Reveno) {
    reveno.domain().apply {

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

        transaction("updateAccount") { txn, ctx ->
            val beforeId = txn.longArg("id")
            val before = ctx.repo().get(Account.domain, beforeId)
            val afterId = txn.id(Account.domain)
            val after = ctx.repo().store(afterId, before.update(txn.arg<(Account)->Account>("update")))
            val entityChangedEvent = EntityChangedEvent(after.entity, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Changed balance of account ${before.entity.value.name} from ${before.entity.value.balance} to ${after.entity.value.balance}")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand { command, context ->
            val account = context.repo().get(Account.domain, command.longArg("id"))
            command.longArg("id") == latestVersion(currentSnapshot.get(), account.entity.identifier, Account.view).id
        }
        .command()

        transaction("deleteAccount") { txn, ctx ->
            val beforeId = txn.longArg("id")
            val before = ctx.repo().get(Account.domain, beforeId)
            val afterId = txn.id(Account.domain)
            val after = ctx.repo().store(afterId, before.delete())
            val entityChangedEvent = EntityChangedEvent(after.entity, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Deleted $after")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand { command, context ->
            val account = context.repo().get(Account.domain, command.longArg("id"))
            command.longArg("id") == latestVersion(currentSnapshot.get(), account.entity.identifier, Account.view).id
        }
        .command()

        Account.map(reveno.domain())
    }
}