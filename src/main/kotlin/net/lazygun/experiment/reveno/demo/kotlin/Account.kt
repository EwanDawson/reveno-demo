package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager

data class Account(val name: String, val balance: Int) {
    operator fun plus(amount: Int) : Account = copy(balance = balance + amount)
    data class Entity private constructor(val entity: VersionedEntity<Account>) {
        constructor(name: String, balance: Int) : this(VersionedEntity(Account(name, balance), type))
        private fun update(mutator: (Account) -> Account) : Entity = Entity(entity.update(mutator))
        fun delete() : Entity = Entity(entity.delete())
        operator fun plus(amount: Int) : Entity = update { it.plus(amount) }
    }
    data class View(val identifier: String, val name: String, val balance: Int, val version: Long, val deleted: Boolean)
    companion object {
        val type = "Account"
        val domain = Entity::class.java
        val view = View::class.java
        fun map(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, e, r ->
            View(e.entity.identifier, e.entity.value.name, e.entity.value.balance, e.entity.version, e.entity.deleted)
        }}
    }
}

internal fun initAccountDomain(reveno: Reveno) {
    reveno.domain().apply {

        transaction("createAccount") { txn, ctx ->
            val id = txn.id(Account.domain)
            val newAccount = ctx.repo().store(id, Account.Entity(txn.arg(), 0))
            val entityChangedEvent = EntityChangedEvent(newAccount.entity, id)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Created $newAccount")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .command()

        transaction("changeBalance") { txn, ctx ->
            val beforeId = txn.arg<Long>()
            val before = ctx.repo().get(Account.domain, beforeId)
            val afterId = txn.id(Account.domain)
            val after = ctx.repo().store(afterId, before + txn.intArg("inc"))
            val entityChangedEvent = EntityChangedEvent(after.entity, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Changed balance of account ${before.entity.value.name} from ${before.entity.value.balance} to ${after.entity.value.balance}")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand { cmd, ctx ->
            val accountToChange = ctx.repo().get(Account.domain, cmd.arg()).entity
            reveno.query().select(Account.view) { acc ->
                acc.identifier == accountToChange.identifier && acc.version == accountToChange.version + 1
            }
            .isEmpty()
        }
        .command()

        transaction("deleteAccount") { txn, ctx ->
            val beforeId = txn.arg<Long>()
            val before = ctx.repo().get(Account.domain, beforeId)
            val afterId = txn.id(Account.domain)
            val after = ctx.repo().store(afterId, before.delete())
            val entityChangedEvent = EntityChangedEvent(after.entity, beforeId, afterId)
            entityChange(txn.id(EntityChange.domain), entityChangedEvent, ctx.repo())
            ctx.eventBus().publishEvent(entityChangedEvent)
            println("Deleted $after")
        }
        .uniqueIdFor(Account.domain, EntityChange.domain)
        .conditionalCommand { cmd, ctx ->
            val accountToDelete = ctx.repo().get(Account.domain, cmd.arg()).entity
            reveno.query()
            .select(Account.view) { acc ->
                acc.identifier == accountToDelete.identifier && acc.version == accountToDelete.version + 1
            }
            .isEmpty()
        }
        .command()

        Account.map(reveno.domain())
    }
}