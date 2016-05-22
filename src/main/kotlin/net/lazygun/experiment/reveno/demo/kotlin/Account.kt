package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.api.Reveno
import org.reveno.atp.api.RevenoManager

data class Account private constructor (val name: String, val balance: Int, val childAccounts: Set<String>, private val e: VersionedEntityDelegate) : VersionedEntity by e {
    constructor(name: String) : this (name, 0, setOf(), VersionedEntityDelegate(Account.type))
    operator fun plus(amount: Int) = copy(balance = balance + amount, e = e.update())
    operator fun minus(amount: Int) = copy(balance = balance - amount, e = e.update())
    fun close() = copy(e = e.delete())
    fun addChild(identifier: String) = copy(childAccounts = childAccounts + identifier)
    override fun toString() = e.toString("name=$name, balance=$balance, childAccounts=$childAccounts")
    data class View(override val versionId: Long, override val entityId: String, val name: String, val balance: Int, val version: Long, val deleted: Boolean, val childAccounts: Set<String>) : VersionedEntityView
    companion object {
        val type = "Account"
        val domain = Account::class.java
        val view = View::class.java
        fun mapDomainToView(reveno: RevenoManager) { reveno.viewMapper(domain, view) { id, account, r -> account.run {
            View(id, entityId, name, balance, version, deleted, childAccounts)
        }}}
    }
}

data class CreateAccountCommand(val name: String)
data class UpdateAccountBalanceCommand(val versionId: Long, val amount: Int)
data class AddChildAccountCommand(val versionId: Long, val childEntityId: String)
data class CloseAccountCommand(val versionId: Long)
private data class AccountTransaction(val versionId: Long, val account: Account, val entityChange: EntityChange)

internal fun initAccountDomain(reveno: Reveno) {
    reveno.domain().apply {

        command(CreateAccountCommand::class.java, Long::class.java) { command, context ->
            val accountId = context.id(Account.domain)
            val account = Account(command.name)
            val entityChangeId = context.id(EntityChange.domain)
            val entityChange = EntityChange.create(entityChangeId, account, accountId)
            context.executeTxAction(AccountTransaction(accountId, account, entityChange))
            accountId
        }

        command(UpdateAccountBalanceCommand::class.java, Long::class.java) { command, context ->
            val beforeId = command.versionId
            val before = context.repo().get(Account.domain, beforeId)
            val afterId = context.id(Account.domain)
            val after = before + command.amount
            val entityChangeId = context.id(EntityChange.domain)
            val entityChange = EntityChange.update(entityChangeId, after, beforeId, afterId)
            context.executeTxAction(AccountTransaction(afterId, after, entityChange))
            afterId
        }

        command(AddChildAccountCommand::class.java, Long::class.java) { command, context ->
            val beforeId = command.versionId
            val before = context.repo().get(Account.domain, beforeId)
            val afterId = context.id(Account.domain)
            val after = before.addChild(command.childEntityId)
            val entityChangeId = context.id(EntityChange.domain)
            val entityChange = EntityChange.update(entityChangeId, after, beforeId, afterId)
            context.executeTxAction(AccountTransaction(afterId, after, entityChange))
            afterId
        }

        command(CloseAccountCommand::class.java, Long::class.java) { command, context ->
            val beforeId = command.versionId
            val before = context.repo().get(Account.domain, beforeId)
            val afterId = context.id(Account.domain)
            val after = before.close()
            val entityChangeId = context.id(EntityChange.domain)
            val entityChange = EntityChange.delete(entityChangeId, after, beforeId, afterId)
            context.executeTxAction(AccountTransaction(afterId, after, entityChange))
            afterId
        }

        transactionAction(AccountTransaction::class.java) { transaction, context ->
            transaction.run {
                if (transaction.entityChange.beforeVersionId != null) {
                    val latestVersion = VersionedEntityQuery(entityChange.snapshotId).find(account.entityId, Account.view)
                    if (latestVersion.versionId != transaction.entityChange.beforeVersionId)
                        throw IllegalStateException("${transaction.account.entityId} has been modified in another transaction")

                }
                context.repo().store(versionId, account)
                context.repo().store(entityChange.id, entityChange)
                context.repo().remap(entityChange.snapshotId, Snapshot.domain) { id, s -> s + entityChange }
                context.eventBus().publishEvent(entityChange)
            }
        }

        Account.mapDomainToView(reveno.domain())
    }
}