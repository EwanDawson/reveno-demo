package net.lazygun.experiment.reveno.demo.kotlin

import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.longs.LongLists

internal fun list(head: Collection<Long>, tail: Collection<Long>) = LongArrayList().apply { addAll(head); addAll(tail) }
internal fun list(head: Long, tail: Collection<Long>) = LongArrayList().apply { add(head); addAll(tail) }
internal fun list(head: Long) = LongLists.singleton(head)
internal fun list() = LongLists.EMPTY_LIST
