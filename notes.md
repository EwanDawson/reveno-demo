* You may only use a single view mapping per domain class
* Event handling doesn't happen inside the transaction. Sync events will be processed in the order in which they
  are fired, but they are only handled after the transaction has committed.
* _Does `conditionalCommand` run before of after the transaction handler? Can we specify post-conditions on the
  transaction?_
* Don't use a Kotlin `lazy` delegated property in your domain classes, as Protostuff will get into an infinite loop
  when trying to serialise the field. It doesn't seem to be possible to mark a delegated property as transient,
  so don't use them.

Instead of viewing the latest version of an entity for the current Snapshot, we need to do it by EntityChange id.
This is because, when are dealing with more than one entity instance, other updates may have occurred in the current
 Snapshot since we obtained the first entity. Thus, we may see an inconsistent view over the entities (this matters
 if there are invariants that hold between entity instances, or if we are trying to compute some value over a number
 of entity instances, and we want that value to represent a specific moment in time).
 So, when we start our read operations, we need to set not the current Snapshot id, but the latest EntityChange id
 for the current Snapshot. Then, when obtaining the "current" version of entity instances, we only consider
  Entity Changes that have an id less than or equal to the current EntityChange id, and are part of the history
  of the current Snapshot. - Implemented this in `VersionedEntityQuery` class.

### TODOS

* TODO Experiment with tree structures
* TODO Create performance test
** Run various commands at random, creating a random version control structure, and creating and modifying entities
   at random within this structure.
** Extend the test to run multi-threaded, dealing with concurrent modification exceptions