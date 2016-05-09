* You may only use a single view mapping per domain class
* Event handling doesn't happen inside the transaction. Sync events will be processed in the order in which they
  are fired, but they are only handled after the transaction has committed.
* _Does `conditionalCommand` run before of after the transaction handler? Can we specify post-conditions on the
  transaction?_