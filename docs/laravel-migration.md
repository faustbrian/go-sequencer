# Migrating Laravel one-time operations

Inventory every Laravel or Cline Sequencer operation with its historical name,
execution evidence, dependencies, transaction assumptions, retries, and side
effects. Preserve the old identifier in tags or audit metadata.

Replace container lookup with a concrete Go handler whose dependencies are
constructor fields. Convert `shouldRun` logic to an explicit condition with a
safe reason. Convert queueing to identity-only dispatch and treat worker
execution as a separate transaction.

Import successful historical operations into the ledger through a reviewed
application migration or baseline tool. Never replay them merely because the
new ledger starts empty. Verify Postal and Location histories before switching
deployment control to Go.
