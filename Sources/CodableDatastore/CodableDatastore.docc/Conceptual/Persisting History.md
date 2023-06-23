# Persisting History

How ``CodableDatastore`` saves history, enabling a potentially limitless undo
stack.

## Overview

Due to how ``DiskPersistence`` organizes its files, it is possible for
``CodableDatastore`` to save each transaction in a historical record that
can be rewound. Three different historical records are planned to be supported:

- User-Created Backups
- Snapshots
- Multiple Roots

These goals inform how data stores are persisted on disk, and dictate the
design of the API needed to access them.

### User-Created Backups

The user of an app can request that a backup of their data store be taken
manually or regularly, and have these be displayed in a user interface for them
to export or restore.

Backups are created from full copies of the persisted datastores, and restores
are themselves copied from backups, making sure that they don't inadvertedly
change.

Backups utilize file-system block sharing when available, such as on APFS, and
as such should be a relatively cheap operating to complete.

### Snapshots

Snapshots are similar to backups, except they represent a live view into a data
store at one moment in time. Snapshots may be created by an app automatically
before a migration takes place, or the user attempts to perform an irreversible
change such as merging or replacing data during a sync operation.

Importantly, snapshots only create copies of data when they are created, and
otherwise offer a read-write view into their data store when made active.

Note: Using snapshots to partition data is not recommended, as backups will
only ever represent a single snapshot.

### Multiple Roots

Within a snapshot, and by extention backups, is a root object that identifies
the state of the data within each datastore. Because a new root object is
written at the end of every top-level transaction, they form a chain that can
be traversed to go back, or forward in the persistence's timeline.

A persistence can be configured in a few different ways:

- Support a set number of roots, such as the last 10 transactions.
- Support a dynamic number of roots, such as all edits within the last 28 days.
- Support a limitless number of roots, from the very first edit.
- Support a limitless number of roots, allowing the user to clear the history
  should they choose to do so.

In each scenario, we require a robust way of both representing this history,
but also being able to manipulate it, such as to clear out older entries.

``CodableDatastore`` already utilises a few tricks that make this possible.
Namely, changes to data always result in a new page, and as a result new index
manifests, new datastore roots, and new snapshot roots. The new index manifests
reference pages that both existed before, but also the new pages that got
created, while the roots above them simply point to the new and old copies down
the chain.

In order to properly maintain history, the new roots simply need to remember
which pages they are invalidating in the process of replacing them with new
ones. Then, when it comes time to cleaning up the oldest root, we only need to
delete the root chain it points to, along with the pages that the next newest
root replaces.

To make it easier to track such chains, the snapshot root should refer to the
root it replaces along with its tree of data. Then, we simply need to traverse
this chain to find the oldest root, along with the second oldest one that
specifies pages that are safe to delete once the oldest one is removed. As long
as roots are always cleaned up from oldest to newest, the next oldest root will
always be ready to remove.

To properly support _rewinding_ history, we should also store the pages that got
introduced with each new root. Then, if we need to trim our history from the
_other_ direction, such as would be the case if a user undid an action, but
instead of redoing it chose to make a different edit, we could clear the edits
made in that moment. That said, deleting undone history is also a configurable
transaction, should the user choose to re-explore that branch outside of your
app's use case.

These branches will however be deleted if older history is pruned such that the
oldest root that links multiple branches is itself deleted. This is made
possible by forward references that are saved ontop of older roots in the
process of creating the new root that replaces them. Since this edit would
atomically replace a root but would not change the consistency requirements of
the data that root points to, it is a safe operation to do after the fact.

#### Multiple Reading Processes

In order to support multiple processes that are reading a potentially live
datastore, as can be the case in an extension of an app, it is always suggested
to have a few transactions always present, such that if a process grabs a
read-only view based on an out-of-date root, that root does not immediately
vanish from under them as they read its pages.
