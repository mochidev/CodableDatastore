# On Disk Representation

The on-disk representation of a ``DiskPersistence``.

## File Storage Overview

``Datastore``s are grouped together within a single ``DiskPersistence`` when
saved to disk, located at the user's chosen ``/Foundation/URL``.

### Persistence Store

The persistence store is the top-most level a ``DiskPersistence`` uses, and
contains a list of [snapshots](#Snapshots) and [backups](#Backups), a pointer
to the most recent snapshot, and some basic metadata such as version and last
modification date in `Info.json`.

The file layout is as follows:

```
- 📦 Path/To/Data.persistencestore/
    - 📃 Info.json
    - 📁 Snapshots/
    - 📁 Backups/
```

### Snapshots

Snapshots are saved in an organized grouping of folders based on the file names
of individual snapshots. This is to ensure file system performance does not get
impacted in the presence of many snapshots.

For ease of manual inspection, a [snapshot](#Snapshot) is encoded with the
following date format for when it was created:
`yyyy-MM-dd HH-mm-ss %%%%%%%%%%%%%%%%.snapshot`, where `%%` is a random hex byte
to ensure uniqueness. Keep in mind that the active snapshot is not necessarily
the one with the most recent date, as the date and time can be changed by
the user of the system at any time. The timezone used for the date is locked
to GMT.

Folders are grouped by year, month and day, hour and minute, followed by the
actual file names: `/yyyy/MM-dd/HH-mm/yyyy-MM-dd HH-mm-ss %%%%%%%%%%%%%%%%.snapshot`.

```
- 📂 Snapshots/
    - 📂 yyyy/
        - 📂 MM-dd/
            - 📂 HH-mm/
                - 📦 yyyy-MM-dd HH-mm-ss %%%%%%%%%%%%%%%%.snapshot/
```

### Backups

Backups are implemented in the same way as [Snapshots](#Snapshots), and in fact
store the same types. The difference, however, is that Backups are something
you can expect the user of your app to generate, and won't be deleted
automatically. Snapshots, however, are likely to be automatically generated
through the use of your app, and are just as likely to be deleted automatically
when something like storage space is necessary for the store to continue
operating.

```
- 📂 Backups/
    - 📂 yyyy/
        - 📂 MM-dd/
            - 📂 HH-mm/
                - 📦 yyyy-MM-dd HH-mm-ss %%%%%%%%%%%%%%%%.snapshot/
```

### Snapshot

A snapshot represents a collection of [data stores](#Data-Store) at a given
moment in time. It is composed of a collection of data stores along with a
manifest file that names those data stores and links to their root objects.

Data stores are named using an auto-generated name: `A-%%%%%%%%%%%%%%%%.datastore`,
where `A` is a string derived from the key, and `%%` is a random hex byte
to ensure uniqueness. The original keys are stored in the Manifest. As
a limited number of data stores are expected for a given app, no further
optimizations are used.

Any transaction performed on a ``Persistence`` is guaranteed to leave a snapshot
in a consistent state. This is because pointers to the roots of each datastore
are updated all at once in the `Manifest.json` of a snapshot. However, if a
read-only persistence were to load a Manifest that was changed before it read
the root, it would still reference an older root that should be kept around
long enough for it to operate successfully.

The `Dirty` file is only present if a snapshot didn't complete the process of
cleaning up files after writing data. Although the snapshot itself should be in
a consistent state, extra files might still be present, and thus a cleanup pass
should happen.

A data store supports external writes as a form of patches on top of the main
data set. These patches are then consumed by the main process handling the
datastore, one at a time. Patches are not guaranteed to be applied, especially
if their prior state no longer matches the expectations they were based on.

```
- 📦 2023-06-04 17-06-40 0011223344556677.snapshot
    - 📃 Manifest.json
    - 📃 Dirty
    - 📂 Datastores/
        - 📦 A-%%%%%%%%%%%%%%%%.datastore
        - 📦 B-%%%%%%%%%%%%%%%%.datastore
        - 📦 C-%%%%%%%%%%%%%%%%.datastore
        - ...
    - 📂 Inbox/
        - 📃 yyyy-MM-dd HH-mm-ss %%%%%%%%%%%%%%%%.patch
        - ...
```

### Data Store

A data store stores [pages](#Direct-Index–Page-Format) of data for a single
key and type, along with its [indexes](#Data-Store-Index). The root object that
points to each index, and contains metadata about those indices, is stored in
`Root`. Only root objects for the past 10 or so edits are kept around, so other
readers of a data store can reliably read from a data store while other writes
are happening, potentially destroying obsolete records in the process.
[Snapshots](#Snapshot) point to a single root object at a time.

Root objects are written as new files, making sure to update the parent
[snapshot](#Snapshot) before deleting older root objects. Once a new root
object is written, older ones may be updated solely to indicate which pages
they uniquely refer to, so this information only needs to be re-calculated if
the store is found to be dirty.

Two kinds of indexes are supported: direct indexes, and secondary indexes.
Direct indexes contain the entire instance and can be queried without a layer
of indirection, though having more than one can result in higher write overhead
along with more space usage. Secondary indexes only store the value their index
refers to, along with a pointer to the page in the primary direct index
(ie. the one based on the identifier of each instance).

If indexes defined in code change, a difference will be calculated, deleting
any old indexes, and re-computing pages for any new or updated indexes. This
process will happen automatically on the first read (if the index changed or
is new) or write (to any index) to the datastore, though it can be pre-empted
by warming the datastore first and displaying appropriate UI with progress.

```
- 📦 A-0011223344556677.datastore
    - 📂 Root/
        - 📃 yyyy-MM-dd HH-mm-ss %%%%%%%%%%%%%%%%.json
        - ...
    - 📂 DirectIndexes/
        - 📦 Primary.datastoreindex
        - 📦 A-%%%%%%%%%%%%%%%%.datastoreindex
        - 📦 B-%%%%%%%%%%%%%%%%.datastoreindex
        - 📦 C-%%%%%%%%%%%%%%%%.datastoreindex
        - ...
    - 📂 SecondaryIndex/
        - 📦 A-%%%%%%%%%%%%%%%%.datastoreindex
        - 📦 B-%%%%%%%%%%%%%%%%.datastoreindex
        - 📦 C-%%%%%%%%%%%%%%%%.datastoreindex
        - ...
```

### Data Store Index

A data store index is a collection of [pages](#Direct-Index–Page-Format), along
with a single manifest file that points to the set of pages in their relevant
order. Other information about the index is stored within the [data store](#Data-Store).

Like the root objects in the data store, manifests are always written as
new files, making sure to update the parent [snapshot](#Snapshot) before
deleting older manifests. Once a new manifest is written, older ones may be
updated solely to indicate which pages they uniquely refer to, so this
information only needs to be re-calculated if the store is found to be dirty.
Pages that are uniquely referred to can be safely deleted 

```
- 📦 A-0011223344556677.datastoreindex
    - 📂 Manifest/
        - 📃 yyyy-MM-dd HH-mm-ss %%%%%%%%%%%%%%%%.manifest
        - ...
    - 📂 Pages/
        - 📂 yyyy/
            - 📂 MM-dd/
                - 📂 HH-mm/
                    - 📃 yyyy-MM-dd HH-mm-ss %%%%%%%%%%%%%%%%.datastorepage
```

### Direct Index Page Format

A direct index page is a collection of encoded instances along with some
limited metadata, such as the index value they are sorted under and their
version. These data blocks are prefixed accordingly such that they can span
multiple pages should data be large.

```
PAGE
<15
ata": "data" 
}
=45
8 version1
7 object3

{
    "data": "data" 
}
>30
8 version1
7 object4

{
    "d
```

In the example above, a page explicitely starts with `PAGE\n`. The `\n` here
indicates the page is using the first version of the human-readable page format.
`PAGE ` (with a space) is reserved for future iterations of human-readable
formats, while `PAGE\0` is reserved for a compressed and optionally encrypted
binary representation.

This is followed by a number of data blocks, that can take one of four forms:
`<`, `=`, `>`, and `~`:
- `<` indicates a block that represents the tail-end of a block from a previous
  page. It is formed by collating `<`, a decimal number of the payload size,
  a new line `\n`, the payload, and a final new line `\n`. The payload is
  concatenated to a previous payload to build the entry.
    - If a block still doesn't fit on a single page, the preceding symbol is `~`
      instead of `<`, and the payload size used is the amount of data contained
      on _this_ page; the next page must be opened as well to form a full entry.
- `=` indicates a block completely represented on this page. It is formed by
  collating `=`, a decimal number of the payload size, a new line `\n`, the
  payload, and a final new line `\n`.
- `>` indicates a block that is started on this page, but requires more space
  to be fully represented, and the next page should be opened. It is formed by
  collating `=`, a decimal number of the payload size, a new line `\n`, the
  payload, and a final new line `\n`.

Once concatenated, an entry has the following structure:
- A decimal number, indicating the size in bytes of the version string.
- A space ` `.
- A version string.
- A new line `\n`.
- For non-primary indexes:
    - A decimal number, indicating the size in bytes of the index value.
    - A space ` `.
    - The index value.
    - A new line `\n`.
- A decimal number, indicating the size in bytes of the identifier value.
- A space ` `.
- The identifier value.
- A new line `\n`.
- One more new line `\n` to separate the headers from the content.
- The encoded data for the instance.


### Secondary Index Page Format

Similarly to a [direct index page](#Direct-Index-Page-Format), a secondary index
page is a collection of entries, however, the data stored in each entry is
limited to the index value and a pointer to the entry within the primary direct
index's page.

```
PAGE
<3
ct2
=16
5 date3

object1
=16
5 date3

object7
>13
5 date4

obje
```

In the above example, the overall page format is identical, but the payloads
have a slightly different structure:
- A decimal number, indicating the size in bytes of the index value.
- A space ` `.
- The index value.
- A new line `\n`.
- One more new line `\n` to separate the headers from the content.
- The identifier of the object as stored in the primary index.

## Operations

The four common database operations are outlined below:

### Persisting Instances

1. If a [persistence store](#Persistence-Store) doesn't already exist, one is
   created with an empty [snapshot](#Snapshot). Note that an empty persistence
   store and snapshot may be created in advance via ``DiskPersistence/createPersistenceIfNecessary()`` to
   reserve the file path should the need arise.
2. If a [data store](#Data-Store) does not yet exist, it is created on first
   write.
3. If a snapshot is marked as dirty, it is scanned for abandoned pages and they
   are cleaned up, usually by making a new snapshot so lost data can potentially
   be recovered (TODO: Figure out a mechanism for data recovery).
4. If [indexes](#Data-Store-Index) already exist, their types and keys are
   compared to determine if they must change.
    - Any removed or updated index are staged for deletion.
    - Any new or updated indexes are staged for creation.
5. If a primary direct index already exists, its manifest is loaded, and
   a binary search begins by loading pages, comparing object index values until
   a suitable page is found.
    - If the primary direct index must be re-built, this process is skipped and
      the entire data set is streamed in so it can be re-indexed.
6. The snapshot is marked dirty by creating an empty `Dirty` file.
7. A copy of the page where an update must be made, and the new instance is
   either updated at the existing location, or inserted appropriately. If the
   page grows in size, data blocks at the boundary are trimmed and a new page
   is made after it.
8. A new manifest for the primary index is written, inserting the updated or
   new pages.
9. Other indexes are subsequently updated by using the old value if exists to
   look up their locations.
10. A new root is written for the data store that was updated.
11. The snapshot manifest is updated to point to the new root.
12. Older roots and manifests are updated with any unique page references they
    may contain, and are pruned if necessary.
13. Non-referenced indexes that were staged are similarly pruned if they have
    no references.
14. Non-referenced pages in existing indexes are also pruned.
15. The `Dirty` file is removed for the snapshot.

The above is performed for a single transaction, so multiple updates can take
advantage of this and be written at the same time. Updates made without
a transaction are performed within one automatically, though these edits may
be coalesced if they happen between writes to disk from multiple sources.

As pages end up getting loaded repeatedly, manifests are cached in memory in
persistences that write to disk, along with pages ordered by when they were
last accessed.
