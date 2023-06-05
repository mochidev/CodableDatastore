# On Disk Representation

The on-disk representation of a ``DiskPersistence``.

## Overview

``Datastore``s are grouped together within a single ``DiskPersistence`` when
saved to disk, located at the user's chosen ``/Foundation/URL``.

### Persistence Store

The persistence store is the top-most level a ``DiskPersistence`` uses, and
contains a list of [snapshots](#Snapshots) and [backups](#Backups), a pointer
to the most recent snapshot, and some basic metadata such as version and last
modification date in `Manifest.json`.

The file layout is as follows:

```
- 📦 Path/To/Data.persistencestore/
    - 📃 Manifest.json
    - 📁 Snapshots/
    - 📁 Backups/
```

### Snapshots

Snapshots are saved in an organized grouping of folders based on the file names
of individual snapshots. This is to ensure file system performance does not get
impacted in the presence of many snapshots.

For ease of manual inspection, a [snapshot][#Snapshot] is encoded with the
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

A snapshot represents a collection of data stores at a given moment in time. It
is composed of a collection of data stores along with a manifest file that names
those data stores and links to their root objects.

Datastores are named using an auto-generated name: `A-%%%%%%%%%%%%%%%%.datastore`,
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

```
- 📦 2023-06-04 17-06-40 0011223344556677.snapshot
    - 📃 Manifest.json
    - 📂 Datastores/
        - 📦 A-%%%%%%%%%%%%%%%%.datastore
        - 📦 B-%%%%%%%%%%%%%%%%.datastore
        - 📦 C-%%%%%%%%%%%%%%%%.datastore
        - ...
```
