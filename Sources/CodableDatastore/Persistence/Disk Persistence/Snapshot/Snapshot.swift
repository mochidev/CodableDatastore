//
//  Snapshot.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-09.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias SnapshotIdentifier = DatedIdentifier<Snapshot<ReadOnly>>

/// A type that manages access to a snapshot on disk.
actor Snapshot<AccessMode: _AccessMode> {
    /// The identifier of the snapshot.
    ///
    /// This is used to determine where on disk the snapshot is stored.
    let id: SnapshotIdentifier
    
    /// The persistence the stapshot is a part of.
    ///
    /// Prefer to access ``Snapshot/persistence`` instead, which offers non-optional access to the same persistence.
    unowned let persistence: DiskPersistence<AccessMode>
    
    /// A flag indicating if this is a backup snapshot.
    ///
    /// This is used to determine where on disk the snapshot is stored.
    let isBackup: Bool
    
    /// A cached instance of the manifest as last loaded from disk.
    var cachedManifest: SnapshotManifest?
    
    /// A cached instance of the current iteration as last loaded from disk.
    var cachedIteration: SnapshotIteration?
    
    /// A pointer to the last manifest updater, so updates can be serialized after the last request.
    var lastUpdateManifestTask: Task<Sendable, Error>?
    
    /// The loaded datastores.
    var datastores: [DatastoreIdentifier: DiskPersistence<AccessMode>.Datastore] = [:]
    
    init(
        id: SnapshotIdentifier,
        persistence: DiskPersistence<AccessMode>,
        isBackup: Bool = false
    ) {
        self.id = id
        self.persistence = persistence
        self.isBackup = isBackup
    }
}

// MARK: - Common URL Accessors
extension Snapshot {
    /// The URL that points to the Snapshot directory.
    nonisolated var snapshotURL: URL {
        guard let components = try? id.components else { preconditionFailure("Components could not be determined for Snapshot.") }
        
        let baseURL = isBackup ? persistence.backupsURL : persistence.snapshotsURL
        
        return baseURL
            .appendingPathComponent(components.year, isDirectory: true)
            .appendingPathComponent(components.monthDay, isDirectory: true)
            .appendingPathComponent(components.hourMinute, isDirectory: true)
            .appendingPathComponent("\(id).snapshot", isDirectory: true)
    }
    
    /// The URL that points to the Manifest.json file.
    nonisolated var manifestURL: URL {
        snapshotURL.appendingPathComponent("Manifest.json", isDirectory: false)
    }
    
    /// The URL that points to the Dirty file.
    nonisolated var dirtyURL: URL {
        snapshotURL.appendingPathComponent("Dirty", isDirectory: false)
    }
    
    /// The URL that points to the `Iterations` directory.
    nonisolated var iterationsURL: URL {
        snapshotURL.appendingPathComponent("Iterations", isDirectory: true)
    }
    
    nonisolated func iterationURL(for id: SnapshotIterationIdentifier) -> URL {
        guard let components = try? id.components else { preconditionFailure("Components could not be determined for Snapshot.") }
        
        return iterationsURL
            .appendingPathComponent(components.year, isDirectory: true)
            .appendingPathComponent(components.monthDay, isDirectory: true)
            .appendingPathComponent(components.hourMinute, isDirectory: true)
            .appendingPathComponent("\(id).json", isDirectory: false)
    }
    
    /// The URL that points to the Datastores directory.
    nonisolated var datastoresURL: URL {
        snapshotURL.appendingPathComponent("Datastores", isDirectory: true)
    }
    
    /// The URL that points to the Inbox directory.
    nonisolated var inboxURL: URL {
        snapshotURL.appendingPathComponent("Inbox", isDirectory: true)
    }
}

// MARK: - Snapshot Manifest Management
extension Snapshot {
    /// Load the manifest from disk, or create a suitable starting value if such a file does not exist.
    private func loadManifest() throws -> SnapshotManifest {
        do {
            let data = try Data(contentsOf: manifestURL)

            let manifest = try JSONDecoder.shared.decode(SnapshotManifest.self, from: data)

            cachedManifest = manifest
            return manifest
        } catch URLError.fileDoesNotExist, CocoaError.fileNoSuchFile, CocoaError.fileReadNoSuchFile, POSIXError.ENOENT {
            return SnapshotManifest(id: id, modificationDate: Date())
        } catch {
            throw error
        }
    }
    
    /// Load an iteration from disk, or create a suitable starting value if such a file does not exist.
    private func loadIteration(for iterationID: SnapshotIterationIdentifier) throws -> SnapshotIteration {
        do {
            let data = try Data(contentsOf: iterationURL(for: iterationID))

            let iteration = try JSONDecoder.shared.decode(SnapshotIteration.self, from: data)

            cachedIteration = iteration
            return iteration
        } catch {
            throw error
        }
    }
    
    /// Write the specified manifest to the store, and cache the results in ``Snapshot/cachedManifest``.
    private func write(manifest: SnapshotManifest) throws where AccessMode == ReadWrite {
        /// Make sure the directories exists first.
        if cachedManifest == nil {
            try FileManager.default.createDirectory(at: snapshotURL, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: datastoresURL, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: inboxURL, withIntermediateDirectories: true)
        }

        /// Encode the provided manifest, and write it to disk.
        let data = try JSONEncoder.shared.encode(manifest)
        try data.write(to: manifestURL, options: .atomic)

        /// Update the cache since we know what it should be.
        cachedManifest = manifest
    }
    
    /// Write the specified iteration to the store, and cache the results in ``Snapshot/cachedIteration``.
    private func write(iteration: SnapshotIteration) throws where AccessMode == ReadWrite {
        let iterationURL = iterationURL(for: iteration.id)
        /// Make sure the directories exists first.
        try FileManager.default.createDirectory(at: iterationURL.deletingLastPathComponent(), withIntermediateDirectories: true)

        /// Encode the provided iteration, and write it to disk.
        let data = try JSONEncoder.shared.encode(iteration)
        try data.write(to: iterationURL, options: .atomic)

        /// Update the cache since we know what it should be.
        cachedIteration = iteration
    }

    /// Load and update the manifest in an updater, returning the task for the updater.
    ///
    /// This method loads the ``SnapshotManifest`` from cache, offers it to be mutated, then writes it back to disk, if it changed. It is up to the caller to update the modification date of the store.
    ///
    /// - Note: Calling this method when no manifest exists on disk will create it, even if no changes occur in the block.
    /// - Parameter updater: An updater that takes a mutable reference to a manifest, and will forward the returned value to the caller.
    /// - Returns: A ``/Swift/Task`` which contains the value of the updater upon completion.
    func updateManifest<T>(
        updater: @Sendable @escaping (_ manifest: inout SnapshotManifest, _ iteration: inout SnapshotIteration) async throws -> T
    ) -> Task<T, Error> where AccessMode == ReadWrite {
        if let (manifest, iteration) = SnapshotTaskLocals.manifest {
            return Task {
                var updatedManifest = manifest
                var updatedIteration = iteration
                let returnValue = try await updater(&updatedManifest, &updatedIteration)
                
                guard updatedManifest == manifest, updatedIteration == iteration
                else { throw DiskPersistenceInternalError.nestedSnapshotWrite }
                
                return returnValue
            }
        }
        
        /// Grab the last task so we can chain off of it in a serial manner.
        let lastUpdaterTask = lastUpdateManifestTask
        let updaterTask = Task {
            /// We don't care if the last request throws an error or not, but we do want it to complete first.
            _ = try? await lastUpdaterTask?.value

            /// Load the manifest so we have a fresh copy, unless we have a cached copy already.
            var manifest = try cachedManifest ?? self.loadManifest()
            var iteration: SnapshotIteration
            if let cachedIteration, cachedIteration.id == manifest.currentIteration {
                iteration = cachedIteration
            } else if let iterationID = manifest.currentIteration {
                iteration = try self.loadIteration(for: iterationID)
            } else {
                let date = Date()
                iteration = SnapshotIteration(id: SnapshotIterationIdentifier(date: date), creationDate: date)
            }

            /// Let the updater do something with the manifest, storing the variable on the Task Local stack.
            let returnValue = try await SnapshotTaskLocals.$manifest.withValue((manifest, iteration)) {
                try await updater(&manifest, &iteration)
            }
            
            /// Only write to the store if we changed the manifest for any reason
            if iteration.isMeaningfullyChanged(from: cachedIteration) {
                iteration.creationDate = Date()
                iteration.id = SnapshotIterationIdentifier(date: iteration.creationDate)
                iteration.precedingIteration = cachedIteration?.id
                
                try write(iteration: iteration)
            }
            
            manifest.currentIteration = iteration.id
            
            /// Only write to the store if we changed the manifest for any reason
            if manifest != cachedManifest {
                try write(manifest: manifest)
            }
            return returnValue
        }
        /// Assign the task to our pointer so we can depend on it the next time. Also, re-wrap it so we can keep proper type information when returning from this method.
        lastUpdateManifestTask = Task { try await updaterTask.value }

        return updaterTask
    }

    /// Load the manifest in an accessor, returning the task for the updater.
    ///
    /// This method loads the ``SnapshotManifest`` from cache.
    ///
    /// - Parameter accessor: An accessor that takes an immutable reference to a manifest, and will forward the returned value to the caller.
    /// - Returns: A ``/Swift/Task`` which contains the value of the updater upon completion.
    func readManifest<T>(
        accessor: @Sendable @escaping (_ manifest: SnapshotManifest, _ iteration: SnapshotIteration) async throws -> T
    ) -> Task<T, Error> {
        
        if let (manifest, iteration) = SnapshotTaskLocals.manifest {
            return Task { try await accessor(manifest, iteration) }
        }
        
        /// Grab the last task so we can chain off of it in a serial manner.
        let lastUpdaterTask = lastUpdateManifestTask
        let readerTask = Task {
            /// We don't care if the last request throws an error or not, but we do want it to complete first.
            _ = try? await lastUpdaterTask?.value

            /// Load the manifest so we have a fresh copy, unless we have a cached copy already.
            let manifest = try cachedManifest ?? self.loadManifest()
            var iteration: SnapshotIteration
            if let cachedIteration, cachedIteration.id == manifest.currentIteration {
                iteration = cachedIteration
            } else if let iterationID = manifest.currentIteration {
                iteration = try self.loadIteration(for: iterationID)
            } else {
                let date = Date()
                iteration = SnapshotIteration(id: SnapshotIterationIdentifier(date: date), creationDate: date)
            }

            /// Let the accessor do something with the manifest, storing the variable on the Task Local stack.
            return try await SnapshotTaskLocals.$manifest.withValue((manifest, iteration)) {
                try await accessor(manifest, iteration)
            }
        }
        /// Assign the task to our pointer so we can depend on it the next time. Also, re-wrap it so we can keep proper type information when returning from this method.
        lastUpdateManifestTask = Task { try await readerTask.value }

        return readerTask
    }

    /// Load and update the manifest in an updater.
    ///
    /// This method loads the ``SnapshotManifest`` from cache, offers it to be mutated, then writes it back to disk, if it changed. It is up to the caller to update the modification date of the store.
    ///
    /// - Note: Calling this method when no manifest exists on disk will create it, even if no changes occur in the block.
    /// - Parameter updater: An updater that takes a mutable reference to a manifest, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `updater`.
    func updatingManifest<T: Sendable>(
        updater: @Sendable (_ manifest: inout SnapshotManifest, _ iteration: inout SnapshotIteration) async throws -> T
    ) async throws -> T where AccessMode == ReadWrite {
        try await withoutActuallyEscaping(updater) { escapingClosure in
            try await updateManifest(updater: escapingClosure).value
        }
    }

    /// Load the manifest in an updater.
    ///
    /// This method loads the ``SnapshotManifest`` from cache.
    ///
    /// - Parameter accessor: An accessor that takes an immutable reference to a manifest, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `accessor`.
    @_disfavoredOverload
    func readingManifest<T: Sendable>(
        accessor: @Sendable (_ manifest: SnapshotManifest, _ iteration: SnapshotIteration) async throws -> T
    ) async throws -> T {
        try await withoutActuallyEscaping(accessor) { escapingClosure in
            try await readManifest(accessor: escapingClosure).value
        }
    }
}

private enum SnapshotTaskLocals {
    @TaskLocal
    static var manifest: (SnapshotManifest, SnapshotIteration)?
}

// MARK: - Datastore Management
extension Snapshot {
    /// Load the datastore for the given key.
    func loadDatastore(for key: DatastoreKey, from iteration: SnapshotIteration) -> (DiskPersistence<AccessMode>.Datastore, DatastoreRootIdentifier?) {
        let datastoreInfo: (id: DatastoreIdentifier, root: DatastoreRootIdentifier?) = {
            if let info = iteration.dataStores[key] {
                return (info.id, info.root)
            } else {
                return (DatastoreIdentifier(name: key.rawValue), nil)
            }
        }()
        
        if let datastore = datastores[datastoreInfo.id] {
            return (datastore, datastoreInfo.root)
        }
        
        let datastore = DiskPersistence<AccessMode>.Datastore(id: datastoreInfo.id, snapshot: self)
        datastores[datastoreInfo.id] = datastore
        
        return (datastore, datastoreInfo.root)
    }
}
