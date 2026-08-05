//
//  Snapshot.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-09.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Foundation
import QuestionableConcurrency

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
    /// This is used to determine which parent directory on disk the snapshot is stored.
    let isBackup: Bool
    
    /// A cached instance of the manifest as last loaded from disk.
    var cachedManifest: SnapshotManifest?
    
    /// Cache for the loaded iterations as last loaded from disk. ``isExtendedIterationCacheEnabled`` controls if multiple iterations are cached or not.
    var cachedIterations: [SnapshotIterationIdentifier : SnapshotIteration] = [:]
    var isExtendedIterationCacheEnabled: Bool
    
    /// A transaction stream for manifest updates, so reads and writes can be serialized in request order.
    var manifestTransactionStream = TransactionStream()
    
    /// The loaded datastores.
    var datastores: [DatastoreIdentifier: DiskPersistence<AccessMode>.Datastore] = [:]
    
    /// The chain of iterations
    var iterationChain: SparseIterationChain
    var iterationChainState: IterationChainState
    
//    private let (pruningTasksStream, pruningTasksStreamProvider) = AsyncStream.makeStream(of: Task<Void, Error>.self)
//    private var pruningTasksIterator: AsyncStream<Task<Void, any Error>>.AsyncIterator
    private var pruningWatermark = 0
    private var lastPruningTask: Task<Void, Error>?
    
    init(
        id: SnapshotIdentifier,
        persistence: DiskPersistence<AccessMode>,
        isBackup: Bool = false,
        isExtendedIterationCacheEnabled: Bool = false
    ) {
        self.id = id
        self.persistence = persistence
        self.isBackup = isBackup
        self.isExtendedIterationCacheEnabled = isExtendedIterationCacheEnabled
        
        self.iterationChain = SparseIterationChain()
        self.iterationChainState = .forwardEditsOnly
//        prunedIterationIterator = prunedIterationStream.makeAsyncIterator()
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
    
    /// The URL for a specific datastore within the snapshot.
    nonisolated func datastoreURL(for id: DatastoreIdentifier) -> URL {
        datastoresURL.appendingPathComponent("\(id).datastore", isDirectory: true)
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
    
    func setExtendedIterationCacheEnabled(_ isEnabled: Bool) async {
        isExtendedIterationCacheEnabled = isEnabled
        
        await invalidateIterationChainState()
    }
    
    private func invalidateIterationChainState() async {
        switch iterationChainState {
        case .forwardEditsOnly:
            /// If we are currently only collecting forward edits, and the extended cache was just enabled, start the crawling process.
            if isExtendedIterationCacheEnabled {
                iterationChainState = .crawling(Task {
                    do {
                        try await crawlIterations()
                        iterationChainState = .complete
                    } catch {
                        print("Error crawling iterations: \(error)")
                        iterationChainState = .forwardEditsOnly
                    }
                })
            }
        case .crawling(let task):
            /// If we are currently crawling, but the extended cache was just disabled, cancel the crawling process and swap back to the incomplete state. Everything we have should still be valid.
            if !isExtendedIterationCacheEnabled {
                /// The state is managed by the task, and doesn't need to be set here, so long as we wait for it to complete up to the cancellation point.
                task.cancel()
                await task.value
            }
        case .complete:
            break
        }
    }
    
    func crawlIterations() async throws {
        var currentIteration: SnapshotIteration
        if let currentIterationID = iterationChain.last?.iteration {
            currentIteration = try self.loadIterationNoCache(for: currentIterationID)
        } else {
            // TODO: We need to do this earlier, otheriwse we may end up duplicating an entry or skipping others, as the cached manifest may have changed since the task was initially started
            /// Load the manifest so we have a fresh copy, unless we have a cached copy already.
            var manifest = try cachedManifest ?? self.loadManifest()
            
            /// If there is no ID here, we are basically done, as we have nothing to crawl
            guard let currentIterationID = manifest.currentIteration
            else { return }
            
            /// Make sure not to await adding this first entry?
            currentIteration = try self.loadIterationNoCache(for: currentIterationID)
            iterationChain.append(iteration: currentIteration)
        }
        
        /// Walk the preceding iteration chain to the oldest iteration we can open, collecting the ones that should be pruned.
        while let precedingIterationID = currentIteration.precedingIteration, let precedingIteration = try? await loadIteration(for: precedingIterationID) {
            try Task.checkCancellation()
            
            if !iterations.isEmpty || transactionRetentionPolicy.shouldIterationBePruned(creationDate: precedingIteration.creationDate, distance: distance) {
                iterations.append(precedingIteration.id)
            } else {
                mainlineSuccessorIteration = precedingIteration
            }
            currentIteration = precedingIteration
            
            distance += 1
            
            if distance % 5000 == 0 {
                print("Found \(iterations.count) iterations to prune. Keeping \(distance - iterations.count) iterations.")
            }
            
//                    await Task.yield()
        }
    }
    
    /// Load an iteration from disk, or create a suitable starting value if such a file does not exist.
    func loadIterationNoCache(for iterationID: SnapshotIterationIdentifier) throws -> SnapshotIteration {
        do {
            let data = try Data(contentsOf: iterationURL(for: iterationID))
            
            let iteration = try JSONDecoder.shared.decode(SnapshotIteration.self, from: data)
            
            if !isExtendedIterationCacheEnabled {
                cachedIterations.removeAll()
            }
            /// Make sure not to grow the cache unecessarily
            if cachedIterations.count >= 256, let firstKey = cachedIterations.keys.first {
                cachedIterations.removeValue(forKey: firstKey)
            }
            cachedIterations[iteration.id] = iteration
            return iteration
        } catch {
            throw error
        }
    }
    
    /// Load an iteration from disk, or create a suitable starting value if such a file does not exist.
    func loadIteration(for iterationID: SnapshotIterationIdentifier?) async throws -> SnapshotIteration? {
        guard let iterationID else { return nil }
        if let iteration = cachedIterations[iterationID] {
            return iteration
        }
        return try loadIterationNoCache(for: iterationID)
    }
    
    func pruneIteration(_ iteration: SnapshotIteration, mode: SnapshotPruneMode, shouldDelete: Bool) async throws {
        let pruneTask = Task {
            try await pruneIteration(iteration, mode: mode)
            return iteration
        }
        lastPruningTask = Task { [lastPruningTask] in
            try await lastPruningTask?.value
            let iteration = try await pruneTask.value
            if shouldDelete {
                deleteIteration(iteration)
            }
        }
        pruningWatermark += 1
        
        /// If we've enqueued at least 64 tasks, pause before returning control so we can drain the pool, checking for cancellation in the process.
        if pruningWatermark >= 64 {
            try Task.checkCancellation()
            pruningWatermark = 0
            try await lastPruningTask?.value
            await Task.yield()
        }
    }
    
    func drainPrunedIterations() async throws {
        pruningWatermark = 0
        try await lastPruningTask?.value
    }
    
    private func pruneIteration(_ iteration: SnapshotIteration, mode: SnapshotPruneMode) async throws {
        /// Collect the datastores and related roots we'll be deleting.
        /// - For datastores, only collect the ones we'll be deleting since the ones we are keeping won't be making references to other deletable assets.
        /// - For the datastore roots, we'll be deleting the entries that are being removed (relative to the direction we are removing from, so the removed ones from the oldest edge, and the added ones from the newest edge, as determined by the caller), while we'll be checking for more assets to remove from entries that have just been added, but only when removing from the oldest edge. We only do this for the oldest edge because entries that have been "removed" from the newest edge are actually being _restored_ and not replaced, which maintains symmetry in a non-obvious way.
        let datastoresToPruneAndDelete = iteration.datastoresToPrune(for: mode)
        var datastoreRootsToPruneAndDelete = iteration.datastoreRootsToPrune(for: mode, options: .pruneAndDelete)
        var datastoreRootsToPrune = iteration.datastoreRootsToPrune(for: mode, options: .pruneOnly)
        
        /// Start by deleting and pruning roots as needed. We attempt to do this twice, as older versions of the persistence (prior to 0.4) didn't record the datastore ID along with the root id, which would therefor require extra work.
        /// First, delete the root entries we know to be removed.
        for datastoreRoot in datastoreRootsToPruneAndDelete {
            guard let datastoreID = datastoreRoot.datastoreID else { continue }
            let datastore = datastores[datastoreID] ?? DiskPersistence<AccessMode>.Datastore(id: datastoreID, snapshot: self)
            do {
                try await datastore.pruneRootObject(with: datastoreRoot.datastoreRootID, mode: mode, shouldDelete: true)
            } catch URLError.fileDoesNotExist, CocoaError.fileReadNoSuchFile, CocoaError.fileNoSuchFile, POSIXError.ENOENT {
                /// This datastore root is already gone.
            } catch {
                print("Could not delete datastore root \(datastoreRoot): \(error)")
                throw error
            }
            datastoreRootsToPruneAndDelete.remove(datastoreRoot)
        }
        /// Prune the root entries that were just added, as they themselves refer to other deleted assets.
        for datastoreRoot in datastoreRootsToPrune {
            guard let datastoreID = datastoreRoot.datastoreID else { continue }
            let datastore = datastores[datastoreID] ?? DiskPersistence<AccessMode>.Datastore(id: datastoreID, snapshot: self)
            do {
                try await datastore.pruneRootObject(with: datastoreRoot.datastoreRootID, mode: mode, shouldDelete: false)
            } catch URLError.fileDoesNotExist, CocoaError.fileReadNoSuchFile, CocoaError.fileNoSuchFile, POSIXError.ENOENT {
                /// This datastore root is already gone.
            } catch {
                print("Could not prune datastore root \(datastoreRoot): \(error)")
                throw error
            }
            datastoreRootsToPrune.remove(datastoreRoot)
        }
        /// If any regerences remain, funnel into this code path for very old persistences.
        if !datastoreRootsToPruneAndDelete.isEmpty || !datastoreRootsToPrune.isEmpty {
            for (_, datastoreInfo) in iteration.dataStores {
                /// Skip any roots for datastores being deleted, since we'll just unlink the whole directory in that case.
                guard !datastoresToPruneAndDelete.contains(datastoreInfo.id) else { continue }
                
                let datastore = datastores[datastoreInfo.id] ?? DiskPersistence<AccessMode>.Datastore(id: datastoreInfo.id, snapshot: self)
                
                /// Delete the root entries we know to be removed.
                for datastoreRoot in datastoreRootsToPruneAndDelete {
                    do {
                        try await datastore.pruneRootObject(with: datastoreRoot.datastoreRootID, mode: mode, shouldDelete: true)
                        datastoreRootsToPruneAndDelete.remove(datastoreRoot)
                    } catch URLError.fileDoesNotExist, CocoaError.fileReadNoSuchFile, CocoaError.fileNoSuchFile, POSIXError.ENOENT {
                        /// This datastore did not contain the specified root, skip it for now.
                    } catch {
                        print("Could not delete datastore root \(datastoreRoot): \(error).")
                        throw error
                    }
                }
                
                /// Prune the root entries that were just added, as they themselves refer to other deleted assets.
                for datastoreRoot in datastoreRootsToPrune {
                    do {
                        try await datastore.pruneRootObject(with: datastoreRoot.datastoreRootID, mode: mode, shouldDelete: false)
                        datastoreRootsToPrune.remove(datastoreRoot)
                    } catch URLError.fileDoesNotExist, CocoaError.fileReadNoSuchFile, CocoaError.fileNoSuchFile, POSIXError.ENOENT {
                        /// This datastore did not contain the specified root, skip it for now.
                    } catch {
                        print("Could not prune datastore root \(datastoreRoot): \(error).")
                        throw error
                    }
                }
            }
        }
        
        /// Delete any datastores in their entirety.
        for datastoreID in datastoresToPruneAndDelete {
            try? FileManager.default.removeItem(at: datastoreURL(for: datastoreID))
        }
    }
    
    /// Delete the iteration. Note that an iteration should be pruned first to delete related files that are specific to the iteration itself.
    private func deleteIteration(_ iteration: SnapshotIteration) {
        cachedIterations.removeValue(forKey: iteration.id)
        
        let iterationURL = iterationURL(for: iteration.id)
        try? FileManager.default.removeItem(at: iterationURL)
        try? FileManager.default.removeDirectoryIfEmpty(url: iterationURL.deletingLastPathComponent(), recursivelyRemoveParents: true)
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
    
    /// Write the specified iteration to the store, and cache the results in ``Snapshot/cachedIterations``.
    private func write(iteration: SnapshotIteration) throws where AccessMode == ReadWrite {
        let iterationURL = iterationURL(for: iteration.id)
        /// Make sure the directories exists first.
        try FileManager.default.createDirectory(at: iterationURL.deletingLastPathComponent(), withIntermediateDirectories: true)

        /// Encode the provided iteration, and write it to disk.
        let data = try JSONEncoder.shared.encode(iteration)
        try data.write(to: iterationURL, options: .atomic)

        /// Update the cache since we know what it should be.
        if !isExtendedIterationCacheEnabled {
            cachedIterations.removeAll()
        }
        cachedIterations[iteration.id] = iteration
    }

    /// Load and update the manifest in an updater.
    ///
    /// This method loads the ``SnapshotManifest`` from cache, offers it to be mutated, then writes it back to disk, if it changed. It is up to the caller to update the modification date of the store.
    ///
    /// - Note: Calling this method when no manifest exists on disk will create it, even if no changes occur in the block.
    /// - Parameter updater: An updater that takes a mutable reference to a manifest, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `updater`.
    func updatingManifest<T: Sendable>(
        updater: (_ manifest: inout SnapshotManifest, _ iteration: inout SnapshotIteration) async throws -> T
    ) async throws -> T where AccessMode == ReadWrite {
        if let (manifest, iteration) = SnapshotTaskLocals.manifest(for: persistence) {
            var updatedManifest = manifest
            var updatedIteration = iteration
            let returnValue = try await updater(&updatedManifest, &updatedIteration)
            
            guard updatedManifest == manifest, updatedIteration == iteration
            else { throw DiskPersistenceInternalError.nestedSnapshotWrite }
            
            return returnValue
        }
        
        return try await manifestTransactionStream.withTransaction {
            /// Load the manifest so we have a fresh copy, unless we have a cached copy already.
            var manifest = try cachedManifest ?? self.loadManifest()
            let precedingIteration = try await self.loadIteration(for: manifest.currentIteration)
            var iteration = precedingIteration ?? SnapshotIteration()

            /// Let the updater do something with the manifest, storing the variable on the Task Local stack.
            let returnValue = try await SnapshotTaskLocals.with(manifest: manifest, iteration: iteration, for: persistence) {
                try await updater(&manifest, &iteration)
            }
            
            /// Only write to the store if we changed the manifest for any reason
            if iteration.isMeaningfullyChanged(from: precedingIteration) {
                iteration.creationDate = Date()
                iteration.id = SnapshotIterationIdentifier(date: iteration.creationDate)
                iteration.precedingIteration = precedingIteration?.id
                
                try write(iteration: iteration)
            }
            
            manifest.currentIteration = iteration.id
            
            /// Only write to the store if we changed the manifest for any reason
            if manifest != cachedManifest {
                try write(manifest: manifest)
            }
            
            /// Add the latest iteration to the chain now that it's been written to disk for this snapshot.
            iterationChain.prepend(iteration: iteration)
            // TODO: Update the target pruning snapshotID
            return returnValue
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
        accessor: (_ manifest: SnapshotManifest, _ iteration: SnapshotIteration) async throws -> T
    ) async throws -> T {
        if let (manifest, iteration) = SnapshotTaskLocals.manifest(for: persistence) {
            return try await accessor(manifest, iteration)
        }
        
        return try await manifestTransactionStream.withTransaction {
            /// Load the manifest so we have a fresh copy, unless we have a cached copy already.
            let manifest = try cachedManifest ?? self.loadManifest()
            let iteration = try await self.loadIteration(for: manifest.currentIteration) ?? SnapshotIteration()

            /// Let the accessor do something with the manifest, storing the variable on the Task Local stack.
            return try await SnapshotTaskLocals.with(manifest: manifest, iteration: iteration, for: persistence) {
                try await accessor(manifest, iteration)
            }
        }
    }
}

private enum SnapshotTaskLocals {
    @TaskLocal
    static var manifestStorage: [ObjectIdentifier : (SnapshotManifest, SnapshotIteration)] = [:]
    
    static func manifest<AccessMode: _AccessMode>(for persistence: DiskPersistence<AccessMode>) -> (SnapshotManifest, SnapshotIteration)? {
        manifestStorage[ObjectIdentifier(persistence)]
    }
    
    static func with<AccessMode: _AccessMode, R>(
        isolation actor: isolated (any Actor)? = #isolation,
        manifest: SnapshotManifest,
        iteration: SnapshotIteration,
        for persistence: DiskPersistence<AccessMode>,
        operation: () async throws -> R
    ) async rethrows -> R {
        var currentStorage = manifestStorage
        currentStorage[ObjectIdentifier(persistence)] = (manifest, iteration)
        
        return try await $manifestStorage.withValue(currentStorage, operation: operation)
    }
}

enum SnapshotPruneMode {
    case pruneRemoved
    case pruneAdded
}

enum SnapshotPruneOptions {
    case pruneAndDelete
    case pruneOnly
}

// MARK: - Datastore Management
extension Snapshot {
    /// Load the datastore for the given key.
    func loadDatastore(
        for key: DatastoreKey,
        from iteration: SnapshotIteration
    ) -> (DiskPersistence<AccessMode>.Datastore, DatastoreRootIdentifier?) {
        let datastoreInfo = if let info = iteration.dataStores[key] {
            (id: info.id, root: info.root)
        } else {
            (id: DatastoreIdentifier(name: key.rawValue), root: DatastoreRootIdentifier?.none)
        }
        
        if let datastore = datastores[datastoreInfo.id] {
            return (datastore, datastoreInfo.root)
        }
        
        let datastore = DiskPersistence<AccessMode>.Datastore(id: datastoreInfo.id, snapshot: self)
        datastores[datastoreInfo.id] = datastore
        
        return (datastore, datastoreInfo.root)
    }
}

// MARK: - Snapshotting

extension Snapshot {
    @discardableResult
    func copy(
        into persistence: DiskPersistence<ReadWrite>,
        actionName: String? = nil,
        newSnapshotIdentifier: SnapshotIdentifier? = nil,
        targetPageSize: Int
    ) async throws -> Snapshot<ReadWrite> {
        try await readingManifest { manifest, iteration in
            /// Create a new snapshot and iteration to load data into
            let newSnapshot = Snapshot<ReadWrite>(id: newSnapshotIdentifier ?? SnapshotIdentifier(), persistence: persistence)
            
            let creationDate = (try? newSnapshot.id.components)?.date ?? Date()
            var newIteration = SnapshotIteration(
                id: SnapshotIterationIdentifier(rawValue: newSnapshot.id.rawValue),
                creationDate: creationDate,
                precedingIteration: iteration.id,
                precedingSnapshot: id,
                successiveIterations: [],
                actionName: actionName,
                dataStores: [:],
                addedDatastores: [],
                removedDatastores: [],
                addedDatastoreRoots: [],
                removedDatastoreRoots: []
            )
            
            /// Iterate through each datastore and copy the data over
            for (_, datastoreInfo) in iteration.dataStores {
                let (datastore, _) = loadDatastore(for: datastoreInfo.key, from: iteration)
                try await datastore.copy(
                    rootIdentifier: datastoreInfo.root,
                    datastoreKey: datastoreInfo.key,
                    into: newSnapshot,
                    iteration: &newIteration,
                    targetPageSize: targetPageSize
                )
            }
            
            /// Create a new manifest with our written data.
            let newManifest = SnapshotManifest(
                id: newSnapshot.id,
                modificationDate: creationDate,
                currentIteration: newIteration.id
            )
            
            /// Write the iteration and manifest records so the persistence is complete.
            try await newSnapshot.write(iteration: newIteration)
            try await newSnapshot.write(manifest: newManifest)
            return newSnapshot
        }
    }
}

struct SparseIterationChain {
    var count: Int
    var groups: [Group]
    
    init() {
        self.count = 0
        self.groups = []
    }
    
    struct Group {
        var first: (iteration: SnapshotIteration.ID, creationDate: Date)
        var last: (iteration: SnapshotIteration.ID, creationDate: Date)
        var contents: [(iteration: SnapshotIteration.ID, creationDate: Date)]?
        var count: Int
        
        init(iteration: SnapshotIteration) {
            first = (iteration.id, iteration.creationDate)
            last = (iteration.id, iteration.creationDate)
            contents = [(iteration.id, iteration.creationDate)]
            count = 1
        }
        
        mutating func prepend(iteration: SnapshotIteration) {
            if contents != nil {
                contents?.insert((iteration.id, iteration.creationDate), at: 0)
            }
            first = (iteration.id, iteration.creationDate)
            count += 1
        }
        
        mutating func append(iteration: SnapshotIteration) {
            if contents != nil {
                contents?.append((iteration.id, iteration.creationDate))
            }
            last = (iteration.id, iteration.creationDate)
            count += 1
        }
    }
    
    mutating func prepend(iteration: SnapshotIteration) {
        count += 1
        if !groups.isEmpty {
            /// If the group happens to have room, prepend to it and return.
            guard groups[0].count >= chunkSize else {
                groups[0].prepend(iteration: iteration)
                return
            }
            /// Otherwise, empty out the subsequent group, and flow through to create a new one
            groups[0].contents = nil
        }
        groups.insert(Group(iteration: iteration), at: 0)
    }
    
    mutating func append(iteration: SnapshotIteration) {
        count += 1
        if !groups.isEmpty {
            /// If the group happens to have room, append to it and return.
            guard groups[groups.count-1].count >= chunkSize else {
                groups[groups.count-1].append(iteration: iteration)
                return
            }
            /// Otherwise, empty out the previous group, and flow through to create a new one
            groups[groups.count-1].contents = nil
        }
        groups.append(Group(iteration: iteration))
    }
    
    mutating func removeIterations(failing snapshotRetentionPolicy: SnapshotRetentionPolicy) -> [Group] {
        var groupsToRemove: [Group] = []
        
        return groupsToRemove
    }
    
    var first: (iteration: SnapshotIteration.ID, creationDate: Date)? {
        groups.first?.first
    }
    
    var last: (iteration: SnapshotIteration.ID, creationDate: Date)? {
        groups.last?.last
    }
    
    var chunkSize: Int {
        1 << max(Int.bitWidth - 1 - count.leadingZeroBitCount - 8, 8)
    }
}

enum IterationChainState {
    case forwardEditsOnly
    case crawling(Task<Void, Never>)
    case complete
}


/*
 
 - enforceRetentionPolicy enabled
   1. iteration chain created
   2. each new iteration added to the front of it
   3. iterations are crawled adding to the back of it
   4. only the first and last chunks are hydrated
   5. iteration chain owned by an actor so access is safe
   6. separate flag controls if iterations have been fully crawled
   7. if iterations are still being crawled, don't do anything
   8. if iterations completed crawling, scan for first one to delete
   9. delete from oldest to first item
   10. if final group is sparse, re-hydrate it
   11. if deleting, set new final iteration
   12. if not deleting
 
 */
