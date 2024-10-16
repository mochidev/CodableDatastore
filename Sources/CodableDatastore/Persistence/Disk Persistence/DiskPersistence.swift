//
//  DiskPersistence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

#if canImport(Darwin)
import Foundation
#else
@preconcurrency import Foundation
#endif

public actor DiskPersistence<AccessMode: _AccessMode>: Persistence {
    /// The location of this persistence.
    let storeURL: URL
    
    /// A cached instance of the store info as last loaded from disk.
    var cachedStoreInfo: StoreInfo?
    
    /// A pointer to the last store info updater, so updates can be serialized after the last request
    var lastUpdateStoreInfoTask: Task<Sendable, Error>?
    
    /// The loaded Snapshots
    var snapshots: [SnapshotIdentifier: Snapshot<AccessMode>] = [:]
    
    var registeredDatastores: [DatastoreKey : [WeakDatastore]] = [:]
    
    var lastTransaction: Transaction?
    
    var _transactionRetentionPolicy: SnapshotRetentionPolicy = .indefinite
    
    var nextSnapshotIterationCandidateToEnforce: (snapshot: Snapshot<ReadWrite>, iteration: SnapshotIteration)?
    var snapshotIterationPruningTask: Task<Void, Never>?
    
    /// Shared caches across all snapshots and datastores.
    var rollingRootObjectCacheIndex = 0
    var rollingRootObjectCache: [Datastore.RootObject] = []
    
    var rollingIndexCacheIndex = 0
    var rollingIndexCache: [Datastore.Index] = []
    
    var rollingPageCacheIndex = 0
    var rollingPageCache: [Datastore.Page] = []
    
    /// Initialize a ``DiskPersistence`` with a read-write URL.
    ///
    /// Use this initializer when creating a persistence from the main process that will access it, such as your app. To access the same persistence from another process, use ``init(readOnlyURL:)`` instead.
    ///
    /// - Throws: The URL must be a file URL. If it isn't, a ``DiskPersistenceError/notFileURL`` error will be thrown.
    public init(readWriteURL: URL) throws where AccessMode == ReadWrite {
        guard readWriteURL.isFileURL else {
            throw DiskPersistenceError.notFileURL
        }
        
        storeURL = readWriteURL
    }
    
    /// Initialize a ``DiskPersistence`` with a read-only URL.
    ///
    /// Use this initializer when you want to access a persistence that is owned by another primary process, which is commonly the case with extensions of apps. This gives you a safe read-only view of the persistence store with no risk of losing data should the main app be active at the same time.
    public init(readOnlyURL: URL) where AccessMode == ReadOnly {
        storeURL = readOnlyURL
    }
    
    deinit {
        snapshotIterationPruningTask?.cancel()
    }
    
    /// The default URL to use for disk persistences.
    static var defaultURL: URL {
        // TODO: Make non-throwing: https://github.com/mochidev/CodableDatastore/issues/15
        get throws {
            guard let appName = Bundle.main.bundleIdentifier else {
                throw DiskPersistenceError.missingBundleID
            }
            
            let persistenceName = "DefaultStore.persistencestore"
            
#if !canImport(Darwin)
            guard let applicationSupportDirectory = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else {
                throw DiskPersistenceError.missingAppSupportDirectory
            }
            return applicationSupportDirectory
                .appendingPathComponent(appName, isDirectory: true)
                .appendingPathComponent(persistenceName, isDirectory: true)
#else
            if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9, *) {
                return URL
                    .applicationSupportDirectory
                    .appending(component: appName, directoryHint: .isDirectory)
                    .appending(component: persistenceName, directoryHint: .isDirectory)
            } else {
                guard let applicationSupportDirectory = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else {
                    throw DiskPersistenceError.missingAppSupportDirectory
                }
                return applicationSupportDirectory
                    .appendingPathComponent(appName, isDirectory: true)
                    .appendingPathComponent(persistenceName, isDirectory: true)
            }
#endif
        }
    }
}

// MARK: - Default Store

extension DiskPersistence where AccessMode == ReadWrite {
    /// The default persistence for the read-write store of an app.
    public static func defaultStore() throws -> DiskPersistence<AccessMode> {
        // TODO: Make this a static property: https://github.com/mochidev/CodableDatastore/issues/17
        let url = try defaultURL
        
        // This is safe since default URL is always a file URL.
        return try! DiskPersistence(readWriteURL: url)
    }
}

extension DiskPersistence where AccessMode == ReadOnly {
    /// The default persistence for the read-only store of an app.
    public static func readOnlyDefaultStore() throws -> DiskPersistence<AccessMode> {
        // TODO: Make this a static property: https://github.com/mochidev/CodableDatastore/issues/17
        return try DiskPersistence(readOnlyURL: defaultURL)
    }
}

// MARK: - Common URL Accessors

extension DiskPersistence {
    /// The URL that points to the Snapshots directory.
    nonisolated var snapshotsURL: URL {
        storeURL.appendingPathComponent("Snapshots", isDirectory: true)
    }
    
    /// The URL that points to the Backups directory.
    nonisolated var backupsURL: URL {
        storeURL.appendingPathComponent("Backups", isDirectory: true)
    }
    
    /// The URL that points to the Info.json file.
    nonisolated var storeInfoURL: URL {
        storeURL.appendingPathComponent("Info.json", isDirectory: false)
    }
}

// MARK: - Store Info

extension DiskPersistence {
    /// Load the store info from disk, or create a suitable starting value if such a file does not exist.
    private func loadStoreInfo() throws -> StoreInfo {
        do {
            let data = try Data(contentsOf: storeInfoURL)
            
            let storeInfo = try JSONDecoder.shared.decode(StoreInfo.self, from: data)
            
            cachedStoreInfo = storeInfo
            return storeInfo
        } catch URLError.fileDoesNotExist, CocoaError.fileNoSuchFile, CocoaError.fileReadNoSuchFile, POSIXError.ENOENT {
            return StoreInfo(modificationDate: Date())
        } catch {
            throw error
        }
    }
    
    /// Write the specified store info to the store, and cache the results in ``DiskPersistence/cachedStoreInfo``.
    private func write(storeInfo: StoreInfo) throws where AccessMode == ReadWrite {
        /// Make sure the directory exists first.
        try createPersistenceDirectories()
        
        /// Encode the provided store info, and write it to disk.
        let data = try JSONEncoder.shared.encode(storeInfo)
        try data.write(to: storeInfoURL, options: .atomic)
        
        /// Update the cache since we know what it should be.
        cachedStoreInfo = storeInfo
    }
    
    /// Load and update the store info in an updater, returning the task for the updater.
    ///
    /// This method loads the ``StoreInfo`` from cache, offers it to be mutated, then writes it back to disk, if it changed. It is up to the caller to update the modification date of the store.
    ///
    /// - Note: Calling this method when no store info exists on disk will create it, even if no changes occur in the block.
    /// - Parameter updater: An updater that takes a mutable reference to a store info, and will forward the returned value to the caller.
    /// - Returns: A ``/Swift/Task`` which contains the value of the updater upon completion.
    func updateStoreInfo<T: Sendable>(
        @_inheritActorContext updater: @Sendable @escaping (_ storeInfo: inout StoreInfo) async throws -> T
    ) -> Task<T, Error> where AccessMode == ReadWrite {
        if let storeInfo = DiskPersistenceTaskLocals.storeInfo {
            return Task {
                var updatedStoreInfo = storeInfo
                let returnValue = try await updater(&updatedStoreInfo)
                
                guard updatedStoreInfo == storeInfo else {
                    throw DiskPersistenceInternalError.nestedStoreWrite
                }
                
                return returnValue
            }
        }
        
        /// Grab the last task so we can chain off of it in a serial manner.
        let lastUpdaterTask = lastUpdateStoreInfoTask
        let updaterTask = Task {
            /// We don't care if the last request throws an error or not, but we do want it to complete first.
            _ = try? await lastUpdaterTask?.value
            
            /// Load the store info so we have a fresh copy, unless we have a cached copy already.
            var storeInfo = try cachedStoreInfo ?? self.loadStoreInfo()
            
            /// Let the updater do something with the store info, storing the variable on the Task Local stack.
            let returnValue = try await DiskPersistenceTaskLocals.$storeInfo.withValue(storeInfo) {
                try await updater(&storeInfo)
            }
            
            /// Only write to the store if we changed the store info for any reason
            if storeInfo != cachedStoreInfo {
                try write(storeInfo: storeInfo)
            }
            return returnValue
        }
        /// Assign the task to our pointer so we can depend on it the next time. Also, re-wrap it so we can keep proper type information when returning from this method.
        lastUpdateStoreInfoTask = Task { try await updaterTask.value }
        
        return updaterTask
    }
    
    /// Load the store info in an accessor, returning the task for the updater.
    ///
    /// This method loads the ``StoreInfo`` from cache.
    ///
    /// - Parameter accessor: An accessor that takes an immutable reference to a store info, and will forward the returned value to the caller.
    /// - Returns: A ``/Swift/Task`` which contains the value of the updater upon completion.
    func updateStoreInfo<T: Sendable>(
        @_inheritActorContext accessor: @Sendable @escaping (_ storeInfo: StoreInfo) async throws -> T
    ) -> Task<T, Error> {
        if let storeInfo = DiskPersistenceTaskLocals.storeInfo {
            return Task { try await accessor(storeInfo) }
        }
        
        /// Grab the last task so we can chain off of it in a serial manner.
        let lastUpdaterTask = lastUpdateStoreInfoTask
        let updaterTask = Task {
            /// We don't care if the last request throws an error or not, but we do want it to complete first.
            _ = try? await lastUpdaterTask?.value
            
            /// Load the store info so we have a fresh copy, unless we have a cached copy already.
            let storeInfo = try cachedStoreInfo ?? self.loadStoreInfo()
            
            /// Let the accessor do something with the store info, storing the variable on the Task Local stack.
            return try await DiskPersistenceTaskLocals.$storeInfo.withValue(storeInfo) {
                try await accessor(storeInfo)
            }
        }
        /// Assign the task to our pointer so we can depend on it the next time. Also, re-wrap it so we can keep proper type information when returning from this method.
        lastUpdateStoreInfoTask = Task { try await updaterTask.value }
        
        return updaterTask
    }
    
    /// Load and update the store info in an updater.
    ///
    /// This method loads the ``StoreInfo`` from cache, offers it to be mutated, then writes it back to disk, if it changed. It is up to the caller to update the modification date of the store.
    ///
    /// - Note: Calling this method when no store info exists on disk will create it, even if no changes occur in the block.
    /// - Parameter updater: An updater that takes a mutable reference to a store info, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `updater`.
    func withStoreInfo<T: Sendable>(
        updater: @Sendable (_ storeInfo: inout StoreInfo) async throws -> T
    ) async throws -> T where AccessMode == ReadWrite {
        try await withoutActuallyEscaping(updater) { escapingClosure in
            try await updateStoreInfo(updater: escapingClosure).value
        }
    }
    
    /// Load the store info in an updater.
    ///
    /// This method loads the ``StoreInfo`` from cache.
    ///
    /// - Parameter accessor: An accessor that takes an immutable reference to a store info, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `accessor`.
    @_disfavoredOverload
    func withStoreInfo<T: Sendable>(
        accessor: @Sendable (_ storeInfo: StoreInfo) async throws -> T
    ) async throws -> T where AccessMode == ReadOnly {
        try await withoutActuallyEscaping(accessor) { escapingClosure in
            try await updateStoreInfo(accessor: escapingClosure).value
        }
    }
}

// MARK: - Snapshot Management

extension DiskPersistence {
    /// Load the default snapshot from disk, or create an empty one if such a file does not exist.
    private func loadSnapshot(from storeInfo: StoreInfo) -> Snapshot<AccessMode> {
        let snapshotID = storeInfo.currentSnapshot ?? SnapshotIdentifier()
        
        if let snapshot = snapshots[snapshotID] {
            return snapshot
        }
        
        let snapshot = Snapshot(id: snapshotID, persistence: self, isExtendedIterationCacheEnabled: !_transactionRetentionPolicy.isIndefinite)
        snapshots[snapshotID] = snapshot
        
        return snapshot
    }
    
    /// Load and update the current snapshot in an updater, returning the task for the updater.
    ///
    /// This method loads the current ``Snapshot`` so it can be updated.
    ///
    /// - Note: Calling this method when no store info exists on disk will create it.
    /// - Parameter dateUpdate: The method to which to update the date of the main store with.
    /// - Parameter updater: An updater that takes a reference to the current ``Snapshot``, and will forward the returned value to the caller.
    /// - Returns: A ``/Swift/Task`` which contains the value of the updater upon completion.
    func updateCurrentSnapshot<T: Sendable>(
        dateUpdate: ModificationUpdate = .updateOnWrite,
        updater: @escaping (_ snapshot: Snapshot<AccessMode>) async throws -> T
    ) -> Task<T, Error> where AccessMode == ReadWrite {
        /// Grab access to the store info to load and update it.
        return updateStoreInfo { storeInfo in
            /// Grab the current snapshot from the store info
            let snapshot = self.loadSnapshot(from: storeInfo)
            
            /// Load a modification date to use
            let modificationDate = dateUpdate.modificationDate(for: storeInfo.modificationDate)
            
            /// Let the updater do what it needs to do with the snapshot
            let returnValue = try await updater(snapshot)
            
            /// Update the store info with snapshot info
            storeInfo.currentSnapshot = snapshot.id
            storeInfo.modificationDate = modificationDate
            
            return returnValue
        }
    }
    
    /// Load the current snapshot in an accessor, returning the task for the accessor.
    ///
    /// - Parameter accessor: An accessor that takes a reference to the current ``Snapshot``, and will forward the returned value to the caller.
    /// - Returns: A ``/Swift/Task`` which contains the value of the updater upon completion.
    func updateCurrentSnapshot<T: Sendable>(
        accessor: @escaping (_ snapshot: Snapshot<AccessMode>) async throws -> T
    ) -> Task<T, Error> {
        /// Grab access to the store info to load and update it.
        return updateStoreInfo { storeInfo in
            /// Grab the current snapshot from the store info
            let snapshot = self.loadSnapshot(from: storeInfo)
            
            /// Let the accessor do what it needs to do with the snapshot
            return try await accessor(snapshot)
        }
    }
    
    /// Load the current snapshot in an updater.
    ///
    /// This method loads the current ``Snapshot`` so it can be updated.
    ///
    /// - Note: Calling this method when no store info exists on disk will create it.
    /// - Parameter dateUpdate: The method to which to update the date of the main store with.
    /// - Parameter updater: An updater that takes a reference to the current ``Snapshot``, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `accessor`. 
    func updatingCurrentSnapshot<T: Sendable>(
        dateUpdate: ModificationUpdate = .updateOnWrite,
        updater: @Sendable (_ snapshot: Snapshot<AccessMode>) async throws -> T
    ) async throws -> T where AccessMode == ReadWrite {
        try await withoutActuallyEscaping(updater) { escapingClosure in
            try await updateCurrentSnapshot(dateUpdate: dateUpdate, updater: escapingClosure).value
        }
    }
    
    /// Load the current snapshot in an accessor.
    ///
    /// This method loads the current ``Snapshot`` so it can be accessed.
    ///
    /// - Note: Calling this method when no store info exists on disk will create it.
    /// - Parameter dateUpdate: The method to which to update the date of the main store with.
    /// - Parameter accessor: An accessor that takes a reference to the current ``Snapshot``, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `accessor`.
    @_disfavoredOverload
    func readingCurrentSnapshot<T: Sendable>(
        accessor: @Sendable (_ snapshot: Snapshot<AccessMode>) async throws -> T
    ) async throws -> T {
        try await withoutActuallyEscaping(accessor) { escapingClosure in
            try await updateCurrentSnapshot(accessor: escapingClosure).value
        }
    }
}

// MARK: - Persistence Creation

extension DiskPersistence where AccessMode == ReadWrite {
    /// Create directories for our persistence.
    private func createPersistenceDirectories() throws {
        /// If we've cached our store info, we must have saved it, along with the rest of the structure.
        guard cachedStoreInfo == nil else { return }
        
        try FileManager.default.createDirectory(at: storeURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: snapshotsURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: backupsURL, withIntermediateDirectories: true)
    }
    
    /// Create the persistence store if necessary.
    ///
    /// It is useful to call this if you wish for stub directories to be created immediately before a data store
    /// is actually written to the disk.
    public func createPersistenceIfNecessary() async throws {
        /// If we've cached our store info, we must have saved it, along with the rest of the structure.
        guard cachedStoreInfo == nil else { return }
        
        /// Create directories for our persistence.
        try createPersistenceDirectories()
        
        /// Load the store info, so we can see if we'll need to write it or not.
        try await withStoreInfo { _ in }
    }
}

// MARK: - Datastore Registration

extension DiskPersistence {
    func register<Format: DatastoreFormat, Access>(
        datastore newDatastore: CodableDatastore.Datastore<Format, Access>
    ) throws {
        guard
            let datastorePersistence = newDatastore.persistence as? DiskPersistence,
            datastorePersistence === self
        else {
            assertionFailure("The datastore has already been registered with another persistence. Make sure to only register a datastore with a single persistence. This will throw an error on release builds.")
            throw DatastoreInterfaceError.multipleRegistrations
        }
        
        var existingDatastores = registeredDatastores[newDatastore.key, default: []].filter(\.isAlive)
        
        for weakDatastore in existingDatastores {
            if weakDatastore.contains(datastore: newDatastore) {
                assertionFailure("The datastore has already been registered with this persistence. Make sure to not call register multiple times per persistence. This will throw an error on release builds.")
                throw DatastoreInterfaceError.alreadyRegistered
            }
            
            if Access.self == ReadWrite.self, weakDatastore.canWrite {
                assertionFailure("An existing datastore that can write to the persistence has already been registered for this key. Only one writer is supported per key. This will throw an error on release builds.")
                throw DatastoreInterfaceError.duplicateWriters
            }
        }
        
        existingDatastores.append(WeakSpecificDatastore(datastore: newDatastore))
        
        registeredDatastores[newDatastore.key] = existingDatastores
    }
    
    func persistenceDatastore(
        for datastoreKey: DatastoreKey
    ) async throws -> (Datastore, DatastoreRootIdentifier?) {
        guard registeredDatastores[datastoreKey] != nil else {
            throw DatastoreInterfaceError.datastoreNotFound
        }
        if let self = self as? DiskPersistence<ReadWrite> {
            let (datastore, rootID) = try await self.updatingCurrentSnapshot { snapshot in
                try await snapshot.updatingManifest { snapshotManifest, currentIteration in
                    let (datastore, root) = await snapshot.loadDatastore(for: datastoreKey, from: currentIteration)
                    currentIteration.dataStores[datastoreKey] = .init(key: datastoreKey, id: datastore.id, root: root)
                    return (datastore, root)
                }
            }
            return (datastore as! DiskPersistence<AccessMode>.Datastore, rootID)
        } else {
            return try await readingCurrentSnapshot { snapshot in
                try await snapshot.readingManifest { snapshotManifest, currentIteration in
                    await snapshot.loadDatastore(for: datastoreKey, from: currentIteration)
                }
            }
        }
    }
}

// MARK: - Transactions

extension DiskPersistence {
    public func _withTransaction<T: Sendable>(
        actionName: String?,
        options: UnsafeTransactionOptions,
        transaction: @Sendable (_ transaction: DatastoreInterfaceProtocol, _ isDurable: Bool) async throws -> T
    ) async throws -> T {
        try await withoutActuallyEscaping(transaction) { escapingTransaction in
            let (transaction, task) = await Transaction.makeTransaction(
                persistence: self,
                lastTransaction: lastTransaction,
                actionName: actionName, options: options
            ) { interface, isDurable in
                try await escapingTransaction(interface, isDurable)
            }
            
            /// Save the last non-concurrent top-level transaction from the list. Note that disk persistence currently does not support concurrent idempotent transactions.
            if !options.contains(.readOnly), transaction.parent == nil {
                lastTransaction = transaction
            }
            
            return try await task.value
        }
    }
    
    func persist(
        actionName: String?,
        roots: [DatastoreKey : Datastore.RootObject],
        addedDatastoreRoots: Set<DatastoreRootReference>,
        removedDatastoreRoots: Set<DatastoreRootReference>
    ) async throws {
        let containsEdits = try await readingCurrentSnapshot { snapshot in
            try await snapshot.readingManifest { manifest, iteration in
                for (key, root) in roots {
                    guard iteration.dataStores[key]?.root == root.id
                    else { return true }
                }
                return false
            }
        }
        
        /// If nothing changed, don't bother writing anything.
        if !containsEdits { return }
        
        /// If we are read-only, make sure no edits have been made
        guard let self = self as? DiskPersistence<ReadWrite>
        else { throw DiskPersistenceError.cannotWrite }
        
        /// If we are read-write, apply the updated root objects to the snapshot.
        let (currentSnapshot, persistedIteration) = try await self.updatingCurrentSnapshot { snapshot in
            try await snapshot.updatingManifest { manifest, iteration in
                iteration.actionName = actionName
                iteration.addedDatastoreRoots = addedDatastoreRoots
                iteration.removedDatastoreRoots = removedDatastoreRoots
                for (key, root) in roots {
                    iteration.dataStores[key] = SnapshotIteration.DatastoreInfo(
                        key: key,
                        id: root.datastore.id,
                        root: root.id
                    )
                }
                return (snapshot, iteration)
            }
        }
        
        enforceRetentionPolicy(snapshot: currentSnapshot, fromIteration: persistedIteration)
    }
}

// MARK: - Retention Policy

extension DiskPersistence where AccessMode == ReadWrite {
    /// The current transaction retention policy for snapshot iterations written to disk.
    public var transactionRetentionPolicy: SnapshotRetentionPolicy {
        get async {
            _transactionRetentionPolicy
        }
    }
    
    /// Update the transaction retention policy for snapshot iterations written to disk.
    ///
    /// - Parameter policy: The new policy to enforce on write.
    ///
    /// - SeeAlso: ``SnapshotRetentionPolicy``.
    public func setTransactionRetentionPolicy(_ policy: SnapshotRetentionPolicy) async {
        _transactionRetentionPolicy = policy
        for (_, snapshot) in snapshots {
            await snapshot.setExtendedIterationCacheEnabled(!_transactionRetentionPolicy.isIndefinite)
        }
    }
    
    /// Enforce the retention policy on the persistence immediately.
    ///
    /// - Note: Transaction retention policies are enforced after ever write transaction, so calling this method directly is often unecessary. However, it can be useful if the user requires disk resources immediately.
    public func enforceRetentionPolicy() async {
        // TODO: Don't create any snapshots if they don't exist yet
        let info = try? await self.readingCurrentSnapshot { snapshot in
            try await snapshot.readingManifest { manifest, iteration in
                (snapshot: snapshot, iteration: iteration)
            }
        }
        
        if let (snapshot, iteration) = info {
            enforceRetentionPolicy(snapshot: snapshot, fromIteration: iteration)
        }
        
        await finishTransactionCleanup()
    }
}

extension DiskPersistence {
    /// Internal method to envorce the retention policy after a transaction is written.
    private func enforceRetentionPolicy(snapshot: Snapshot<ReadWrite>, fromIteration iteration: SnapshotIteration) {
        nextSnapshotIterationCandidateToEnforce = (snapshot, iteration)
        
        if let snapshotIterationPruningTask {
            /// Update the next snapshot iteration we should be checking, and cancel the existing task so we can move on to checking this iteration.
            snapshotIterationPruningTask.cancel()
            return
        }
        
        /// Update the next snapshot iteration we should be checking, and enqueue a task since we know one isn't currently running.
        checkNextSnapshotIterationCandidateForPruning()
    }
    
    /// Private method to check the next candidate for pruning.
    ///
    /// First, this method walks down the linked list defining the iteration chain, from newest to oldest, and collects the iterations that should be pruned. Then, it iterates that list in reverse (from oldest to newest) actually removing the iterations as they are encountered.
    /// - Note: This method should only ever be called when it is known that no `snapshotIterationPruningTask` is ongoing (it is nil), or when one just finishes.
    @discardableResult
    private func checkNextSnapshotIterationCandidateForPruning() -> Task<Void, Never>? {
        let transactionRetentionPolicy = _transactionRetentionPolicy
        let iterationCandidate = nextSnapshotIterationCandidateToEnforce
        
        snapshotIterationPruningTask = nil
        nextSnapshotIterationCandidateToEnforce = nil
        
        guard let (snapshot, iteration) = iterationCandidate, !transactionRetentionPolicy.isIndefinite
        else { return nil }
        
        snapshotIterationPruningTask = Task.detached(priority: .background) {
            await snapshot.setExtendedIterationCacheEnabled(true)
            do {
                var iterations: [SnapshotIteration.ID] = []
                var distance = 1
                var mainlineSuccessorIteration = iteration
                var currentIteration = iteration
                
                /// First, walk the preceding iteration chain to the oldest iteration we can open, collecting the ones that should be pruned.
                while let precedingIterationID = currentIteration.precedingIteration, let precedingIteration = try? await snapshot.loadIteration(for: precedingIterationID) {
                    try Task.checkCancellation()
                    
                    if !iterations.isEmpty || transactionRetentionPolicy.shouldIterationBePruned(iteration: precedingIteration, distance: distance) {
                        iterations.append(precedingIteration.id)
                    } else {
                        mainlineSuccessorIteration = precedingIteration
                    }
                    currentIteration = precedingIteration
                    
                    distance += 1
                    
                    if distance % 1000 == 0 {
                        print("Found \(iterations.count) iterations to prune. Keeping \(distance - iterations.count) iterations.")
                    }
                    
                    await Task.yield()
                }
                
                print("Will prune \(iterations.count) iterations. Keeping \(distance - iterations.count) iterations.")
                
                /// Prune iterations from oldest to newest.
                while let iterationID = iterations.popLast(), let iteration = try await snapshot.loadIteration(for: iterationID) {
                    let index = iterations.count /// The current index, since we just removed the last element
                    let mainlineSuccessorIterationID = index > 0 ? iterations[index-1] : mainlineSuccessorIteration.id
                    
                    var iterationsToPrune: [SnapshotIteration] = []
                    var successorCandidatesToCheck = iteration.successiveIterations
                    successorCandidatesToCheck.removeAll { $0 == mainlineSuccessorIterationID }
                    
                    /// Walk the successor candidates all the way back up so newer iterations are pruned before the ones that reference them. We pull items off from the end, and add new ones to the beginning to make sure they stay in graph order.
                    while let successorCandidateID = successorCandidatesToCheck.popLast() {
                        try Task.checkCancellation()
                        guard let successorIteration = try? await snapshot.loadIteration(for: successorCandidateID)
                        else { continue }
                        
                        iterationsToPrune.append(successorIteration)
                        successorCandidatesToCheck.insert(contentsOf: successorIteration.successiveIterations, at: 0)
                        await Task.yield()
                    }
                    
                    /// First, remove the branch of iterations based on the one we are removing, but representing a history that was previously reverted.
                    /// Prune the iterations in atomic tasks so they don't get cancelled mid-way, and instead check for cancellation in between iterations.
                    while let iteration = iterationsToPrune.popLast() {
                        try await snapshot.pruneIteration(iteration, mode: .pruneAdded, shouldDelete: true)
                    }
                    
                    /// Finally, prune the iteration itself.
                    try await snapshot.pruneIteration(iteration, mode: .pruneRemoved, shouldDelete: true)
                }
                
                try await snapshot.pruneIteration(mainlineSuccessorIteration, mode: .pruneRemoved, shouldDelete: false)
                try await snapshot.drainPrunedIterations()
            } catch {
                try? await snapshot.drainPrunedIterations()
                print("Pruning stopped: \(error)")
            }
            
            await self.checkNextSnapshotIterationCandidateForPruning()?.value
        }
        
        return snapshotIterationPruningTask
    }
    
    /// Await any cleanup since the last complete write transaction to the persistence.
    ///
    /// - Note: An application is not required to await cleanup, as it'll be eventually completed on future runs. It is however useful in cases when disk resources must be cleared before progressing to another step.
    public func finishTransactionCleanup() async {
        await snapshotIterationPruningTask?.value
    }
}

// MARK: - Persistence-wide Caches

extension DiskPersistence {
    func cache(_ rootObject: Datastore.RootObject) {
        if rollingRootObjectCache.count <= rollingRootObjectCacheIndex {
            rollingRootObjectCache.append(rootObject)
        } else {
            rollingRootObjectCache[rollingRootObjectCacheIndex] = rootObject
        }
        /// Limit cache to 16 recent root objects. We only really need one per active datastore.
        /// Note more-recently accessed entries may be represented multiple times in the cache, and are more likely to survive.
        rollingRootObjectCacheIndex = (rollingRootObjectCacheIndex + 1) % 16
    }
    
    func cache(_ index: Datastore.Index) {
        if rollingIndexCache.count <= rollingIndexCacheIndex {
            rollingIndexCache.append(index)
        } else {
            rollingIndexCache[rollingIndexCacheIndex] = index
        }
        /// Limit cache to 128 recent indexes, which is 8 per datastore.
        /// Note more-recently accessed entries may be represented multiple times in the cache, and are more likely to survive.
        rollingIndexCacheIndex = (rollingIndexCacheIndex + 1) % 128
    }
    
    func cache(_ page: Datastore.Page) {
        if rollingPageCache.count <= rollingPageCacheIndex {
            rollingPageCache.append(page)
        } else {
            rollingPageCache[rollingPageCacheIndex] = page
        }
        /// Limit cache to 4096 recent pages, which is up to 16MB.
        /// Note more-recently accessed entries may be represented multiple times in the cache, and are more likely to survive.
        rollingPageCacheIndex = (rollingPageCacheIndex + 1) % 4096
    }
}

// MARK: - Helper Types

class WeakDatastore {
    var canWrite: Bool = false
    
    var isAlive: Bool { return true }
    
    func contains<Format: DatastoreFormat, AccessMode>(
        datastore: Datastore<Format, AccessMode>
    ) -> Bool {
        return false
    }
}

class WeakSpecificDatastore<Format: DatastoreFormat, AccessMode: _AccessMode>: WeakDatastore {
    weak var datastore: Datastore<Format, AccessMode>?
    
    override var isAlive: Bool { return datastore != nil }
    
    init(datastore: Datastore<Format, AccessMode>) {
        self.datastore = datastore
        super.init()
        self.canWrite = false
    }
    
    init(datastore: Datastore<Format, AccessMode>) where AccessMode == ReadWrite {
        self.datastore = datastore
        super.init()
        self.canWrite = true
    }
    
    override func contains<OtherFormat: DatastoreFormat, OtherAccessMode>(
        datastore: Datastore<OtherFormat, OtherAccessMode>
    ) -> Bool {
        return datastore === self.datastore
    }
}

enum ModificationUpdate {
    case transparent
    case updateOnWrite
    case set(Date)
    
    func modificationDate(for date: Date) -> Date {
        switch self {
        case .transparent: return date
        case .updateOnWrite: return Date()
        case .set(let newDate): return newDate
        }
    }
}

private enum DiskPersistenceTaskLocals {
    @TaskLocal
    static var storeInfo: StoreInfo?
}

