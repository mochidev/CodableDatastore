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
    
    /// Shared caches across all snapshots and datastores.
    var rollingRootObjectCacheIndex = 0
    var rollingRootObjectCache: [Datastore.RootObject] = []
    
    var rollingIndexCacheIndex = 0
    var rollingIndexCache: [Datastore.Index] = []
    
    var rollingPageCacheIndex = 0
    var rollingPageCache: [Datastore.Page] = []
    
    var transactionCounter = 0
    
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
        if let storeInfo = DiskPersistenceTaskLocals.storeInfo(for: self) {
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
            let returnValue = try await DiskPersistenceTaskLocals.with(storeInfo: storeInfo, for: self) {
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
        if let storeInfo = DiskPersistenceTaskLocals.storeInfo(for: self) {
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
            return try await DiskPersistenceTaskLocals.with(storeInfo: storeInfo, for: self) {
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
    ) async throws -> T {
        try await withoutActuallyEscaping(accessor) { escapingClosure in
            try await updateStoreInfo(accessor: escapingClosure).value
        }
    }
}

// MARK: - Snapshot Management

extension DiskPersistence {
    /// Load the default snapshot from disk, or create an empty one if such a file does not exist.
    ///
    /// - Parameters:
    ///   - storeInfo: The store infor to load from.
    ///   - newSnapshotIdentifier: A new snapshot identifier to use if the store doesn't have one yet. If nil, a new one will be created automatically.
    /// - Returns: A snapshot to start using.
    private func loadSnapshot(
        from storeInfo: StoreInfo,
        newSnapshotIdentifier: SnapshotIdentifier? = nil
    ) -> Snapshot<AccessMode> {
        let snapshotID = storeInfo.currentSnapshot ?? newSnapshotIdentifier ?? SnapshotIdentifier()
        
        if let snapshot = snapshots[snapshotID] {
            return snapshot
        }
        
        let snapshot = Snapshot(id: snapshotID, persistence: self)
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
    
    /// Create a new snapshot from the current snapshot the persistence is pointing to.
    ///
    /// This method is temporarily publc — once all the components are public, you should use them directly before the next minor version.
    public func _takeSnapshot() async throws where AccessMode == ReadWrite {
        try await _takeSnapshot(newSnapshotIdentifier: nil)
    }
    
    func _takeSnapshot(
        newSnapshotIdentifier: SnapshotIdentifier?
    ) async throws where AccessMode == ReadWrite { // TODO: return new snapshot iteration
        let readSnapshot = try await currentSnapshot
        let newSnapshot = try await createSnapshot(from: readSnapshot, newSnapshotIdentifier: newSnapshotIdentifier)
        try await setCurrentSnapshot(snapshot: newSnapshot)
    }
    
    /// Load the current snapshot the persistence is reading and writing to.
    var currentSnapshot: Snapshot<AccessMode> {
        // TODO: This should return a readonly snapshot, but we need to be able to make a read-only copy from the persistence first.
        get async throws {
            try await withStoreInfo { await loadSnapshot(from: $0) }
        }
    }
    
    func setCurrentSnapshot(
        snapshot: Snapshot<ReadWrite>,
        dateUpdate: ModificationUpdate = .updateOnWrite
    ) async throws where AccessMode == ReadWrite {
        try await withStoreInfo { storeInfo in
            guard snapshot.persistence === self
            else { throw DiskPersistenceError.wrongPersistence }
            
            /// Update the store info with snapshot and modification date to use
            storeInfo.currentSnapshot = snapshot.id
            storeInfo.modificationDate = dateUpdate.modificationDate(for: storeInfo.modificationDate)
        }
    }
    
    func loadSnapshot(id: SnapshotIdentifier) async throws -> Snapshot<ReadOnly> {
        preconditionFailure("Unimplemented")
    }
    
//    var allSnapshots: AsyncStream<Snapshot<ReadOnly>> {
//        /// Crawl the `Snapshots` directory, and collect all the .snapshot folders. Return the manifest structure.
//        preconditionFailure("Unimplemented")
//    }
    
    func createSnapshot(
        from snapshot: Snapshot<ReadWrite>, // TODO: Shouldn't need to be readwrite
        actionName: String? = nil,
        newSnapshotIdentifier: SnapshotIdentifier? = nil
    ) async throws -> Snapshot<ReadWrite> where AccessMode == ReadWrite {
        let newSnapshot = try await snapshot.copy(
            into: self,
            actionName: actionName,
            newSnapshotIdentifier: newSnapshotIdentifier,
            targetPageSize: Configuration.defaultPageSize
        )
        
        /// Save a reference to this new snapshot, and return it.
        snapshots[newSnapshot.id] = newSnapshot
        return newSnapshot
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
    func nextTransactionCounter() -> Int {
        let transactionIndex = transactionCounter
        transactionCounter += 1
        return transactionIndex
    }
    
    public func _withTransaction<T: Sendable>(
        actionName: String?,
        options: UnsafeTransactionOptions,
        transaction: @Sendable (_ transaction: DatastoreInterfaceProtocol, _ isDurable: Bool) async throws -> T
    ) async throws -> T {
        try await withoutActuallyEscaping(transaction) { escapingTransaction in
            /// If the transaction is starting in the context of another persistence's transaction, make sure it is a read-only one. Otherwise assert and throw an error as it likely indicates a mistake and could lead to unexpected consistency violations if one persistence succeeds while the other fails.
            if
                Transaction.isTransactingExternally(to: self),
                !options.contains(.readOnly)
            {
                assertionFailure(DatastoreInterfaceError.transactingWithinExternalPersistence.localizedDescription)
                throw DatastoreInterfaceError.transactingWithinExternalPersistence
            }
            
            let currentCounter = nextTransactionCounter()
//            print("[CDS] [\(storeURL.lastPathComponent)] Starting transaction \(currentCounter) “\(actionName ?? "")” - \(options)")
            let (transaction, task) = await Transaction.makeTransaction(
                persistence: self,
                transactionIndex: currentCounter,
                lastTransaction: lastTransaction,
                actionName: actionName,
                options: options
            ) { interface, isDurable in
                try await escapingTransaction(interface, isDurable)
            }
            
            /// Save the last non-concurrent top-level transaction from the list. Note that disk persistence currently does not support concurrent idempotent transactions.
            if !options.contains(.readOnly), transaction.parent == nil {
                lastTransaction = transaction
            }
            
            let result = try await task.value
//            print("[CDS] [\(storeURL.lastPathComponent)] Finished transaction \(currentCounter) “\(actionName ?? "")” - \(options)")
            return result
        }
    }
    
    func persist(
        transactionIndex: Int,
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
        
//        print("[CDS] [\(storeURL.lastPathComponent)] Persisting \(transactionIndex) “\(actionName ?? "")” - \(roots.keys), added \(addedDatastoreRoots), removed \(removedDatastoreRoots)")
        
        /// If we are read-only, make sure no edits have been made
        guard let self = self as? DiskPersistence<ReadWrite>
        else { throw DiskPersistenceError.cannotWrite }
        
        /// If we are read-write, apply the updated root objects to the snapshot.
        try await self.updatingCurrentSnapshot { snapshot in
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
            }
        }
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
    static var storeInfoStorage: [ObjectIdentifier : StoreInfo] = [:]
    
    static func storeInfo<AccessMode: _AccessMode>(for persistence: DiskPersistence<AccessMode>) -> StoreInfo? {
        storeInfoStorage[ObjectIdentifier(persistence)]
    }
    
    static func with<AccessMode: _AccessMode, R>(
        storeInfo: StoreInfo,
        for persistence: DiskPersistence<AccessMode>,
        operation: () async throws -> R
    ) async rethrows -> R {
        var currentStorage = storeInfoStorage
        currentStorage[ObjectIdentifier(persistence)] = storeInfo
        
        return try await $storeInfoStorage.withValue(currentStorage, operation: operation)
    }
}

