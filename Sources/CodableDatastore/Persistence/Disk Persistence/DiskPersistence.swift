//
//  DiskPersistence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

public actor DiskPersistence<AccessMode: _AccessMode>: Persistence {
    /// The location of this persistence.
    let storeURL: URL
    
    /// A cached instance of the store info as last loaded from disk.
    var cachedStoreInfo: StoreInfo?
    
    /// A pointer to the last store info updater, so updates can be serialized after the last request
    var lastUpdateStoreInfoTask: Task<Any, Error>?
    
    /// The loaded Snapshots
    var snapshots: [SnapshotIdentifier: Snapshot<AccessMode>] = [:]
    
    var registeredDatastores: [String: [WeakDatastore]] = [:]
    
    var lastTransaction: Transaction?
    
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
            if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 8, *) {
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
        } catch URLError.fileDoesNotExist, CocoaError.fileReadNoSuchFile {
            return StoreInfo(modificationDate: Date())
        } catch let error as NSError where error.domain == NSPOSIXErrorDomain && error.code == Int(POSIXError.ENOENT.rawValue) {
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
    func updateStoreInfo<T>(updater: @escaping (_ storeInfo: inout StoreInfo) async throws -> T) -> Task<T, Error> where AccessMode == ReadWrite {
        
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
    func updateStoreInfo<T>(accessor: @escaping (_ storeInfo: StoreInfo) async throws -> T) -> Task<T, Error> {
        
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
    func withStoreInfo<T>(updater: @escaping (_ storeInfo: inout StoreInfo) async throws -> T) async throws -> T where AccessMode == ReadWrite {
        try await updateStoreInfo(updater: updater).value
    }
    
    /// Load the store info in an updater.
    ///
    /// This method loads the ``StoreInfo`` from cache.
    ///
    /// - Parameter accessor: An accessor that takes an immutable reference to a store info, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `accessor`.
    func withStoreInfo<T>(accessor: @escaping (_ storeInfo: StoreInfo) async throws -> T) async throws -> T where AccessMode == ReadOnly {
        try await updateStoreInfo(accessor: accessor).value
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
    func updateCurrentSnapshot<T>(
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
    func updateCurrentSnapshot<T>(
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
    func withCurrentSnapshot<T>(
        dateUpdate: ModificationUpdate = .updateOnWrite,
        updater: @escaping (_ snapshot: Snapshot<AccessMode>) async throws -> T
    ) async throws -> T where AccessMode == ReadWrite {
        try await updateCurrentSnapshot(dateUpdate: dateUpdate, updater: updater).value
    }
    
    /// Load the current snapshot in an accessor.
    ///
    /// This method loads the current ``Snapshot`` so it can be accessed.
    ///
    /// - Note: Calling this method when no store info exists on disk will create it.
    /// - Parameter dateUpdate: The method to which to update the date of the main store with.
    /// - Parameter accessor: An accessor that takes a reference to the current ``Snapshot``, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `accessor`.
    func withCurrentSnapshot<T>(
        accessor: @escaping (_ snapshot: Snapshot<AccessMode>) async throws -> T
    ) async throws -> T {
        try await updateCurrentSnapshot(accessor: accessor).value
    }
}

// MARK: - Persisitence Creation
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
    public func register<V, C, I, A>(
        datastore newDatastore: CodableDatastore.Datastore<V, C, I, A>
    ) async throws -> DatastoreDescriptor? {
        guard
            let datastorePersistence = newDatastore.persistence as? DiskPersistence,
            datastorePersistence === self
        else {
            assertionFailure("The datastore has already been registered with another persistence. Make sure to only register a datastore with a single persistence. This will throw an error on release builds.")
            throw PersistenceError.multipleRegistrations
        }
        
        var existingDatastores = registeredDatastores[newDatastore.key, default: []].filter(\.isAlive)
        
        for weakDatastore in existingDatastores {
            if weakDatastore.contains(datastore: newDatastore) {
                assertionFailure("The datastore has already been registered with this persistence. Make sure to not call register multiple times per persistence. This will throw an error on release builds.")
                throw PersistenceError.alreadyRegistered
            }
            
            if A.self == ReadWrite.self, weakDatastore.canWrite {
                assertionFailure("An existing datastore that can write to the persistence has already been registered for this key. Only one writer is suppored per key. This will throw an error on release builds.")
                throw PersistenceError.duplicateWriters
            }
        }
        
        existingDatastores.append(WeakSpecificDatastore(datastore: newDatastore))
        
        registeredDatastores[newDatastore.key] = existingDatastores
        
        return try await datastoreDescriptor(for: newDatastore)
    }
    
    public func datastoreDescriptor<V, C, I, A>(
        for datastore: CodableDatastore.Datastore<V, C, I, A>
    ) async throws -> DatastoreDescriptor? {
        guard
            let datastorePersistence = datastore.persistence as? DiskPersistence,
            datastorePersistence === self
        else {
            assertionFailure("The datastore is registered with another persistence. Make sure to only register a datastore with a single persistence. This will throw an error on release builds.")
            throw PersistenceError.multipleRegistrations
        }
        
        return try await withCurrentSnapshot { snapshot in
            let (datastoreActor, rootObject) = try await snapshot.withManifest { snapshotManifest in
                await snapshot.loadDatastore(for: datastore.key, from: snapshotManifest)
            }
            
            guard let rootObject else { return nil }
            let datastoreInfo = try await datastoreActor.loadRootObject(for: rootObject)
            return datastoreInfo.descriptor
        }
    }
    
    public func apply(
        descriptor: DatastoreDescriptor,
        for datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
}

// MARK: - Cursor Lookups

extension DiskPersistence {
    public func primaryIndexCursor<IdentifierType: Indexable>(
        for identifier: IdentifierType,
        datastoreKey: String
    ) async throws -> (
        cursor: any InstanceCursorProtocol,
        instanceData: Data,
        versionData: Data
    ) {
        preconditionFailure("Unimplemented")
    }
    
    public func primaryIndexCursor<IdentifierType: Indexable>(
        inserting identifier: IdentifierType,
        datastoreKey: String
    ) async throws -> any InsertionCursorProtocol {
        preconditionFailure("Unimplemented")
    }
    
    public func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        for index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> (
        cursor: any InstanceCursorProtocol,
        instanceData: Data,
        versionData: Data
    ) {
        preconditionFailure("Unimplemented")
    }
    
    public func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> any InsertionCursorProtocol {
        preconditionFailure("Unimplemented")
    }
    
    public func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        for index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> any InstanceCursorProtocol {
        preconditionFailure("Unimplemented")
    }
    
    public func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> any InsertionCursorProtocol {
        preconditionFailure("Unimplemented")
    }
}

// MARK: - Entry Manipulation

extension DiskPersistence {
    public func persistPrimaryIndexEntry<IdentifierType: Indexable>(
        versionData: Data,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor: some InsertionCursorProtocol,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    public func deletePrimaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    public func resetPrimaryIndex(
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    public func persistDirectIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        versionData: Data,
        indexValue: IndexType,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor: some InsertionCursorProtocol,
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    public func deleteDirectIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    public func deleteDirectIndex(
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    public func persistSecondaryIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        indexValue: IndexType,
        identifierValue: IdentifierType,
        cursor: some InsertionCursorProtocol,
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    public func deleteSecondaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    public func deleteSecondaryIndex(
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
}

// MARK: - Transactions

extension DiskPersistence {
    public func withUnsafeTransaction(options: TransactionOptions, transaction: @escaping (_ persistence: DiskPersistence) async throws -> ()) async throws {
        let (transacrion, task) = await Transaction.makeTransaction(persistence: self, lastTransaction: lastTransaction, options: options) {
            try await transaction(self)
        }
        
        /// Save the last non-concurrent transaction from the list. Note that disk persistence currently does not support concurrent idempotent transactions.
        if !options.contains(.readOnly) {
            lastTransaction = transacrion
        }
        
        try await task.value
    }
}

// MARK: - Helper Types

class WeakDatastore {
    var canWrite: Bool = false
    
    var isAlive: Bool { return true }
    
    func contains<Version, CodedType, IdentifierType, AccessMode>(datastore: Datastore<Version, CodedType, IdentifierType, AccessMode>) -> Bool {
        return false
    }
}

class WeakSpecificDatastore<
    Version: RawRepresentable & Hashable & CaseIterable,
    CodedType: Codable,
    IdentifierType: Indexable,
    AccessMode: _AccessMode
>: WeakDatastore where Version.RawValue: Indexable & Comparable {
    weak var datastore: Datastore<Version, CodedType, IdentifierType, AccessMode>?
    
    override var isAlive: Bool { return datastore != nil }
    
    init(datastore: Datastore<Version, CodedType, IdentifierType, AccessMode>) {
        self.datastore = datastore
        super.init()
        self.canWrite = false
    }
    
    init(datastore: Datastore<Version, CodedType, IdentifierType, AccessMode>) where AccessMode == ReadWrite {
        self.datastore = datastore
        super.init()
        self.canWrite = true
    }
    
    override func contains<OtherVersion, OtherCodedType, OtherIdentifierType, OtherAccessMode>(datastore: Datastore<OtherVersion, OtherCodedType, OtherIdentifierType, OtherAccessMode>) -> Bool {
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

