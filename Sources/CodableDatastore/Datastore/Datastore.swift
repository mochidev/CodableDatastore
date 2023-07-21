//
//  Datastore.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A store for a homogenous collection of instances.
public actor Datastore<
    Version: RawRepresentable & Hashable & CaseIterable,
    CodedType: Codable,
    IdentifierType: Indexable,
    AccessMode: _AccessMode
> where Version.RawValue: Indexable & Comparable {
    let persistence: any Persistence
    let key: DatastoreKey
    let version: Version
    let encoder: (_ instance: CodedType) async throws -> Data
    let decoders: [Version: (_ data: Data) async throws -> (id: IdentifierType, instance: CodedType)]
    let directIndexes: [IndexPath<CodedType, _AnyIndexed>]
    let computedIndexes: [IndexPath<CodedType, _AnyIndexed>]
    
    var updatedDescriptor: DatastoreDescriptor?
    
    fileprivate var warmupStatus: TaskStatus = .waiting
    fileprivate var warmupProgressHandlers: [ProgressHandler] = []
    
    fileprivate var storeMigrationStatus: TaskStatus = .waiting
    fileprivate var storeMigrationProgressHandlers: [ProgressHandler] = []
    
    fileprivate var indexMigrationStatus: [IndexPath<CodedType, _AnyIndexed> : TaskStatus] = [:]
    fileprivate var indexMigrationProgressHandlers: [IndexPath<CodedType, _AnyIndexed> : ProgressHandler] = [:]
    
    public init(
        persistence: some Persistence<AccessMode>,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        encoder: @escaping (_ instance: CodedType) async throws -> Data,
        decoders: [Version: (_ data: Data) async throws -> (id: IdentifierType, instance: CodedType)],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) where AccessMode == ReadWrite {
        self.persistence = persistence
        self.key = key
        self.version = version
        self.encoder = encoder
        self.decoders = decoders
        self.directIndexes = directIndexes
        self.computedIndexes = computedIndexes
        
        for decoderVersion in Version.allCases {
            guard decoders[decoderVersion] == nil else { continue }
            assertionFailure("Decoders missing case for \(decoderVersion). Please make sure you have a decoder configured for this version or you may encounter errors at runtime.")
        }
    }
    
    public init(
        persistence: some Persistence,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        decoders: [Version: (_ data: Data) async throws -> (id: IdentifierType, instance: CodedType)],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) where AccessMode == ReadOnly {
        self.persistence = persistence
        self.key = key
        self.version = version
        self.encoder = { _ in preconditionFailure("Encode called on read-only instance.") }
        self.decoders = decoders
        self.directIndexes = directIndexes
        self.computedIndexes = computedIndexes
    }
}

// MARK: - Helper Methods

extension Datastore {
    func updatedDescriptor(for instance: CodedType) throws -> DatastoreDescriptor {
        if let updatedDescriptor {
            return updatedDescriptor
        }
        
        let descriptor = try DatastoreDescriptor(
            version: version,
            sampleInstance: instance,
            identifierType: IdentifierType.self,
            directIndexes: directIndexes,
            computedIndexes: computedIndexes
        )
        updatedDescriptor = descriptor
        return descriptor
    }
    
    func decoder(for version: Version) throws -> (_ data: Data) async throws -> (id: IdentifierType, instance: CodedType) {
        guard let decoder = decoders[version] else {
            throw DatastoreError.missingDecoder(version: String(describing: version))
        }
        return decoder
    }
}

// MARK: - Warmup

extension Datastore {
    /// Migrates and warms the data store ahead of time.
    ///
    /// It is recommended you call this method before accessing any data, as it will offer you an opportunity to show a loading screen during potentially long migrations, rather than leaving it for the first read or write on the data store.
    ///
    /// - Parameter progressHandler: A closure that will be regularly called with progress during the migration. If no migration needs to occur, it won't be called, so setup and tear down any UI within the handler.
    public func warm(progressHandler: ProgressHandler? = nil) async throws {
        try await warmupIfNeeded(progressHandler: progressHandler)
    }
    
    func warmupIfNeeded(progressHandler: ProgressHandler? = nil) async throws {
        switch warmupStatus {
        case .complete: return
        case .inProgress(let task):
            if let progressHandler {
                warmupProgressHandlers.append(progressHandler)
            }
            try await task.value
        case .waiting:
            if let progressHandler {
                warmupProgressHandlers.append(progressHandler)
            }
            let warmupTask = Task {
                try await persistence._withTransaction(
                    actionName: "Migrate Entries",
                    options: []
                ) { transaction, _ in
                    try await self.registerAndMigrate(with: transaction)
                }
            }
            warmupStatus = .inProgress(warmupTask)
            try await warmupTask.value
        }
    }
    
    func registerAndMigrate(with transaction: DatastoreInterfaceProtocol) async throws {
        let persistedDescriptor = try await transaction.register(datastore: self)
        
        /// Only operate on read-write datastores beyond this point.
        guard let self = self as? Datastore<Version, CodedType, IdentifierType, ReadWrite>
        else { return }
        
        /// Make sure we have a descriptor, and that there is at least one entry, otherwise stop here.
        guard let persistedDescriptor, persistedDescriptor.size > 0
        else { return }
        
        /// Check the version to see if the current one is greater or equal to the one in the existing descriptor. If we can't decode it, stop here and throw an error — the data store is unsupported.
        let persistedVersion = try Version(persistedDescriptor.version)
        guard persistedVersion.rawValue <= version.rawValue
        else { throw DatastoreError.incompatibleVersion(version: String(describing: persistedVersion)) }
        
        /// Notify progress handlers we are evaluating for possible migrations.
        for handler in warmupProgressHandlers {
            handler(.evaluating)
        }
        
        var newDescriptor: DatastoreDescriptor?
        
        let primaryIndex = load(IndexRange(), order: .ascending, awaitWarmup: false)
        
        var rebuildPrimaryIndex = false
        var directIndexesToBuild: Set<IndexName> = []
        var secondaryIndexesToBuild: Set<IndexName> = []
        var index = 0
        
        let versionData = try Data(self.version)
        
        for try await (idenfifier, instance) in primaryIndex {
            defer { index += 1 }
            /// Use the first index to grab an up-to-date descriptor
            if newDescriptor == nil {
                let updatedDescriptor = try updatedDescriptor(for: instance)
                newDescriptor = updatedDescriptor
                
                /// Check the primary index for compatibility.
                if persistedDescriptor.identifierType != updatedDescriptor.identifierType {
                    try await transaction.resetPrimaryIndex(datastoreKey: key)
                    rebuildPrimaryIndex = true
                }
                
                /// Check existing direct indexes for compatibility
                for (_, persistedIndex) in persistedDescriptor.directIndexes {
                    if let updatedIndex = updatedDescriptor.directIndexes[persistedIndex.name] {
                        /// If the index still exists, make sure it is compatible by checking their types, or checking if the primary index must be re-built.
                        if persistedIndex.type != updatedIndex.type || rebuildPrimaryIndex {
                            /// They were not compatible, so delete the bad index, and queue it to be re-built.
                            try await transaction.deleteDirectIndex(indexName: persistedIndex.name, datastoreKey: key)
                            directIndexesToBuild.insert(persistedIndex.name)
                        }
                    } else {
                        /// The index is no longer needed, delete it.
                        try await transaction.deleteDirectIndex(indexName: persistedIndex.name, datastoreKey: key)
                    }
                }
                
                /// Check for new direct indexes to build
                for (_, updatedIndex) in updatedDescriptor.directIndexes {
                    guard persistedDescriptor.directIndexes[updatedIndex.name] == nil else { continue }
                    /// The index does not yet exist, so queue it to be built.
                    directIndexesToBuild.insert(updatedIndex.name)
                }
                
                /// Check existing secondary indexes for compatibility
                for (_, persistedIndex) in persistedDescriptor.secondaryIndexes {
                    if let updatedIndex = updatedDescriptor.secondaryIndexes[persistedIndex.name] {
                        /// If the index still exists, make sure it is compatible
                        if persistedIndex.type != updatedIndex.type {
                            /// They were not compatible, so delete the bad index, and queue it to be re-built.
                            try await transaction.deleteDirectIndex(indexName: persistedIndex.name, datastoreKey: key)
                            secondaryIndexesToBuild.insert(persistedIndex.name)
                        }
                    } else {
                        /// The index is no longer needed, delete it.
                        try await transaction.deleteDirectIndex(indexName: persistedIndex.name, datastoreKey: key)
                    }
                }
                
                /// Check for new secondary indexes to build
                for (_, updatedIndex) in updatedDescriptor.secondaryIndexes {
                    guard persistedDescriptor.secondaryIndexes[updatedIndex.name] == nil else { continue }
                    /// The index does not yet exist, so queue it to be built.
                    secondaryIndexesToBuild.insert(updatedIndex.name)
                }
                
                /// Remove any direct indexes from the secondary ones we may have requested.
                secondaryIndexesToBuild.subtract(directIndexesToBuild)
                
                /// If we don't need to migrate anything, stop here.
                if rebuildPrimaryIndex == false, directIndexesToBuild.isEmpty, secondaryIndexesToBuild.isEmpty {
                    break
                }
                
                /// Create any missing indexes and prime the datastore for writing.
                try await transaction.apply(descriptor: updatedDescriptor, for: key)
            }
            
            /// Notify progress handlers we are starting an entry.
            for handler in warmupProgressHandlers {
                handler(.working(current: index, total: persistedDescriptor.size))
            }
            
            let instanceData = try await encoder(instance)
            
            if rebuildPrimaryIndex {
                let insertionCursor = try await transaction.primaryIndexCursor(inserting: idenfifier, datastoreKey: key)
                
                try await transaction.persistPrimaryIndexEntry(
                    versionData: versionData,
                    identifierValue: idenfifier,
                    instanceData: instanceData,
                    cursor: insertionCursor,
                    datastoreKey: key
                )
            }
            
            var queriedIndexes: Set<IndexName> = []
            
            /// Persist the direct indexes with full copies
            for indexPath in directIndexes {
                let indexName = indexPath.path
                guard
                    directIndexesToBuild.contains(indexName),
                    !queriedIndexes.contains(indexName)
                else { continue }
                queriedIndexes.insert(indexName)
                
                let updatedValue = instance[keyPath: indexPath]
                
                /// Grab a cursor to insert the new value in the index.
                let updatedValueCursor = try await transaction.directIndexCursor(
                    inserting: updatedValue.indexed,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: key
                )
                
                /// Insert it.
                try await transaction.persistDirectIndexEntry(
                    versionData: versionData,
                    indexValue: updatedValue.indexed,
                    identifierValue: idenfifier,
                    instanceData: instanceData,
                    cursor: updatedValueCursor,
                    indexName: indexName,
                    datastoreKey: key
                )
            }
            
            /// Next, go through any remaining computed indexes as secondary indexes.
            for indexPath in computedIndexes {
                let indexName = indexPath.path
                guard
                    secondaryIndexesToBuild.contains(indexName),
                    !queriedIndexes.contains(indexName)
                else { continue }
                queriedIndexes.insert(indexName)
                
                let updatedValue = instance[keyPath: indexPath]
                
                /// Grab a cursor to insert the new value in the index.
                let updatedValueCursor = try await transaction.secondaryIndexCursor(
                    inserting: updatedValue.indexed,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: self.key
                )
                
                /// Insert it.
                try await transaction.persistSecondaryIndexEntry(
                    indexValue: updatedValue.indexed,
                    identifierValue: idenfifier,
                    cursor: updatedValueCursor,
                    indexName: indexName,
                    datastoreKey: self.key
                )
            }
            
            /// Re-insert any remaining indexed values into the new index.
            try await Mirror.indexedChildren(from: instance, assertIdentifiable: true) { indexName, value in
                let indexName = IndexName(indexName)
                guard
                    secondaryIndexesToBuild.contains(indexName),
                    !queriedIndexes.contains(indexName)
                else { return }
                
                /// Grab a cursor to insert the new value in the index.
                let updatedValueCursor = try await transaction.secondaryIndexCursor(
                    inserting: value,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: self.key
                )
                
                /// Insert it.
                try await transaction.persistSecondaryIndexEntry(
                    indexValue: value,
                    identifierValue: idenfifier,
                    cursor: updatedValueCursor,
                    indexName: indexName,
                    datastoreKey: self.key
                )
            }
        }
        
        for handler in warmupProgressHandlers {
            handler(.complete(total: persistedDescriptor.size))
        }
        
        warmupProgressHandlers.removeAll()
        warmupStatus = .complete
    }
}

// MARK: - Migrations

extension Datastore where AccessMode == ReadWrite {
    /// Manually migrate an index if the version persisted is less than a given minimum version.
    /// 
    /// Only use this if you must force an index to be re-calculated, which is sometimes necessary when the implementation of the compare method changes between releases.
    ///
    /// - Parameters:
    ///   - index: The index to migrate.
    ///   - minimumVersion: The minimum valid version for an index to not be migrated.
    ///   - progressHandler: A closure that will be regularly called with progress during the migration. If no migration needs to occur, it won't be called, so setup and tear down any UI within the handler.
    public func migrate(index: IndexPath<CodedType, _AnyIndexed>, ifLessThan minimumVersion: Version, progressHandler: ProgressHandler? = nil) async throws {
        try await persistence._withTransaction(
            actionName: "Migrate Entries",
            options: []
        ) { transaction, _ in
            guard
                /// If we have no descriptor, then no data exists to be migrated.
                let descriptor = try await transaction.datastoreDescriptor(for: self.key),
                descriptor.size > 0,
                /// If we don't have an index stored, there is nothing to do here. This means we can skip checking it on the type.
                let matchingIndex = descriptor.directIndexes[index.path] ?? descriptor.secondaryIndexes[index.path],
                /// We don't care in this method of the version is incompatible — the index will be discarded.
                let version = try? Version(matchingIndex.version),
                /// Make sure the stored version is smaller than the one we require, otherwise stop early.
                version.rawValue < minimumVersion.rawValue
            else { return }
            
            var warmUpProgress: Progress = .complete(total: 0)
            try await self.warmupIfNeeded { progress in
                warmUpProgress = progress
                progressHandler?(progress.adding(current: 0, total: descriptor.size))
            }
            
            /// Make sure we still need to do the work, as the warm up may have made changes anyways due to incompatible types.
            guard
                /// If we have no descriptor, then no data exists to be migrated.
                let descriptor = try await transaction.datastoreDescriptor(for: self.key),
                descriptor.size > 0,
                /// If we don't have an index stored, there is nothing to do here. This means we can skip checking it on the type.
                let matchingIndex = descriptor.directIndexes[index.path] ?? descriptor.secondaryIndexes[index.path],
                /// We don't care in this method of the version is incompatible — the index will be discarded.
                let version = try? Version(matchingIndex.version),
                /// Make sure the stored version is smaller than the one we require, otherwise stop early.
                version.rawValue < minimumVersion.rawValue
            else {
                progressHandler?(warmUpProgress.adding(current: descriptor.size, total: descriptor.size))
                return
            }
            
            try await self.migrate(index: index) { migrateProgress in
                progressHandler?(warmUpProgress.adding(migrateProgress))
            }
        }
    }
    
    func migrate(index: IndexPath<CodedType, _AnyIndexed>, progressHandler: ProgressHandler? = nil) async throws {
        // TODO: Migrate just that index, use indexMigrationStatus and indexMigrationProgressHandlers to record progress.
    }
    
    /// Manually migrate the entire store if the primary index version persisted is less than a given minimum version.
    ///
    /// Only use this if you must force the entire store to be re-calculated, which is sometimes necessary when the implementation of the `IdentifierType`'s compare method changes between releases.
    ///
    /// - Parameters:
    ///   - minimumVersion: The minimum valid version for an index to not be migrated.
    ///   - progressHandler: A closure that will be regularly called with progress during the migration. If no migration needs to occur, it won't be called, so setup and tear down any UI within the handler.
    public func migrateEntireStore(ifLessThan minimumVersion: Version, progressHandler: ProgressHandler? = nil) async throws {
        // TODO: Like the method above, check the description to see if a migration is needed
    }
    
    func migrateEntireStore(progressHandler: ProgressHandler?) async throws {
        // TODO: Migrate all indexes, use storeMigrationStatus and storeMigrationProgressHandlers to record progress.
    }
}

// MARK: - Loading

extension Datastore {
    /// The number of objects in the datastore.
    ///
    /// - Note: This count may not reflect an up to dat value while data is being written concurrently, but will be acurate after such a transaction finishes.
    public var count: Int {
        get async throws {
            try await warmupIfNeeded()
            
            return try await persistence._withTransaction(
                actionName: nil,
                options: [.idempotent, .readOnly]
            ) { transaction, _ in
                let descriptor = try await transaction.datastoreDescriptor(for: self.key)
                return descriptor?.size ?? 0
            }
        }
    }
    
    public func load(_ identifier: IdentifierType) async throws -> CodedType? {
        try await warmupIfNeeded()
        
        return try await persistence._withTransaction(
            actionName: nil,
            options: [.idempotent, .readOnly]
        ) { transaction, _ in
            do {
                let persistedEntry = try await transaction.primaryIndexCursor(for: identifier, datastoreKey: self.key)
                
                let entryVersion = try Version(persistedEntry.versionData)
                let decoder = try await self.decoder(for: entryVersion)
                let instance = try await decoder(persistedEntry.instanceData).instance
                
                return instance
            } catch DatastoreInterfaceError.instanceNotFound {
                return nil
            } catch {
                throw error
            }
        }
    }
    
    nonisolated func load(
        _ range: some IndexRangeExpression<IdentifierType>,
        order: RangeOrder,
        awaitWarmup: Bool
    ) -> some TypedAsyncSequence<(id: IdentifierType, instance: CodedType)> {
        AsyncThrowingBackpressureStream { provider in
            if awaitWarmup {
                try await self.warmupIfNeeded()
            }
            
            try await self.persistence._withTransaction(
                actionName: nil,
                options: [.readOnly]
            ) { transaction, _ in
                try await transaction.primaryIndexScan(range: range.applying(order), datastoreKey: self.key) { versionData, instanceData in
                    let entryVersion = try Version(versionData)
                    let decoder = try await self.decoder(for: entryVersion)
                    let decodedValue = try await decoder(instanceData)
                    
                    try await provider.yield(decodedValue)
                }
            }
        }
    }
    
    nonisolated public func load(
        _ range: some IndexRangeExpression<IdentifierType>,
        order: RangeOrder = .ascending
    ) -> some TypedAsyncSequence<CodedType> {
        load(range, order: order, awaitWarmup: true)
            .map { $0.instance }
    }
    
    @_disfavoredOverload
    public nonisolated func load(
        _ range: IndexRange<IdentifierType>,
        order: RangeOrder = .ascending
    ) -> some TypedAsyncSequence<CodedType> {
        load(range, order: order)
    }
    
    public nonisolated func load(
        _ range: Swift.UnboundedRange,
        order: RangeOrder = .ascending
    ) -> some TypedAsyncSequence<CodedType> {
        load(IndexRange(), order: order)
    }
    
    public nonisolated func load<IndexedValue: Indexable>(
        _ range: some IndexRangeExpression<IndexedValue>,
        order: RangeOrder = .ascending,
        from indexPath: IndexPath<CodedType, _SomeIndexed<IndexedValue>>
    ) -> some TypedAsyncSequence<CodedType> {
        let a: AsyncThrowingBackpressureStream<CodedType> = AsyncThrowingBackpressureStream { provider in
            try await self.warmupIfNeeded()
            
            try await self.persistence._withTransaction(
                actionName: nil,
                options: [.readOnly]
            ) { transaction, _ in
                let isDirectIndex = self.directIndexes.contains { $0.path == indexPath.path }
                
                if isDirectIndex {
                    try await transaction.directIndexScan(
                        range: range.applying(order),
                        indexName: indexPath.path,
                        datastoreKey: self.key
                    ) { versionData, instanceData in
                        let entryVersion = try Version(versionData)
                        let decoder = try await self.decoder(for: entryVersion)
                        let instance = try await decoder(instanceData).instance
                        
                        try await provider.yield(instance)
                    }
                } else {
                    try await transaction.secondaryIndexScan(
                        range: range.applying(order),
                        indexName: indexPath.path,
                        datastoreKey: self.key
                    ) { (identifier: IdentifierType) in
                        let persistedEntry = try await transaction.primaryIndexCursor(for: identifier, datastoreKey: self.key)
                        
                        let entryVersion = try Version(persistedEntry.versionData)
                        let decoder = try await self.decoder(for: entryVersion)
                        let instance = try await decoder(persistedEntry.instanceData).instance
                        
                        try await provider.yield(instance)
                    }
                }
            }
        }
        return a
    }
    
    @_disfavoredOverload
    public nonisolated func load<IndexedValue: Indexable>(
        _ range: IndexRange<IndexedValue>,
        order: RangeOrder = .ascending,
        from keypath: IndexPath<CodedType, _SomeIndexed<IndexedValue>>
    ) -> some TypedAsyncSequence<CodedType> {
        load(range, order: order, from: keypath)
    }
    
    public nonisolated func load<IndexedValue: Indexable>(
        _ range: Swift.UnboundedRange,
        order: RangeOrder = .ascending,
        from keypath: IndexPath<CodedType, _SomeIndexed<IndexedValue>>
    ) -> some TypedAsyncSequence<CodedType> {
        load(IndexRange<IndexedValue>(), order: order, from: keypath)
    }
    
    public nonisolated func load<IndexedValue: Indexable>(
        _ value: IndexedValue,
        order: RangeOrder = .ascending,
        from keypath: IndexPath<CodedType, _SomeIndexed<IndexedValue>>
    ) -> some TypedAsyncSequence<CodedType> {
        load(value...value, order: order, from: keypath)
    }
}

// MARK: - Observation

extension Datastore {
    public func observe(_ idenfifier: IdentifierType) async throws -> some TypedAsyncSequence<ObservedEvent<IdentifierType, CodedType>> {
        try await self.observe()
            .filter { $0.id == idenfifier }
    }
    
    public func observe() async throws -> some TypedAsyncSequence<ObservedEvent<IdentifierType, CodedType>> {
        try await warmupIfNeeded()
        
        return try await persistence._withTransaction(
            actionName: nil,
            options: [.idempotent, .readOnly]
        ) { transaction, _ in
            try await transaction.makeObserver(
                identifierType: IdentifierType.self,
                datastoreKey: self.key,
                bufferingPolicy: .unbounded
            )
        }.compactMap { event in
            try? await event.mapEntries { entry in
                let version = try Version(entry.versionData)
                let decoder = try await self.decoder(for: version)
                let instance = try await decoder(entry.instanceData).instance
                return instance
            }
        }
    }
}

// MARK: - Writing

extension Datastore where AccessMode == ReadWrite {
    /// Persist an instance for a given identifier.
    ///
    /// If an instance does not already exist for the specified identifier, it will be created. If an instance already exists, it will be updated.
    /// - Note: If you instance conforms to Identifiable, it it preferable to use ``persist(_:)`` instead.
    /// - Parameters:
    ///   - instance: The instance to persist.
    ///   - idenfifier: The unique identifier to use to reference the item being persisted.
    public func persist(_ instance: CodedType, to idenfifier: IdentifierType) async throws {
        try await warmupIfNeeded()
        
        let updatedDescriptor = try self.updatedDescriptor(for: instance)
        let versionData = try Data(self.version)
        let instanceData = try await self.encoder(instance)
        
        try await persistence._withTransaction(
            actionName: "Persist Entry",
            options: [.idempotent]
        ) { transaction, _ in
            /// Create any missing indexes or prime the datastore for writing.
            try await transaction.apply(descriptor: updatedDescriptor, for: self.key)
            
            let existingEntry: (cursor: any InstanceCursorProtocol, instance: CodedType, versionData: Data, instanceData: Data)? = try await {
                do {
                    let existingEntry = try await transaction.primaryIndexCursor(for: idenfifier, datastoreKey: self.key)
                    
                    let existingVersion = try Version(existingEntry.versionData)
                    let decoder = try await self.decoder(for: existingVersion)
                    let existingInstance = try await decoder(existingEntry.instanceData).instance
                    
                    return (
                        cursor: existingEntry.cursor,
                        instance: existingInstance,
                        versionData: existingEntry.versionData,
                        instanceData: existingEntry.instanceData
                    )
                } catch DatastoreInterfaceError.instanceNotFound {
                    return nil
                } catch {
                    throw error
                }
            }()
            
            /// Grab the insertion cursor in the primary index.
            let existingInstance = existingEntry?.instance
            let insertionCursor: any InsertionCursorProtocol = try await {
                if let existingEntry { return existingEntry.cursor }
                return try await transaction.primaryIndexCursor(inserting: idenfifier, datastoreKey: self.key)
            }()
            
            if let existingEntry {
                try await transaction.emit(
                    event: .updated(
                        id: idenfifier,
                        oldEntry: ObservationEntry(
                            versionData: existingEntry.versionData,
                            instanceData: existingEntry.instanceData
                        ),
                        newEntry: ObservationEntry(
                            versionData: versionData,
                            instanceData: instanceData
                        )
                    ),
                    datastoreKey: self.key
                )
            } else {
                try await transaction.emit(
                    event: .created(
                        id: idenfifier,
                        newEntry: ObservationEntry(
                            versionData: versionData,
                            instanceData: instanceData
                        )
                    ),
                    datastoreKey: self.key
                )
            }
            
            /// Persist the entry in the primary index
            try await transaction.persistPrimaryIndexEntry(
                versionData: versionData,
                identifierValue: idenfifier,
                instanceData: instanceData,
                cursor: insertionCursor,
                datastoreKey: self.key
            )
            
            var queriedIndexes: Set<IndexName> = []
            
            /// Persist the direct indexes with full copies
            for indexPath in self.directIndexes {
                let indexName = indexPath.path
                guard !queriedIndexes.contains(indexName) else { continue }
                queriedIndexes.insert(indexName)
                
                let existingValue = existingInstance?[keyPath: indexPath]
                let updatedValue = instance[keyPath: indexPath]
//                let indexType = updatedValue.indexedType
                
                if let existingValue {
                    /// Grab a cursor to the old value in the index.
                    let existingValueCursor = try await transaction.directIndexCursor(
                        for: existingValue.indexed,
                        identifier: idenfifier,
                        indexName: indexName,
                        datastoreKey: self.key
                    )
                    
                    /// Delete it.
                    try await transaction.deleteDirectIndexEntry(
                        cursor: existingValueCursor.cursor,
                        indexName: indexName,
                        datastoreKey: self.key
                    )
                }
                
                /// Grab a cursor to insert the new value in the index.
                let updatedValueCursor = try await transaction.directIndexCursor(
                    inserting: updatedValue.indexed,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: self.key
                )
                
                /// Insert it.
                try await transaction.persistDirectIndexEntry(
                    versionData: versionData,
                    indexValue: updatedValue.indexed,
                    identifierValue: idenfifier,
                    instanceData: instanceData,
                    cursor: updatedValueCursor,
                    indexName: indexName,
                    datastoreKey: self.key
                )
            }
            
            /// Next, go through any remaining computed indexes as secondary indexes.
            for indexPath in self.computedIndexes {
                let indexName = indexPath.path
                guard !queriedIndexes.contains(indexName) else { continue }
                queriedIndexes.insert(indexName)
                
                let existingValue = existingInstance?[keyPath: indexPath]
                let updatedValue = instance[keyPath: indexPath]
//                let indexType = updatedValue.indexedType
                
                if let existingValue {
                    /// Grab a cursor to the old value in the index.
                    let existingValueCursor = try await transaction.secondaryIndexCursor(
                        for: existingValue.indexed,
                        identifier: idenfifier,
                        indexName: indexName,
                        datastoreKey: self.key
                    )
                    
                    /// Delete it.
                    try await transaction.deleteSecondaryIndexEntry(
                        cursor: existingValueCursor,
                        indexName: indexName,
                        datastoreKey: self.key
                    )
                }
                
                /// Grab a cursor to insert the new value in the index.
                let updatedValueCursor = try await transaction.secondaryIndexCursor(
                    inserting: updatedValue.indexed,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: self.key
                )
                
                /// Insert it.
                try await transaction.persistSecondaryIndexEntry(
                    indexValue: updatedValue.indexed,
                    identifierValue: idenfifier,
                    cursor: updatedValueCursor,
                    indexName: indexName,
                    datastoreKey: self.key
                )
            }
            
            /// Remove any remaining indexed values from the old instance.
            try await Mirror.indexedChildren(from: existingInstance) { indexName, value in
                let indexName = IndexName(indexName)
                guard !queriedIndexes.contains(indexName) else { return }
                
                /// Grab a cursor to the old value in the index.
                let existingValueCursor = try await transaction.secondaryIndexCursor(
                    for: value,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: self.key
                )
                
                /// Delete it.
                try await transaction.deleteSecondaryIndexEntry(
                    cursor: existingValueCursor,
                    indexName: indexName,
                    datastoreKey: self.key
                )
            }
            
            /// Re-insert those indexes into the new index.
            try await Mirror.indexedChildren(from: instance, assertIdentifiable: true) { indexName, value in
                let indexName = IndexName(indexName)
                guard !queriedIndexes.contains(indexName) else { return }
                
                /// Grab a cursor to insert the new value in the index.
                let updatedValueCursor = try await transaction.secondaryIndexCursor(
                    inserting: value,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: self.key
                )
                
                /// Insert it.
                try await transaction.persistSecondaryIndexEntry(
                    indexValue: value,
                    identifierValue: idenfifier,
                    cursor: updatedValueCursor,
                    indexName: indexName,
                    datastoreKey: self.key
                )
            }
        }
    }
    
    /// Persist an instance for a given identifier, keyed to the specified path.
    ///
    /// If an instance does not already exist for the specified identifier, it will be created. If an instance already exists, it will be updated.
    /// - Note: If you instance conforms to Identifiable, it it preferable to use ``persist(_:)`` instead.
    /// - Parameters:
    ///   - instance: The instance to persist.
    ///   - keypath: The keypath the identifier is located at.
    public func persist(_ instance: CodedType, id keypath: KeyPath<CodedType, IdentifierType>) async throws {
        try await persist(instance, to: instance[keyPath: keypath])
    }
    
    @discardableResult
    public func delete(_ idenfifier: IdentifierType) async throws -> CodedType {
        try await warmupIfNeeded()
        
        return try await persistence._withTransaction(
            actionName: "Delete Entry",
            options: [.idempotent]
        ) { transaction, _ in
            /// Get a cursor to the entry within the primary index.
            let existingEntry = try await transaction.primaryIndexCursor(for: idenfifier, datastoreKey: self.key)
            
            /// Delete the instance at that cursor.
            try await transaction.deletePrimaryIndexEntry(cursor: existingEntry.cursor, datastoreKey: self.key)
            
            /// Load the instance completely so we can delete the entry within the direct and secondary indexes too.
            let existingVersion = try Version(existingEntry.versionData)
            let decoder = try await self.decoder(for: existingVersion)
            let existingInstance = try await decoder(existingEntry.instanceData).instance
            
            try await transaction.emit(
                event: .deleted(
                    id: idenfifier,
                    oldEntry: ObservationEntry(
                        versionData: existingEntry.versionData,
                        instanceData: existingEntry.instanceData
                    )
                ),
                datastoreKey: self.key
            )
            
            var queriedIndexes: Set<IndexName> = []
            
            /// Persist the direct indexes with full copies
            for indexPath in self.directIndexes {
                let indexName = indexPath.path
                guard !queriedIndexes.contains(indexName) else { continue }
                queriedIndexes.insert(indexName)
                
                let existingValue = existingInstance[keyPath: indexPath]
                
                /// Grab a cursor to the old value in the index.
                let existingValueCursor = try await transaction.directIndexCursor(
                    for: existingValue.indexed,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: self.key
                )
                
                /// Delete it.
                try await transaction.deleteDirectIndexEntry(
                    cursor: existingValueCursor.cursor,
                    indexName: indexName,
                    datastoreKey: self.key
                )
            }
            
            /// Next, go through any remaining computed indexes as secondary indexes.
            for indexPath in self.computedIndexes {
                let indexName = indexPath.path
                guard !queriedIndexes.contains(indexName) else { continue }
                queriedIndexes.insert(indexName)
                
                let existingValue = existingInstance[keyPath: indexPath]
                
                /// Grab a cursor to the old value in the index.
                let existingValueCursor = try await transaction.secondaryIndexCursor(
                    for: existingValue.indexed,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: self.key
                )
                
                /// Delete it.
                try await transaction.deleteSecondaryIndexEntry(
                    cursor: existingValueCursor,
                    indexName: indexName,
                    datastoreKey: self.key
                )
            }
            
            /// Remove any remaining indexed values from the old instance.
            try await Mirror.indexedChildren(from: existingInstance) { indexName, value in
                let indexName = IndexName(indexName)
                guard !queriedIndexes.contains(indexName) else { return }
                
                /// Grab a cursor to the old value in the index.
                let existingValueCursor = try await transaction.secondaryIndexCursor(
                    for: value,
                    identifier: idenfifier,
                    indexName: indexName,
                    datastoreKey: self.key
                )
                
                /// Delete it.
                try await transaction.deleteSecondaryIndexEntry(
                    cursor: existingValueCursor,
                    indexName: indexName,
                    datastoreKey: self.key
                )
            }
            
            return existingInstance
        }
    }
    
    /// A read-only view into the data store.
    // TODO: Make a proper copy here
    public var readOnly: Datastore<Version, CodedType, IdentifierType, ReadOnly> { self as Any as! Datastore<Version, CodedType, IdentifierType, ReadOnly> }
}

// MARK: Idetifiable CodedType

extension Datastore where CodedType: Identifiable, IdentifierType == CodedType.ID {
    /// Persist an instance to the data store.
    ///
    /// If an instance does not already exist for the specified identifier, it will be created. If an instance already exists, it will be updated.
    /// - Parameter instance: The instance to persist.
    public func persist(_ instance: CodedType) async throws where AccessMode == ReadWrite {
        try await self.persist(instance, to: instance.id)
    }
    
    public func delete(_ instance: CodedType) async throws where AccessMode == ReadWrite {
        try await self.delete(instance.id)
    }
    
    public func load(_ instance: CodedType) async throws -> CodedType? {
        try await self.load(instance.id)
    }
    
    public func observe(_ instance: CodedType) async throws -> some TypedAsyncSequence<ObservedEvent<IdentifierType, CodedType>> {
        try await observe(instance.id)
    }
}

// MARK: - JSON and Plist Stores

extension Datastore where AccessMode == ReadWrite {
    public static func JSONStore(
        persistence: some Persistence<AccessMode>,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> (id: IdentifierType, instance: CodedType)],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: identifierType,
            encoder: { try encoder.encode($0) },
            decoders: migrations.mapValues { migration in
                { data in try await migration(data, decoder) }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func propertyListStore(
        persistence: some Persistence<AccessMode>,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        outputFormat: PropertyListSerialization.PropertyListFormat = .binary,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> (id: IdentifierType, instance: CodedType)],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        let encoder = PropertyListEncoder()
        encoder.outputFormat = outputFormat
        
        let decoder = PropertyListDecoder()
        
        return self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: identifierType,
            encoder: { try encoder.encode($0) },
            decoders: migrations.mapValues { migration in
                { data in try await migration(data, decoder) }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
}

extension Datastore where AccessMode == ReadOnly {
    public static func readOnlyJSONStore(
        persistence: some Persistence,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> (id: IdentifierType, instance: CodedType)],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: identifierType,
            decoders: migrations.mapValues { migration in
                { data in try await migration(data, decoder) }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func readOnlyPropertyListStore(
        persistence: some Persistence,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> (id: IdentifierType, instance: CodedType)],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        let decoder = PropertyListDecoder()
        
        return self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: identifierType,
            decoders: migrations.mapValues { migration in
                { data in try await migration(data, decoder) }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
}

// MARK: - Identifiable CodedType Initializers

extension Datastore where CodedType: Identifiable, IdentifierType == CodedType.ID, AccessMode == ReadWrite {
    public init(
        persistence: some Persistence<AccessMode>,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        encoder: @escaping (_ object: CodedType) async throws -> Data,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            encoder: encoder,
            decoders: decoders.mapValues { decoder in
                { data in
                    let instance = try await decoder(data)
                    return (id: instance.id, instance: instance)
                }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func JSONStore(
        persistence: some Persistence<AccessMode>,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.JSONStore(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            encoder: encoder,
            decoder: decoder,
            migrations: migrations.mapValues { migration in
                { data, decoder in
                    let instance = try await migration(data, decoder)
                    return (id: instance.id, instance: instance)
                }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func propertyListStore(
        persistence: some Persistence<AccessMode>,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        outputFormat: PropertyListSerialization.PropertyListFormat = .binary,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.propertyListStore(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            outputFormat: outputFormat,
            migrations: migrations.mapValues { migration in
                { data, decoder in
                    let instance = try await migration(data, decoder)
                    return (id: instance.id, instance: instance)
                }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
}

extension Datastore where CodedType: Identifiable, IdentifierType == CodedType.ID, AccessMode == ReadOnly {
    public init(
        persistence: some Persistence,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            decoders: decoders.mapValues { decoder in
                { data in
                    let instance = try await decoder(data)
                    return (id: instance.id, instance: instance)
                }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func readOnlyJSONStore(
        persistence: some Persistence,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.readOnlyJSONStore(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            decoder: decoder,
            migrations: migrations.mapValues { migration in
                { data, decoder in
                    let instance = try await migration(data, decoder)
                    return (id: instance.id, instance: instance)
                }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func readOnlyPropertyListStore(
        persistence: some Persistence,
        key: DatastoreKey,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        computedIndexes: [IndexPath<CodedType, _AnyIndexed>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.readOnlyPropertyListStore(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            migrations: migrations.mapValues { migration in
                { data, decoder in
                    let instance = try await migration(data, decoder)
                    return (id: instance.id, instance: instance)
                }
            },
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
}

// MARK: - Helper Types

private enum TaskStatus {
    case waiting
    case inProgress(Task<Void, Error>)
    case complete
}
