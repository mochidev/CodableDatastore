//
//  Datastore.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A store for a homogenous collection of instances.
public actor Datastore<Format: DatastoreFormat, AccessMode: _AccessMode> {
    /// A type representing the version of the datastore within the persistence.
    ///
    /// - SeeAlso: ``DatastoreFormat/Version``
    public typealias Version = Format.Version
    
    /// The instance type to use when persisting and loading values from the datastore.
    ///
    /// - SeeAlso: ``DatastoreFormat/Instance``
    public typealias InstanceType = Format.Instance
    
    /// The identifier to be used when de-duplicating instances saved in the persistence.
    ///
    /// - SeeAlso: ``DatastoreFormat/Identifier``
    public typealias IdentifierType = Format.Identifier
    
    let persistence: any Persistence
    let format: Format
    let key: DatastoreKey
    let version: Version
    let encoder: (_ instance: InstanceType) async throws -> Data
    let decoders: [Version: (_ data: Data) async throws -> (id: IdentifierType, instance: InstanceType)]
    let indexRepresentations: [AnyIndexRepresentation<InstanceType> : GeneratedIndexRepresentation<InstanceType>]
    
    var updatedDescriptor: DatastoreDescriptor?
    
    fileprivate var warmupStatus: TaskStatus = .waiting
    fileprivate var warmupProgressHandlers: [ProgressHandler] = []
    
    fileprivate var storeMigrationStatus: TaskStatus = .waiting
    fileprivate var storeMigrationProgressHandlers: [ProgressHandler] = []
    
    fileprivate var indexMigrationStatus: [AnyIndexRepresentation<InstanceType> : TaskStatus] = [:]
    fileprivate var indexMigrationProgressHandlers: [AnyIndexRepresentation<InstanceType> : ProgressHandler] = [:]
    
    public init(
        persistence: some Persistence<AccessMode>,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        encoder: @escaping (_ instance: InstanceType) async throws -> Data,
        decoders: [Version: (_ data: Data) async throws -> (id: IdentifierType, instance: InstanceType)],
        configuration: Configuration = .init()
    ) where AccessMode == ReadWrite {
        self.persistence = persistence
        let format = Format()
        self.format = format
        self.indexRepresentations = format.generateIndexRepresentations(assertIdentifiable: true)
        self.key = key
        self.version = version
        self.encoder = encoder
        self.decoders = decoders
        
        var usedIndexNames: Set<IndexName> = []
        for (indexKey, indexRepresentation) in indexRepresentations {
            assert(!usedIndexNames.contains(indexRepresentation.indexName), "Index \"\(indexRepresentation.indexName.rawValue)\" (\(indexRepresentation.index.indexType.rawValue)) was used more than once, which will lead to undefined behavior on every run. Please make sure \(String(describing: Format.self)) only declares a single index for \"\(indexRepresentation.indexName.rawValue)\".")
            usedIndexNames.insert(indexRepresentation.indexName)
            
            assert(indexKey == AnyIndexRepresentation(indexRepresentation: indexRepresentation.index), "The key returned for index \"\(indexRepresentation.indexName.rawValue)\" does not match the generated representation. Please double check to make sure that these values are aligned!")
        }
        
        for decoderVersion in Version.allCases {
            guard decoders[decoderVersion] == nil else { continue }
            assertionFailure("Decoders missing case for \(decoderVersion). Please make sure you have a decoder configured for this version or you may encounter errors at runtime.")
        }
    }
    
    public init(
        persistence: some Persistence,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        decoders: [Version: (_ data: Data) async throws -> (id: IdentifierType, instance: InstanceType)],
        configuration: Configuration = .init()
    ) where AccessMode == ReadOnly {
        self.persistence = persistence
        let format = Format()
        self.format = format
        self.indexRepresentations = format.generateIndexRepresentations(assertIdentifiable: true)
        self.key = key
        self.version = version
        self.encoder = { _ in preconditionFailure("Encode called on read-only instance.") }
        self.decoders = decoders
        
        var usedIndexNames: Set<IndexName> = []
        for (indexKey, indexRepresentation) in indexRepresentations {
            assert(!usedIndexNames.contains(indexRepresentation.indexName), "Index \"\(indexRepresentation.indexName.rawValue)\" (\(indexRepresentation.index.indexType.rawValue)) was used more than once, which will lead to undefined behavior on every run. Please make sure \(String(describing: Format.self)) only declares a single index for \"\(indexRepresentation.indexName.rawValue)\".")
            usedIndexNames.insert(indexRepresentation.indexName)
            
            assert(indexKey == AnyIndexRepresentation(indexRepresentation: indexRepresentation.index), "The key returned for index \"\(indexRepresentation.indexName.rawValue)\" does not match the generated representation. Please double check to make sure that these values are aligned!")
        }
        
        for decoderVersion in Version.allCases {
            guard decoders[decoderVersion] == nil else { continue }
            assertionFailure("Decoders missing case for \(decoderVersion). Please make sure you have a decoder configured for this version or you may encounter errors at runtime.")
        }
    }
}

// MARK: - Helper Methods

extension Datastore {
    func generateUpdatedDescriptor() throws -> DatastoreDescriptor {
        if let updatedDescriptor {
            return updatedDescriptor
        }
        
        let descriptor = try DatastoreDescriptor(
            format: format,
            version: version
        )
        updatedDescriptor = descriptor
        return descriptor
    }
    
    func decoder(for version: Version) throws -> (_ data: Data) async throws -> (id: IdentifierType, instance: InstanceType) {
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
        guard let self = self as? Datastore<Format, ReadWrite>
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
        
        let primaryIndex = _load(IndexRange(), order: .ascending, awaitWarmup: false)
        
        var rebuildPrimaryIndex = false
        var directIndexesToBuild: Set<IndexName> = []
        var secondaryIndexesToBuild: Set<IndexName> = []
        var index = 0
        
        let versionData = try Data(self.version)
        
        for try await (idenfifier, instance) in primaryIndex {
            defer { index += 1 }
            /// Use the first index to grab an up-to-date descriptor
            if newDescriptor == nil {
                let updatedDescriptor = try generateUpdatedDescriptor()
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
                for (_, persistedIndex) in persistedDescriptor.referenceIndexes {
                    if let updatedIndex = updatedDescriptor.referenceIndexes[persistedIndex.name] {
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
                for (_, updatedIndex) in updatedDescriptor.referenceIndexes {
                    guard persistedDescriptor.referenceIndexes[updatedIndex.name] == nil else { continue }
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
            
            for (_, generatedRepresentation) in indexRepresentations {
                let indexName = generatedRepresentation.indexName
                switch generatedRepresentation.storage {
                case .direct:
                    guard
                        directIndexesToBuild.contains(indexName),
                        !queriedIndexes.contains(indexName)
                    else { return }
                    queriedIndexes.insert(indexName)
                    
                    for updatedValue in instance[index: generatedRepresentation.index] {
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
                case .reference:
                    guard
                        secondaryIndexesToBuild.contains(indexName),
                        !queriedIndexes.contains(indexName)
                    else { return }
                    queriedIndexes.insert(indexName)
                    
                    for updatedValue in instance[index: generatedRepresentation.index] {
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
                }
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
    /// Force a full migration of an index if the version persisted is less than the specified minimum version.
    ///
    /// Only use this if you must force an index to be re-calculated, which is sometimes necessary when the implementation of the compare method changes between releases.
    ///
    /// - Parameters:
    ///   - index: The index to migrate.
    ///   - minimumVersion: The minimum valid version for an index to not be migrated.
    ///   - progressHandler: A closure that will be regularly called with progress during the migration. If no migration needs to occur, it won't be called, so setup and tear down any UI within the handler.
    public func migrate<Index: IndexRepresentation<InstanceType>>(index: KeyPath<Format, Index>, ifLessThan minimumVersion: Version, progressHandler: ProgressHandler? = nil) async throws {
        try await persistence._withTransaction(
            actionName: "Migrate Entries",
            options: []
        ) { transaction, _ in
            guard
                /// If we have no descriptor, then no data exists to be migrated.
                let descriptor = try await transaction.datastoreDescriptor(for: self.key),
                descriptor.size > 0,
                /// If we didn't declare the index, we can't do anything. This is likely an error only encountered to self-implementers of ``DatastoreFormat``'s ``DatastoreFormat/generateIndexRepresentations``.
                let declaredIndex = self.indexRepresentations[AnyIndexRepresentation(indexRepresentation: self.format[keyPath: index])],
                /// If we don't have an index stored, there is nothing to do here. This means we can skip checking it on the type.
                let matchingDescriptor =
                    descriptor.directIndexes[declaredIndex.indexName.rawValue] ?? descriptor.referenceIndexes[declaredIndex.indexName.rawValue],
                /// We don't care in this method of the version is incompatible — the index will be discarded.
                let version = try? Version(matchingDescriptor.version),
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
                /// If we didn't declare the index, we can't do anything. This is likely an error only encountered to self-implementers of ``DatastoreFormat``'s ``DatastoreFormat/generateIndexRepresentations``.
                let declaredIndex = self.indexRepresentations[AnyIndexRepresentation(indexRepresentation: self.format[keyPath: index])],
                /// If we don't have an index stored, there is nothing to do here. This means we can skip checking it on the type.
                let matchingDescriptor =
                    descriptor.directIndexes[declaredIndex.indexName.rawValue] ?? descriptor.referenceIndexes[declaredIndex.indexName.rawValue],
                /// We don't care in this method of the version is incompatible — the index will be discarded.
                let version = try? Version(matchingDescriptor.version),
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
    
    func migrate<Index: IndexRepresentation<InstanceType>>(index: KeyPath<Format, Index>, progressHandler: ProgressHandler? = nil) async throws {
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
    /// - Note: This count may not reflect an up to date value while instances are being written concurrently, but will be acurate after such a transaction finishes.
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
    
    /// Load an instance with a given identifier, or return nil if one is not found.
    /// - Parameter identifier: The identifier of the instance to load.
    /// - Returns: The instance keyed to the identifier, or nil if none are found.
    public func load(_ identifier: IdentifierType) async throws -> InstanceType? {
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
            } catch DatastoreInterfaceError.datastoreKeyNotFound {
                /// There isn't a datastore yet, so no entries would exist either.
                return nil
            } catch {
                throw error
            }
        }
    }
    
    /// **Internal:** Load a range of instances from a datastore based on the identifier range passed in as an async sequence.
    /// - Parameters:
    ///   - identifierRange: The range to load.
    ///   - order: The order to process instances in.
    ///   - awaitWarmup: Whether the sequence should await warmup or jump right into loading.
    /// - Returns: An asynchronous sequence containing the instances matching the range of values in that sequence.
    nonisolated func _load(
        _ identifierRange: some IndexRangeExpression<IdentifierType>,
        order: RangeOrder,
        awaitWarmup: Bool
    ) -> some TypedAsyncSequence<(id: IdentifierType, instance: InstanceType)> {
        AsyncThrowingBackpressureStream { provider in
            if awaitWarmup {
                try await self.warmupIfNeeded()
            }
            
            try await self.persistence._withTransaction(
                actionName: nil,
                options: [.readOnly]
            ) { transaction, _ in
                do {
                    try await transaction.primaryIndexScan(range: identifierRange.applying(order), datastoreKey: self.key) { versionData, instanceData in
                        let entryVersion = try Version(versionData)
                        let decoder = try await self.decoder(for: entryVersion)
                        let decodedValue = try await decoder(instanceData)
                        
                        try await provider.yield(decodedValue)
                    }
                } catch DatastoreInterfaceError.datastoreKeyNotFound {
                    /// There isn't a datastore yet, so no entries would exist either. Do nothing and let the stream end.
                }
            }
        }
    }
    
    /// Load a range of instances from a datastore based on the identifier range passed in as an async sequence.
    ///
    /// The sequence should be consumed a single time, ideally within the same transaction it was created in as it holds a reference to that transaction and thus snapshot of the datastore for data consistency.
    /// - Parameters:
    ///   - identifierRange: The range to load.
    ///   - order: The order to process instances in.
    /// - Returns: An asynchronous sequence containing the instances matching the range of identifiers.
    public nonisolated func load(
        _ identifierRange: some IndexRangeExpression<IdentifierType>,
        order: RangeOrder = .ascending
    ) -> some TypedAsyncSequence<InstanceType> where IdentifierType: RangedIndexable {
        _load(identifierRange, order: order, awaitWarmup: true)
            .map { $0.instance }
    }
    
    /// Load a range of instances from a datastore based on the identifier range passed in as an async sequence.
    /// 
    /// The sequence should be consumed a single time, ideally within the same transaction it was created in as it holds a reference to that transaction and thus snapshot of the datastore for data consistency.
    /// - Parameters:
    ///   - identifierRange: The range to load.
    ///   - order: The order to process instances in.
    /// - Returns: An asynchronous sequence containing the instances matching the range of identifiers.
    @_disfavoredOverload
    public nonisolated func load(
        _ identifierRange: IndexRange<IdentifierType>,
        order: RangeOrder = .ascending
    ) -> some TypedAsyncSequence<InstanceType> where IdentifierType: RangedIndexable {
        load(identifierRange, order: order)
    }
    
    /// Load all instances in a datastore as an async sequence.
    ///
    /// The sequence should be consumed a single time, ideally within the same transaction it was created in as it holds a reference to that transaction and thus snapshot of the datastore for data consistency.
    /// - Parameters:
    ///   - unboundedRange: The range to load. Specify `...` to load every instance.
    ///   - order: The order to process instances in.
    /// - Returns: An asynchronous sequence containing all the instances.
    public nonisolated func load(
        _ unboundedRange: Swift.UnboundedRange,
        order: RangeOrder = .ascending
    ) -> some TypedAsyncSequence<InstanceType> {
        _load(IndexRange(), order: order, awaitWarmup: true)
            .map { $0.instance }
    }
    
    /// **Internal:** Load a range of instances from a given index as an async sequence.
    /// - Parameters:
    ///   - range: The range to load.
    ///   - order: The order to process instances in.
    ///   - index: The index to load from.
    /// - Returns: An asynchronous sequence containing the instances matching the range of values in that sequence.
    @usableFromInline
    nonisolated func _load<Index: IndexRepresentation<InstanceType>, Range: IndexRangeExpression>(
        _ range: Range,
        order: RangeOrder = .ascending,
        from index: KeyPath<Format, Index>
    ) -> some TypedAsyncSequence<InstanceType> where Range.Bound: Indexable {
        AsyncThrowingBackpressureStream { provider in
            guard let declaredIndex = self.indexRepresentations[AnyIndexRepresentation(indexRepresentation: self.format[keyPath: index])]
            else { throw DatastoreError.missingIndex }
            
            try await self.warmupIfNeeded()
            
            try await self.persistence._withTransaction(
                actionName: nil,
                options: [.readOnly]
            ) { transaction, _ in
                do {
                    switch declaredIndex.storage {
                    case .direct:
                        try await transaction.directIndexScan(
                            range: range.applying(order),
                            indexName: declaredIndex.indexName,
                            datastoreKey: self.key
                        ) { versionData, instanceData in
                            let entryVersion = try Version(versionData)
                            let decoder = try await self.decoder(for: entryVersion)
                            let instance = try await decoder(instanceData).instance
                            
                            try await provider.yield(instance)
                        }
                    case .reference:
                        try await transaction.secondaryIndexScan(
                            range: range.applying(order),
                            indexName: declaredIndex.indexName,
                            datastoreKey: self.key
                        ) { (identifier: IdentifierType) in
                            let persistedEntry = try await transaction.primaryIndexCursor(for: identifier, datastoreKey: self.key)
                            
                            let entryVersion = try Version(persistedEntry.versionData)
                            let decoder = try await self.decoder(for: entryVersion)
                            let instance = try await decoder(persistedEntry.instanceData).instance
                            
                            try await provider.yield(instance)
                        }
                    }
                } catch DatastoreInterfaceError.datastoreKeyNotFound {
                    /// There isn't a datastore yet, so no entries would exist either. Do nothing and let the stream end.
                }
            }
        }
    }
    
    /// Load all instances with the matching indexed value as an async sequence.
    ///
    /// This is conceptually similar to loading all instances and filtering only those who's indexed key path matches the specified value, but is much more efficient as an index is already maintained for that value.
    ///
    /// The sequence should be consumed a single time, ideally within the same transaction it was created in as it holds a reference to that transaction and thus snapshot of the datastore for data consistency.
    /// - Parameters:
    ///   - value: The value to match against.
    ///   - order: The order to process instances in.   
    ///   - index: The index to load from.
    /// - Returns: An asynchronous sequence containing the instances matching the specified indexed value.
    public nonisolated func load<
        Value: DiscreteIndexable,
        Index: RetrievableIndexRepresentation<InstanceType, Value>
    >(
        _ value: Index.Value,
        order: RangeOrder = .ascending,
        from index: KeyPath<Format, Index>
    ) -> some TypedAsyncSequence<InstanceType> {
        _load(IndexRange(only: value), order: order, from: index)
    }
    
    /// Load an instance with the matching indexed value, or return nil if one is not found.
    ///
    /// This requires either a ``DatastoreFormat/OneToOneIndex`` or ``DatastoreFormat/ManyToOneIndex`` to be declared as the index, and a guarantee on the caller's part that at most only a single instance will match the specified value. If multiple instancess match, the one with the identifier that sorts first will be returned.
    /// - Parameters:
    ///   - value: The value to match against.
    ///   - index: The index to load from.
    /// - Returns: The instance keyed to the specified indexed value, or nil if none are found.
    public nonisolated func load<
        Value,
        Index: SingleInstanceIndexRepresentation<InstanceType, Value>
    >(
        _ value: Index.Value,
        from index: KeyPath<Format, Index>
    ) async throws -> InstanceType? {
        try await _load(IndexRange(only: value), from: index).first(where: { _ in true })
    }
    
    /// Load a range of instances from a given index as an async sequence.
    ///
    /// This is conceptually similar to loading all instances and filtering only those who's indexed key path matches the specified range, but is much more efficient as an index is already maintained for that range of values.
    ///
    /// The sequence should be consumed a single time, ideally within the same transaction it was created in as it holds a reference to that transaction and thus snapshot of the datastore for data consistency.
    /// - Parameters:
    ///   - range: The range to load.
    ///   - order: The order to process instances in.
    ///   - index: The index to load from.
    /// - Returns: An asynchronous sequence containing the instances matching the range of values in that sequence.
    public nonisolated func load<
        Value: RangedIndexable,
        Index: RetrievableIndexRepresentation<InstanceType, Value>
    >(
        _ range: some IndexRangeExpression<Value>,
        order: RangeOrder = .ascending,
        from index: KeyPath<Format, Index>
    ) -> some TypedAsyncSequence<InstanceType> {
        _load(range, order: order, from: index)
    }
    
    /// Load a range of instances from a given index as an async sequence.
    ///
    /// This is conceptually similar to loading all instances and filtering only those who's indexed key path matches the specified range, but is much more efficient as an index is already maintained for that range of values.
    ///
    /// The sequence should be consumed a single time, ideally within the same transaction it was created in as it holds a reference to that transaction and thus snapshot of the datastore for data consistency.
    /// - Parameters:
    ///   - range: The range to load.
    ///   - order: The order to process instances in.
    ///   - index: The index to load from.
    /// - Returns: An asynchronous sequence containing the instances matching the range of values in that sequence.
    @_disfavoredOverload
    public nonisolated func load<
        Value: RangedIndexable,
        Index: RetrievableIndexRepresentation<InstanceType, Value>
    >(
        _ range: IndexRange<Value>,
        order: RangeOrder = .ascending,
        from index: KeyPath<Format, Index>
    ) -> some TypedAsyncSequence<InstanceType> {
        _load(range, order: order, from: index)
    }
    
    /// Load all instances in a datastore in index order as an async sequence.
    ///
    /// The sequence should be consumed a single time, ideally within the same transaction it was created in as it holds a reference to that transaction and thus snapshot of the datastore for data consistency.
    ///
    /// - Note: If the index is a Mant-to-Any type of index, a smaller or larger number of results may be returned here, as some instances may not be respresented in the index, while others are other-represented and may show up multiple times.
    /// - Parameters:
    ///   - unboundedRange: The range to load. Specify `...` to load every instance.
    ///   - order: The order to process instances in.
    ///   - index: The index to load from.
    /// - Returns: An asynchronous sequence containing all the instances, ordered by the specified index.
    public nonisolated func load<Index: IndexRepresentation<InstanceType>>(
        _ unboundedRange: Swift.UnboundedRange,
        order: RangeOrder = .ascending,
        from index: KeyPath<Format, Index>
    ) -> some TypedAsyncSequence<InstanceType> {
        _load(IndexRange.unbounded, order: order, from: index)
    }
}

// MARK: - Observation

extension Datastore {
    public func observe(_ idenfifier: IdentifierType) async throws -> some TypedAsyncSequence<ObservedEvent<IdentifierType, InstanceType>> {
        try await self.observe()
            .filter { $0.id == idenfifier }
    }
    
    public func observe() async throws -> some TypedAsyncSequence<ObservedEvent<IdentifierType, InstanceType>> {
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
    @discardableResult
    public func persist(_ instance: InstanceType, to idenfifier: IdentifierType) async throws -> InstanceType? {
        try await warmupIfNeeded()
        
        let updatedDescriptor = try self.generateUpdatedDescriptor()
        let versionData = try Data(self.version)
        let instanceData = try await self.encoder(instance)
        
        return try await persistence._withTransaction(
            actionName: "Persist Entry",
            options: [.idempotent]
        ) { transaction, _ in
            /// Create any missing indexes or prime the datastore for writing.
            try await transaction.apply(descriptor: updatedDescriptor, for: self.key)
            
            let existingEntry: (cursor: any InstanceCursorProtocol, instance: InstanceType, versionData: Data, instanceData: Data)? = try await {
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
            
            for (_, generatedRepresentation) in self.indexRepresentations {
                let indexName = generatedRepresentation.indexName
                guard !queriedIndexes.contains(indexName) else { continue }
                queriedIndexes.insert(indexName)
                
                switch generatedRepresentation.storage {
                case .direct:
                    /// Persist the direct indexes with full copies
                    for existingValue in existingInstance?[index: generatedRepresentation.index] ?? [] {
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
                    
                    for updatedValue in instance[index: generatedRepresentation.index] {
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
                case .reference:
                    /// Persist the reference indexes with identifiers only
                    for existingValue in existingInstance?[index: generatedRepresentation.index] ?? [] {
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
                    
                    for updatedValue in instance[index: generatedRepresentation.index] {
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
                }
            }
            
            return existingInstance
        }
    }
    
    /// Persist an instance for a given identifier, keyed to the specified path.
    ///
    /// If an instance does not already exist for the specified identifier, it will be created. If an instance already exists, it will be updated.
    /// - Note: If you instance conforms to Identifiable, it it preferable to use ``persist(_:)`` instead.
    /// - Parameters:
    ///   - instance: The instance to persist.
    ///   - keypath: The keypath the identifier is located at.
    @discardableResult
    public func persist(_ instance: InstanceType, id keypath: KeyPath<InstanceType, IdentifierType>) async throws -> InstanceType? {
        try await persist(instance, to: instance[keyPath: keypath])
    }
    
    @discardableResult
    public func delete(_ idenfifier: IdentifierType) async throws -> InstanceType {
        guard let deletedInstance = try await deleteIfPresent(idenfifier)
        else { throw DatastoreInterfaceError.instanceNotFound }
        return deletedInstance
    }
    
    @discardableResult
    public func deleteIfPresent(_ idenfifier: IdentifierType) async throws -> InstanceType? {
        try await warmupIfNeeded()
        
        return try await persistence._withTransaction(
            actionName: "Delete Entry",
            options: [.idempotent]
        ) { transaction, _ in
            /// Get a cursor to the entry within the primary index.
            let existingEntry: (cursor: any InstanceCursorProtocol, instanceData: Data, versionData: Data)
            do {
                existingEntry = try await transaction.primaryIndexCursor(for: idenfifier, datastoreKey: self.key)
            } catch DatastoreInterfaceError.instanceNotFound {
                return nil
            } catch DatastoreInterfaceError.datastoreKeyNotFound {
                /// There isn't a datastore yet, so no entries would exist either.
                return nil
            } catch {
                throw error
            }
            
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
            
            for (_, generatedRepresentation) in self.indexRepresentations {
                let indexName = generatedRepresentation.indexName
                guard !queriedIndexes.contains(indexName) else { continue }
                queriedIndexes.insert(indexName)
                
                switch generatedRepresentation.storage {
                case .direct:
                    for existingValue in existingInstance[index: generatedRepresentation.index] {
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
                case .reference:
                    for existingValue in existingInstance[index: generatedRepresentation.index] {
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
                }
            }
            
            return existingInstance
        }
    }
    
    /// A read-only view into the data store.
    // TODO: Make a proper copy here
    public var readOnly: Datastore<Format, ReadOnly> {
        Datastore<Format, ReadOnly>(
            persistence: persistence,
            format: Format.self,
            key: key,
            version: version,
            decoders: decoders
//            configuration: configuration // TODO: Copy configuration here
        )
    }
}

// MARK: Identifiable InstanceType

extension Datastore where InstanceType: Identifiable, IdentifierType == InstanceType.ID {
    /// Persist an instance to the data store.
    ///
    /// If an instance does not already exist for the specified identifier, it will be created. If an instance already exists, it will be updated.
    /// - Parameter instance: The instance to persist.
    @_disfavoredOverload
    @discardableResult
    public func persist(_ instance: InstanceType) async throws -> InstanceType? where AccessMode == ReadWrite {
        try await self.persist(instance, to: instance.id)
    }
    
    @_disfavoredOverload
    @discardableResult
    public func delete(_ instance: InstanceType) async throws -> InstanceType where AccessMode == ReadWrite {
        try await self.delete(instance.id)
    }
    
    @_disfavoredOverload
    @discardableResult
    public func deleteIfPresent(_ instance: InstanceType) async throws -> InstanceType? where AccessMode == ReadWrite {
        try await self.deleteIfPresent(instance.id)
    }
    
    @_disfavoredOverload
    public func load(_ instance: InstanceType) async throws -> InstanceType? {
        try await self.load(instance.id)
    }
    
    @_disfavoredOverload
    public func observe(_ instance: InstanceType) async throws -> some TypedAsyncSequence<ObservedEvent<IdentifierType, InstanceType>> {
        try await observe(instance.id)
    }
}

// MARK: - JSON and Plist Stores

extension Datastore where AccessMode == ReadWrite {
    public static func JSONStore(
        persistence: some Persistence<AccessMode>,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> (id: IdentifierType, instance: InstanceType)],
        configuration: Configuration = .init()
    ) -> Self {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            encoder: { try encoder.encode($0) },
            decoders: migrations.mapValues { migration in
                { data in try await migration(data, decoder) }
            },
            configuration: configuration
        )
    }
    
    public static func propertyListStore(
        persistence: some Persistence<AccessMode>,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        outputFormat: PropertyListSerialization.PropertyListFormat = .binary,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> (id: IdentifierType, instance: InstanceType)],
        configuration: Configuration = .init()
    ) -> Self {
        let encoder = PropertyListEncoder()
        encoder.outputFormat = outputFormat
        
        let decoder = PropertyListDecoder()
        
        return self.init(
            persistence: persistence,
            key: key,
            version: version,
            encoder: { try encoder.encode($0) },
            decoders: migrations.mapValues { migration in
                { data in try await migration(data, decoder) }
            },
            configuration: configuration
        )
    }
}

extension Datastore where AccessMode == ReadOnly {
    public static func readOnlyJSONStore(
        persistence: some Persistence,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> (id: IdentifierType, instance: InstanceType)],
        configuration: Configuration = .init()
    ) -> Self {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            decoders: migrations.mapValues { migration in
                { data in try await migration(data, decoder) }
            },
            configuration: configuration
        )
    }
    
    public static func readOnlyPropertyListStore(
        persistence: some Persistence,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> (id: IdentifierType, instance: InstanceType)],
        configuration: Configuration = .init()
    ) -> Self {
        let decoder = PropertyListDecoder()
        
        return self.init(
            persistence: persistence,
            key: key,
            version: version,
            decoders: migrations.mapValues { migration in
                { data in try await migration(data, decoder) }
            },
            configuration: configuration
        )
    }
}

// MARK: - Identifiable InstanceType Initializers

extension Datastore where InstanceType: Identifiable, IdentifierType == InstanceType.ID, AccessMode == ReadWrite {
    public init(
        persistence: some Persistence<AccessMode>,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        encoder: @escaping (_ object: InstanceType) async throws -> Data,
        decoders: [Version: (_ data: Data) async throws -> InstanceType],
        configuration: Configuration = .init()
    ) {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            encoder: encoder,
            decoders: decoders.mapValues { decoder in
                { data in
                    let instance = try await decoder(data)
                    return (id: instance.id, instance: instance)
                }
            },
            configuration: configuration
        )
    }
    
    public static func JSONStore(
        persistence: some Persistence<AccessMode>,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> InstanceType],
        configuration: Configuration = .init()
    ) -> Self {
        self.JSONStore(
            persistence: persistence,
            key: key,
            version: version,
            encoder: encoder,
            decoder: decoder,
            migrations: migrations.mapValues { migration in
                { data, decoder in
                    let instance = try await migration(data, decoder)
                    return (id: instance.id, instance: instance)
                }
            },
            configuration: configuration
        )
    }
    
    public static func propertyListStore(
        persistence: some Persistence<AccessMode>,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        outputFormat: PropertyListSerialization.PropertyListFormat = .binary,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> InstanceType],
        configuration: Configuration = .init()
    ) -> Self {
        self.propertyListStore(
            persistence: persistence,
            key: key,
            version: version,
            outputFormat: outputFormat,
            migrations: migrations.mapValues { migration in
                { data, decoder in
                    let instance = try await migration(data, decoder)
                    return (id: instance.id, instance: instance)
                }
            },
            configuration: configuration
        )
    }
}

extension Datastore where InstanceType: Identifiable, IdentifierType == InstanceType.ID, AccessMode == ReadOnly {
    public init(
        persistence: some Persistence,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        decoders: [Version: (_ data: Data) async throws -> InstanceType],
        configuration: Configuration = .init()
    ) {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            decoders: decoders.mapValues { decoder in
                { data in
                    let instance = try await decoder(data)
                    return (id: instance.id, instance: instance)
                }
            },
            configuration: configuration
        )
    }
    
    public static func readOnlyJSONStore(
        persistence: some Persistence,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> InstanceType],
        configuration: Configuration = .init()
    ) -> Self {
        self.readOnlyJSONStore(
            persistence: persistence,
            key: key,
            version: version,
            decoder: decoder,
            migrations: migrations.mapValues { migration in
                { data, decoder in
                    let instance = try await migration(data, decoder)
                    return (id: instance.id, instance: instance)
                }
            },
            configuration: configuration
        )
    }
    
    public static func readOnlyPropertyListStore(
        persistence: some Persistence,
        format: Format.Type = Format.self,
        key: DatastoreKey = Format.defaultKey,
        version: Version = Format.currentVersion,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> InstanceType],
        configuration: Configuration = .init()
    ) -> Self {
        self.readOnlyPropertyListStore(
            persistence: persistence,
            key: key,
            version: version,
            migrations: migrations.mapValues { migration in
                { data, decoder in
                    let instance = try await migration(data, decoder)
                    return (id: instance.id, instance: instance)
                }
            },
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
