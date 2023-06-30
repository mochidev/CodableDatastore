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
    let key: String
    let version: Version
    let encoder: (_ instance: CodedType) async throws -> Data
    let decoders: [Version: (_ data: Data) async throws -> CodedType]
    let directIndexes: [IndexPath<CodedType>]
    let computedIndexes: [IndexPath<CodedType>]
    
    var updatedDescriptor: DatastoreDescriptor?
    
    fileprivate var warmupStatus: TaskStatus = .waiting
    fileprivate var warmupProgressHandlers: [ProgressHandler] = []
    
    fileprivate var storeMigrationStatus: TaskStatus = .waiting
    fileprivate var storeMigrationProgressHandlers: [ProgressHandler] = []
    
    fileprivate var indexMigrationStatus: [IndexPath<CodedType> : TaskStatus] = [:]
    fileprivate var indexMigrationProgressHandlers: [IndexPath<CodedType> : ProgressHandler] = [:]
    
    public init(
        persistence: some Persistence<AccessMode>,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        encoder: @escaping (_ instance: CodedType) async throws -> Data,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) where AccessMode == ReadWrite {
        self.persistence = persistence
        self.key = key
        self.version = version
        self.encoder = encoder
        self.decoders = decoders
        self.directIndexes = directIndexes
        self.computedIndexes = computedIndexes
    }
    
    public init(
        persistence: some Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
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
    
    func decoder(for version: Version) throws -> (_ data: Data) async throws -> CodedType {
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
                let descriptor = try await persistence._datastoreInterface.register(datastore: self)
                print("\(String(describing: descriptor))")
                
                /// Only operate on read-write datastores beyond this point.
                guard let self = self as? Datastore<Version, CodedType, IdentifierType, ReadWrite> else { return }
                print("\(self)")
                
                for handler in warmupProgressHandlers {
                    handler(.evaluating)
                }
                
                // TODO: Migrate any incompatible indexes by calling the internal methods below as needed.
                await Task.yield() // The "work"
                
                for handler in warmupProgressHandlers {
                    handler(.complete(total: 0))
                }
                
                warmupProgressHandlers.removeAll()
                warmupStatus = .complete
            }
            warmupStatus = .inProgress(warmupTask)
            try await warmupTask.value
        }
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
    public func migrate(index: IndexPath<CodedType>, ifLessThan minimumVersion: Version, progressHandler: ProgressHandler? = nil) async throws {
        guard
            /// If we have no descriptor, then no data exists to be migrated.
            let descriptor = try await persistence._datastoreInterface.datastoreDescriptor(for: self),
            descriptor.size > 0,
            /// If we don't have an index stored, there is nothing to do here. This means we can skip checking it on the type.
            let matchingIndex = descriptor.directIndexes[index.path] ?? descriptor.secondaryIndexes[index.path],
            /// We don't care in this method of the version is incompatible — the index will be discarded.
            let version = try? Version(matchingIndex.version),
            /// Make sure the stored version is smaller than the one we require, otherwise stop early.
            version.rawValue < minimumVersion.rawValue
        else { return }
        
        var warmUpProgress: Progress = .complete(total: 0)
        try await warmupIfNeeded { progress in
            warmUpProgress = progress
            progressHandler?(progress.adding(current: 0, total: descriptor.size))
        }
        
        /// Make sure we still need to do the work, as the warm up may have made changes anyways due to incompatible types.
        guard
            /// If we have no descriptor, then no data exists to be migrated.
            let descriptor = try await persistence._datastoreInterface.datastoreDescriptor(for: self),
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
        
        try await migrate(index: index) { migrateProgress in
            progressHandler?(warmUpProgress.adding(migrateProgress))
        }
    }
    
    func migrate(index: IndexPath<CodedType>, progressHandler: ProgressHandler? = nil) async throws {
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
    public func load(_ idenfifier: IdentifierType) async throws -> CodedType? {
        return nil
    }
    
    public func load(_ range: any IndexRangeExpression<IdentifierType>) async throws -> AsyncStream<CodedType> {
        return AsyncStream<CodedType> { continuation in
            continuation.finish()
        }
    }
    
    public func load<IndexedValue>(
        _ range: any IndexRangeExpression<IndexedValue>,
        from keypath: KeyPath<CodedType, Indexed<IndexedValue>>
    ) async throws -> AsyncStream<CodedType> {
        return AsyncStream<CodedType> { continuation in
            continuation.finish()
        }
    }
}

// MARK: - Observation

public enum Observation<CodedType, IdentifierType> {
    case created(value: CodedType, identifier: IdentifierType)
    case updated(oldValue: CodedType, newValue: CodedType, identifier: IdentifierType)
    case deleted(value: CodedType, identifier: IdentifierType)
}

extension Datastore {
    public func observe(_ idenfifier: IdentifierType) -> AsyncStream<Observation<CodedType, IdentifierType>> {
        return AsyncStream<Observation<CodedType, IdentifierType>> { continuation in
            continuation.finish()
        }
    }
    
    public func observe() -> AsyncStream<CodedType> {
        return AsyncStream<CodedType> { continuation in
            continuation.finish()
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
        
        try await persistence._datastoreInterface.withTransaction(options: [.idempotent]) { transaction in
            /// Create any missing indexes or prime the datastore for writing.
            try await transaction.apply(descriptor: updatedDescriptor, for: self.key)
            
            let existingEntry: (cursor: any InstanceCursorProtocol, instance: CodedType)? = try await {
                do {
                    let existingEntry = try await transaction.primaryIndexCursor(for: idenfifier, datastoreKey: self.key)
                    
                    let existingVersion = try Version(existingEntry.versionData)
                    let decoder = try self.decoder(for: existingVersion)
                    let existingInstance = try await decoder(existingEntry.instanceData)
                    
                    return (cursor: existingEntry.cursor, instance: existingInstance)
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
            
            /// Persist the entry in the primary index
            try await transaction.persistPrimaryIndexEntry(
                versionData: versionData,
                identifierValue: idenfifier,
                instanceData: instanceData,
                cursor: insertionCursor,
                datastoreKey: self.key
            )
            
            var queriedIndexes: Set<String> = []
            
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
            
            /// Re-insert those indexes from the new index.
            try await Mirror.indexedChildren(from: instance, assertIdentifiable: true) { indexName, value in
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
    
    public func delete(_ idenfifier: IdentifierType) async throws {
        
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
    
    public func observe(_ instance: CodedType) -> AsyncStream<Observation<CodedType, IdentifierType>> {
        return AsyncStream<Observation<CodedType, IdentifierType>> { continuation in
            continuation.finish()
        }
    }
}

// MARK: - JSON and Plist Stores

extension Datastore where AccessMode == ReadWrite {
    public static func JSONStore(
        persistence: some Persistence<AccessMode>,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: identifierType,
            encoder: { try encoder.encode($0) },
            decoders: migrations.mapValues({ migration in
                return { data in
                    return try await migration(data, decoder)
                }
            }),
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func propertyListStore(
        persistence: some Persistence<AccessMode>,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        outputFormat: PropertyListSerialization.PropertyListFormat = .binary,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
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
            decoders: migrations.mapValues({ migration in
                return { data in
                    return try await migration(data, decoder)
                }
            }),
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
}

extension Datastore where AccessMode == ReadOnly {
    public static func readOnlyJSONStore(
        persistence: some Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: identifierType,
            decoders: migrations.mapValues({ migration in
                return { data in
                   return try await migration(data, decoder)
                }
            }),
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func readOnlyPropertyListStore(
        persistence: some Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        let decoder = PropertyListDecoder()
        
        return self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: identifierType,
            decoders: migrations.mapValues({ migration in
                return { data in
                   return try await migration(data, decoder)
                }
            }),
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
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        encoder: @escaping (_ object: CodedType) async throws -> Data,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            encoder: encoder,
            decoders: decoders,
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func JSONStore(
        persistence: some Persistence<AccessMode>,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
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
            migrations: migrations,
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func propertyListStore(
        persistence: some Persistence<AccessMode>,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        outputFormat: PropertyListSerialization.PropertyListFormat = .binary,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.propertyListStore(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            outputFormat: outputFormat,
            migrations: migrations,
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
}

extension Datastore where CodedType: Identifiable, IdentifierType == CodedType.ID, AccessMode == ReadOnly {
    public init(
        persistence: some Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) {
        self.init(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            decoders: decoders,
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func readOnlyJSONStore(
        persistence: some Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.readOnlyJSONStore(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            decoder: decoder,
            migrations: migrations,
            directIndexes: directIndexes,
            computedIndexes: computedIndexes,
            configuration: configuration
        )
    }
    
    public static func readOnlyPropertyListStore(
        persistence: some Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) -> Self {
        self.readOnlyPropertyListStore(
            persistence: persistence,
            key: key,
            version: version,
            codedType: codedType,
            identifierType: codedType.ID.self,
            migrations: migrations,
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
