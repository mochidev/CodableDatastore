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
        encoder: (_ instance: CodedType) async throws -> Data,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [IndexPath<CodedType>] = [],
        computedIndexes: [IndexPath<CodedType>] = [],
        configuration: Configuration = .init()
    ) where AccessMode == ReadWrite {
        self.persistence = persistence
        self.key = key
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
    }
    
    /// Migrates and warms the data store ahead of time.
    ///
    /// It is recommended you call this method before accessing any data, as it will offer you an opportunity to show a loading screen during potentially long migrations, rather than leaving it for the first read or write on the data store.
    ///
    /// - Parameter progressHandler: A closure that will be regularly called with progress during the migration. If no migration needs to occur, it won't be called, so setup and tear down any UI within the handler.
    public func warm(progressHandler: @escaping ProgressHandler = { _ in }) async throws {
        try await warmupIfNeeded(progressHandler: progressHandler)
    }
    
    func warmupIfNeeded(progressHandler: @escaping ProgressHandler = { _ in }) async throws {
        switch warmupStatus {
        case .complete: return
        case .inProgress(let task):
            warmupProgressHandlers.append(progressHandler)
            try await task.value
        case .waiting:
            let warmupTask = Task {
                let descriptor = try await persistence.register(datastore: self)
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

extension Datastore where AccessMode == ReadWrite {
    /// Manually migrate an index if the version persisted is less than a given minimum version.
    /// 
    /// Only use this if you must force an index to be re-calculated, which is sometimes necessary when the implementation of the compare method changes between releases.
    ///
    /// - Parameters:
    ///   - index: The index to migrate.
    ///   - minimumVersion: The minimum valid version for an index to not be migrated.
    ///   - progressHandler: A closure that will be regularly called with progress during the migration. If no migration needs to occur, it won't be called, so setup and tear down any UI within the handler.
    public func migrate(index: IndexPath<CodedType>, ifLessThan minimumVersion: Version, progressHandler: @escaping ProgressHandler = { _ in }) async throws {
        guard
            /// If we have no descriptor, then no data exists to be migrated.
            let descriptor = try await persistence.datastoreDescriptor(for: self),
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
            progressHandler(progress.adding(current: 0, total: descriptor.size))
        }
        
        /// Make sure we still need to do the work, as the warm up may have made changes anyways due to incompatible types.
        guard
            /// If we have no descriptor, then no data exists to be migrated.
            let descriptor = try await persistence.datastoreDescriptor(for: self),
            descriptor.size > 0,
            /// If we don't have an index stored, there is nothing to do here. This means we can skip checking it on the type.
            let matchingIndex = descriptor.directIndexes[index.path] ?? descriptor.secondaryIndexes[index.path],
            /// We don't care in this method of the version is incompatible — the index will be discarded.
            let version = try? Version(matchingIndex.version),
            /// Make sure the stored version is smaller than the one we require, otherwise stop early.
            version.rawValue < minimumVersion.rawValue
        else {
            progressHandler(warmUpProgress.adding(current: descriptor.size, total: descriptor.size))
            return
        }
        
        try await migrate(index: index) { migrateProgress in
            progressHandler(warmUpProgress.adding(migrateProgress))
        }
    }
    
    func migrate(index: IndexPath<CodedType>, progressHandler: @escaping ProgressHandler = { _ in }) async throws {
        // TODO: Migrate just that index, use indexMigrationStatus and indexMigrationProgressHandlers to record progress.
    }
    
    /// Manually migrate the entire store if the primary index version persisted is less than a given minimum version.
    ///
    /// Only use this if you must force the entire store to be re-calculated, which is sometimes necessary when the implementation of the `IdentifierType`'s compare method changes between releases.
    ///
    /// - Parameters:
    ///   - minimumVersion: The minimum valid version for an index to not be migrated.
    ///   - progressHandler: A closure that will be regularly called with progress during the migration. If no migration needs to occur, it won't be called, so setup and tear down any UI within the handler.
    public func migrateEntireStore(ifLessThan minimumVersion: Version, progressHandler: @escaping ProgressHandler = { _ in }) async throws {
        // TODO: Like the method above, check the description to see if a migration is needed
    }
    
    func migrateEntireStore(progressHandler: @escaping ProgressHandler = { _ in }) async throws {
        // TODO: Migrate all indexes, use storeMigrationStatus and storeMigrationProgressHandlers to record progress.
    }
}

extension Datastore {
    public func load(_ idenfifier: IdentifierType) async throws -> CodedType? {
        return nil
    }
    
    public func load(_ range: any IndexRangeExpression<IdentifierType>) async throws -> AsyncStream<CodedType> {
        return AsyncStream<CodedType> { continuation in
            continuation.finish()
        }
    }
}

public enum Observation<CodedType, IdentifierType> {
    case create(value: CodedType, identifier: IdentifierType)
    case update(oldValue: CodedType, newValue: CodedType, identifier: IdentifierType)
    case delete(value: CodedType, identifier: IdentifierType)
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

extension Datastore where AccessMode == ReadWrite {
    public func persist(_ instance: CodedType, to idenfifier: IdentifierType) async throws {
        
    }
    
    public func delete(_ idenfifier: IdentifierType) async throws {
        
    }
    
    /// A read-only view into the data store.
    // TODO: Make a proper copy here
    public var readOnly: Datastore<Version, CodedType, IdentifierType, ReadOnly> { self as Any as! Datastore<Version, CodedType, IdentifierType, ReadOnly> }
}

extension Datastore {
    public func load<IndexedValue>(
        _ range: any IndexRangeExpression<IdentifierType>,
        from keypath: KeyPath<CodedType, Indexed<IndexedValue>>
    ) async throws -> AsyncStream<CodedType> {
        return AsyncStream<CodedType> { continuation in
            continuation.finish()
        }
    }
}

extension Datastore where CodedType: Identifiable, IdentifierType == CodedType.ID {
    public func persist(_ instance: CodedType) async throws where AccessMode == ReadWrite {
        try await self.persist(instance, to: instance.id)
    }
    
    func delete(_ instance: CodedType) async throws where AccessMode == ReadWrite {
        try await self.delete(instance.id)
    }
    
    func load(_ instance: CodedType) async throws -> CodedType? {
        try await self.load(instance.id)
    }
    
    public func observe(_ instance: CodedType) -> AsyncStream<Observation<CodedType, IdentifierType>> {
        return AsyncStream<Observation<CodedType, IdentifierType>> { continuation in
            continuation.finish()
        }
    }
}

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

extension Datastore where CodedType: Identifiable, IdentifierType == CodedType.ID, AccessMode == ReadWrite {
    public init(
        persistence: some Persistence<AccessMode>,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        encoder: (_ object: CodedType) async throws -> Data,
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
