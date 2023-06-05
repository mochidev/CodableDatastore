//
//  Datastore.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

public struct Datastore<
    Version: RawRepresentable & Hashable & CaseIterable,
    CodedType: Codable,
    IdentifierType: Indexable,
    AccessMode: _AccessMode
> where Version.RawValue: Indexable {
    public init(
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        encoder: (_ instance: CodedType) async throws -> Data,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        configuration: Configuration = .init()
    ) where AccessMode == ReadWrite {
        
    }
    
    public init(
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        configuration: Configuration = .init()
    ) where AccessMode == ReadOnly {
        
    }
    
    /// Migrates and warms the data store ahead of time
    public func warm() async throws {
        
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
}

extension Datastore where AccessMode == ReadWrite {
    public static func JSONStore(
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
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
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        outputFormat: PropertyListSerialization.PropertyListFormat = .binary,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
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
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
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
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        identifierType: IdentifierType.Type,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
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
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        encoder: (_ object: CodedType) async throws -> Data,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        configuration: Configuration = .init()
    ) where AccessMode == ReadWrite {
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
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
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
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        outputFormat: PropertyListSerialization.PropertyListFormat = .binary,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
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
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        decoders: [Version: (_ data: Data) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        configuration: Configuration = .init()
    ) where AccessMode == ReadOnly {
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
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        decoder: JSONDecoder = JSONDecoder(),
        migrations: [Version: (_ data: Data, _ decoder: JSONDecoder) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
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
        persistence: any Persistence,
        key: String,
        version: Version,
        codedType: CodedType.Type = CodedType.self,
        migrations: [Version: (_ data: Data, _ decoder: PropertyListDecoder) async throws -> CodedType],
        directIndexes: [KeyPath<CodedType, any _Indexed>] = [],
        computedIndexes: [KeyPath<CodedType, any _Indexed>] = [],
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

