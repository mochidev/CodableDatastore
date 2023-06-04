//
//  Datastore.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

public struct Datastore<
    Version: StringIndexable & Hashable & CaseIterable,
    CodedType: Codable,
    IdentifierType: Indexable
> {
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
    ) {
        
    }
    
    /// Migrates and warms the data store ahead of time
    public func warm() async throws {
        
    }
}

extension Datastore {
    public func persist(_ instance: CodedType, to idenfifier: IdentifierType) async throws {
        
    }
    
    public func delete(_ idenfifier: IdentifierType) async throws {
        
    }
    
    public func load(_ idenfifier: IdentifierType) async throws -> CodedType? {
        return nil
    }
    
    public func load(_ range: any RangeExpression<IdentifierType>) async throws -> AsyncStream<CodedType> {
        return AsyncStream<CodedType> { continuation in
            continuation.finish()
        }
    }
}

extension Datastore {
    public func load<IndexedValue>(
        _ range: any RangeExpression<IdentifierType>,
        from keypath: KeyPath<CodedType, Indexed<IndexedValue>>
    ) async throws -> AsyncStream<CodedType> {
        return AsyncStream<CodedType> { continuation in
            continuation.finish()
        }
    }
}

extension Datastore where CodedType: Identifiable, IdentifierType == CodedType.ID {
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
    
    public func persist(_ instance: CodedType) async throws {
        try await self.persist(instance, to: instance.id)
    }
    
    func delete(_ instance: CodedType) async throws {
        try await self.delete(instance.id)
    }
    
    func load(_ instance: CodedType) async throws -> CodedType? {
        try await self.load(instance.id)
    }
}

extension Datastore {
    static func jsonStore(
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
    
    static func plistStore(
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

extension Datastore where CodedType: Identifiable, IdentifierType == CodedType.ID {
    static func jsonStore(
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
        self.jsonStore(
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
    
    static func plistStore(
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
        self.plistStore(
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

