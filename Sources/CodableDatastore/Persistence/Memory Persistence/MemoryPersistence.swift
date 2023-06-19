//
//  MemoryPersistence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

public actor MemoryPersistence: Persistence {
    public typealias AccessMode = ReadWrite
}

extension MemoryPersistence: _Persistence {
    public func register<V, C, I, A>(
        datastore: Datastore<V, C, I, A>
    ) async throws -> DatastoreDescriptor? {
        preconditionFailure("Unimplemented")
    }
    
    public func datastoreDescriptor<V, C, I, A>(
        for datastore: Datastore<V, C, I, A>
    ) async throws -> DatastoreDescriptor? {
        preconditionFailure("Unimplemented")
    }
    
    public func apply(
        descriptor: DatastoreDescriptor,
        for datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
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
    
    public func withTransaction(_ transaction: (MemoryPersistence) -> ()) async throws {
        preconditionFailure("Unimplemented")
    }
}
