//
//  DatastoreInterface.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-30.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

extension DiskPersistence {
    struct DatastoreInterface {
        let persistence: DiskPersistence
    }
}

// MARK: - Datastore Interface

extension DiskPersistence.DatastoreInterface: DatastoreInterfaceProtocol {
    func withTransaction<T>(options: TransactionOptions, transaction: @escaping (DatastoreInterfaceProtocol) async throws -> T) async throws -> T {
        try await persistence.withTransaction(options: options, transaction: transaction)
    }
    
    func register<Version, CodedType, IdentifierType, Access>(
        datastore: Datastore<Version, CodedType, IdentifierType, Access>
    ) async throws -> DatastoreDescriptor? {
        try await persistence.register(datastore: datastore)
        return try await datastoreDescriptor(for: datastore.key)
    }
    
    func datastoreDescriptor(
        for datastoreKey: DatastoreKey
    ) async throws -> DatastoreDescriptor? {
        try await DiskPersistence.Transaction.currentTransaction
            .datastoreDescriptor(for: datastoreKey)
    }
    
    func apply(
        descriptor: DatastoreDescriptor,
        for datastoreKey: DatastoreKey
    ) async throws {
        preconditionFailure("Unimplemented")
    }
}

// MARK: - Cursor Lookups

extension DiskPersistence.DatastoreInterface {
    func primaryIndexCursor<IdentifierType: Indexable>(
        for identifier: IdentifierType,
        datastoreKey: DatastoreKey
    ) async throws -> (
        cursor: any InstanceCursorProtocol,
        instanceData: Data,
        versionData: Data
    ) {
        try await DiskPersistence.Transaction.currentTransaction
            .primaryIndexCursor(
                for: identifier,
                datastoreKey: datastoreKey
            )
    }
    
    func primaryIndexCursor<IdentifierType: Indexable>(
        inserting identifier: IdentifierType,
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol {
        try await DiskPersistence.Transaction.currentTransaction
            .primaryIndexCursor(
                inserting: identifier,
                datastoreKey: datastoreKey
            )
    }
    
    func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        for index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws -> (
        cursor: any InstanceCursorProtocol,
        instanceData: Data,
        versionData: Data
    ) {
        try await DiskPersistence.Transaction.currentTransaction
            .directIndexCursor(
                for: index,
                identifier: identifier,
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
    
    func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol {
        try await DiskPersistence.Transaction.currentTransaction
            .directIndexCursor(
                inserting: index,
                identifier: identifier,
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
    
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        for index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws -> any InstanceCursorProtocol {
        try await DiskPersistence.Transaction.currentTransaction
            .secondaryIndexCursor(
                for: index,
                identifier: identifier,
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
    
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol {
        try await DiskPersistence.Transaction.currentTransaction
            .secondaryIndexCursor(
                inserting: index,
                identifier: identifier,
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
}

// MARK: - Entry Manipulation

extension DiskPersistence.DatastoreInterface {
    func persistPrimaryIndexEntry<IdentifierType: Indexable>(
        versionData: Data,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor: some InsertionCursorProtocol,
        datastoreKey: DatastoreKey
    ) async throws {
        try await DiskPersistence.Transaction.currentTransaction
            .persistPrimaryIndexEntry(
                versionData: versionData,
                identifierValue: identifierValue,
                instanceData: instanceData,
                cursor: cursor,
                datastoreKey: datastoreKey
            )
    }
    
    func deletePrimaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        datastoreKey: DatastoreKey
    ) async throws {
        try await DiskPersistence.Transaction.currentTransaction
            .deletePrimaryIndexEntry(
                cursor: cursor,
                datastoreKey: datastoreKey
            )
    }
    
    func resetPrimaryIndex(
        datastoreKey: DatastoreKey
    ) async throws {
        try await DiskPersistence.Transaction.currentTransaction
            .resetPrimaryIndex(
                datastoreKey: datastoreKey
            )
    }
    
    func persistDirectIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        versionData: Data,
        indexValue: IndexType,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor: some InsertionCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try await DiskPersistence.Transaction.currentTransaction
            .persistDirectIndexEntry(
                versionData: versionData,
                indexValue: indexValue,
                identifierValue: identifierValue,
                instanceData: instanceData,
                cursor: cursor,
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
    
    func deleteDirectIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try await DiskPersistence.Transaction.currentTransaction
            .deleteDirectIndexEntry(
                cursor: cursor,
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
    
    func deleteDirectIndex(
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try await DiskPersistence.Transaction.currentTransaction
            .deleteDirectIndex(
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
    
    func persistSecondaryIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        indexValue: IndexType,
        identifierValue: IdentifierType,
        cursor: some InsertionCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try await DiskPersistence.Transaction.currentTransaction
            .persistSecondaryIndexEntry(
                indexValue: indexValue,
                identifierValue: identifierValue,
                cursor: cursor,
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
    
    func deleteSecondaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try await DiskPersistence.Transaction.currentTransaction
            .deleteSecondaryIndexEntry(
                cursor: cursor,
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
    
    func deleteSecondaryIndex(
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try await DiskPersistence.Transaction.currentTransaction
            .deleteSecondaryIndex(
                indexName: indexName,
                datastoreKey: datastoreKey
            )
    }
}
