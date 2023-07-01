//
//  Transaction.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-21.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

extension DiskPersistence {
    actor Transaction: AnyDiskTransaction {
        let persistence: DiskPersistence
        
        unowned let parent: Transaction?
        var childTransactions: [Transaction] = []
        
        private(set) var task: Task<Void, Error>!
        let options: TransactionOptions
        
        var rootObjects: [String : Datastore.RootObject] = [:]
        
        private init(
            persistence: DiskPersistence,
            parent: Transaction?,
            options: TransactionOptions
        ) {
            self.persistence = persistence
            self.parent = parent
            self.options = options
        }
        
        private func attachTask<T>(
            options: TransactionOptions,
            handler: @escaping () async throws -> T
        ) async -> Task<T, Error> {
            let task = Task {
                let returnValue = try await TransactionTaskLocals.$transaction.withValue(self) {
                    try await handler()
                }
                
                /// If we don't care to collate our writes, go ahead and wait for the persistence to stick
                if !options.contains(.collateWrites) {
                    try await self.persist()
                }
                
                return returnValue
            }
            
            self.task = Task {
                _ = try await task.value
                
                /// If we previously skipped persisting, go ahead and do so now.
                if options.contains(.collateWrites) {
                    try await self.persist()
                }
            }
            
            return task
        }
        
        func apply(_ rootObjects: [String : Datastore.RootObject]) {
            for (key, value) in rootObjects {
                self.rootObjects[key] = value
            }
        }
        
        private func persist() async throws {
            if let parent {
                await parent.apply(rootObjects)
                return
            }
            
            for (_, root) in rootObjects {
                try await root.persistIfNeeded()
            }
        }
        
        static func makeTransaction<T>(
            persistence: DiskPersistence,
            lastTransaction: Transaction?,
            options: TransactionOptions,
            handler: @escaping () async throws -> T
        ) async -> (Transaction, Task<T, Error>) {
            if let parent = Self.unsafeCurrentTransaction {
                let (child, task) = await parent.childTransaction(options: options, handler: handler)
                return (child, task)
            }
            
            let transaction = Transaction(
                persistence: persistence,
                parent: nil,
                options: options
            )
            
            let task = await transaction.attachTask(options: options) {
                /// If the transaction is not read only, wait for the last transaction to properly finish before starting the next one.
                if !options.contains(.readOnly) {
                    try? await lastTransaction?.task.value
                }
                return try await handler()
            }
            
            return (transaction, task)
        }
        
        func childTransaction<T>(
            options: TransactionOptions,
            handler: @escaping () async throws -> T
        ) async -> (Transaction, Task<T, Error>) {
            let transaction = Transaction(
                persistence: persistence,
                parent: self,
                options: options
            )
            
            /// Get the last non-concurrent transaction from the list. Note that disk persistence currently does not support concurrent idempotent transactions.
            let lastChild = childTransactions.last { !$0.options.contains(.readOnly) }
            childTransactions.append(transaction)
            
            let task = await transaction.attachTask(options: options) {
                /// If the transaction is not read only, wait for the last transaction to properly finish before starting the next one.
                if !options.contains(.readOnly) {
                    _ = try? await lastChild?.task.value
                }
                return try await handler()
            }
            
            return (transaction, task)
        }
        
        func rootObject(for datastoreKey: String) async throws -> Datastore.RootObject? {
            if let rootObject = rootObjects[datastoreKey] {
                return rootObject
            }
            
            if let parent = parent {
                guard let rootObject = try await parent.rootObject(for: datastoreKey)
                else { return nil }
                rootObjects[datastoreKey] = rootObject
                return rootObject
            }
            
            let (persistenceDatastore, rootID) = try await persistence.persistenceDatastore(for: datastoreKey)
            
            guard let rootID else { return nil }
            
            let rootObject = await persistenceDatastore.rootObject(for: rootID)
            rootObjects[datastoreKey] = rootObject
            return rootObject
        }
        
        nonisolated static var unsafeCurrentTransaction: Self? {
            TransactionTaskLocals.transaction.map({ $0 as! Self })
        }
        
        nonisolated static var currentTransaction: Self {
            get throws {
                guard let transaction = TransactionTaskLocals.transaction.flatMap({ $0 as? Self })
                else { throw DiskPersistenceInternalError.missingTransaction }
                return transaction
            }
        }
    }
}

// MARK: - Datastore Interface

extension DiskPersistence.Transaction: DatastoreInterfaceProtocol {
    func withTransaction<T>(options: TransactionOptions, transaction: @escaping (DatastoreInterfaceProtocol) async throws -> T) async throws -> T {
        // TODO: Return a child directly?
        try await persistence.withTransaction(options: options, transaction: transaction)
    }
    
    func register<Version, CodedType, IdentifierType, Access>(
        datastore: Datastore<Version, CodedType, IdentifierType, Access>
    ) async throws -> DatastoreDescriptor? {
        try await persistence.register(datastore: datastore)
        return try await datastoreDescriptor(for: datastore)
    }
    
    func datastoreDescriptor<Version, CodedType, IdentifierType, Access>(
        for datastore: Datastore<Version, CodedType, IdentifierType, Access>
    ) async throws -> DatastoreDescriptor? {
        let rootObject = try await rootObject(for: datastore.key)
        return try await rootObject?.descriptor
    }
    
    func apply(descriptor: DatastoreDescriptor, for datastoreKey: String) async throws {
        preconditionFailure("Unimplemented")
    }
}

// MARK: - Cursor Lookups

extension DiskPersistence.Transaction {
    func primaryIndexCursor<IdentifierType: Indexable>(
        for identifier: IdentifierType,
        datastoreKey: String
    ) async throws -> (
        cursor: any InstanceCursorProtocol,
        instanceData: Data,
        versionData: Data
    ) {
        preconditionFailure("Unimplemented")
    }
    
    func primaryIndexCursor<IdentifierType: Indexable>(
        inserting identifier: IdentifierType,
        datastoreKey: String
    ) async throws -> any InsertionCursorProtocol {
        preconditionFailure("Unimplemented")
    }
    
    func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
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
    
    func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> any InsertionCursorProtocol {
        preconditionFailure("Unimplemented")
    }
    
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        for index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> any InstanceCursorProtocol {
        preconditionFailure("Unimplemented")
    }
    
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> any InsertionCursorProtocol {
        preconditionFailure("Unimplemented")
    }
}

// MARK: - Entry Manipulation

extension DiskPersistence.Transaction {
    func persistPrimaryIndexEntry<IdentifierType: Indexable>(
        versionData: Data,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor: some InsertionCursorProtocol,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    func deletePrimaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    func resetPrimaryIndex(
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    func persistDirectIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
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
    
    func deleteDirectIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    func deleteDirectIndex(
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    func persistSecondaryIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        indexValue: IndexType,
        identifierValue: IdentifierType,
        cursor: some InsertionCursorProtocol,
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    func deleteSecondaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
    
    func deleteSecondaryIndex(
        indexName: String,
        datastoreKey: String
    ) async throws {
        preconditionFailure("Unimplemented")
    }
}

// MARK: - Helper Types

fileprivate protocol AnyDiskTransaction {}

fileprivate enum TransactionTaskLocals {
    @TaskLocal
    static var transaction: AnyDiskTransaction?
}
