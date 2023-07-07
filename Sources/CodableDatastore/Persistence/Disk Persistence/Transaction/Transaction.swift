//
//  Transaction.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-21.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation
import Bytes

extension DiskPersistence {
    actor Transaction: AnyDiskTransaction {
        let persistence: DiskPersistence
        
        unowned let parent: Transaction?
        var childTransactions: [Transaction] = []
        
        private(set) var task: Task<Void, Error>!
        let options: TransactionOptions
        
        var rootObjects: [DatastoreKey : Datastore.RootObject] = [:]
        
        var createdRootObjects: Set<Datastore.RootObject> = []
        var createdIndexes: Set<Datastore.Index> = []
        var createdPages: Set<Datastore.Page> = []
        
        var deletedRootObjects: Set<Datastore.RootObject> = []
        var deletedIndexes: Set<Datastore.Index> = []
        var deletedPages: Set<Datastore.Page> = []
        
        // TODO: entryMutations, so we can send events to observers once the whole thing is finished.
        
        var isActive = false
        
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
                isActive = true
                let returnValue = try await TransactionTaskLocals.$transaction.withValue(self) {
                    try await handler()
                }
                isActive = false
                
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
        
        func checkIsActive() throws {
            guard isActive else {
                assertionFailure(DatastoreInterfaceError.transactionInactive.localizedDescription)
                throw DatastoreInterfaceError.transactionInactive
            }
        }
        
        func apply(
            rootObjects: [DatastoreKey : Datastore.RootObject],
            createdRootObjects: Set<Datastore.RootObject>,
            createdIndexes: Set<Datastore.Index>,
            createdPages: Set<Datastore.Page>,
            deletedRootObjects: Set<Datastore.RootObject>,
            deletedIndexes: Set<Datastore.Index>,
            deletedPages: Set<Datastore.Page>
        ) throws {
            try checkIsActive()
            
            for (key, value) in rootObjects {
                self.rootObjects[key] = value
            }
            
            /// We only want to persist the new objects that we didn't also create in this transaction, so if we deleted any objects that we previously just created, remove any references to them as they will only cause bloat once we persist to disk.
            let transientRootObjects = self.createdRootObjects.intersection(deletedRootObjects)
            let deletedRootObjects = deletedRootObjects.subtracting(transientRootObjects)
            self.createdRootObjects.subtract(transientRootObjects)
            self.createdRootObjects.formUnion(createdRootObjects)
            self.deletedRootObjects.formUnion(deletedRootObjects)
            
            let transientIndexes = self.createdIndexes.intersection(deletedIndexes)
            let deletedIndexes = deletedIndexes.subtracting(transientIndexes)
            self.createdIndexes.subtract(transientIndexes)
            self.createdIndexes.formUnion(createdIndexes)
            self.deletedIndexes.formUnion(deletedIndexes)
            
            let transientPages = self.createdPages.intersection(deletedPages)
            let deletedPages = deletedPages.subtracting(transientPages)
            self.createdPages.subtract(transientPages)
            self.createdPages.formUnion(createdPages)
            self.deletedPages.formUnion(deletedPages)
        }
        
        private func persist() async throws {
            if let parent {
                try await parent.apply(
                    rootObjects: rootObjects,
                    createdRootObjects: createdRootObjects,
                    createdIndexes: createdIndexes,
                    createdPages: createdPages,
                    deletedRootObjects: deletedRootObjects,
                    deletedIndexes: deletedIndexes,
                    deletedPages: deletedPages
                )
                return
            }
            
            for page in createdPages {
                try await page.persistIfNeeded()
            }
            
            for index in createdIndexes {
                try await index.persistIfNeeded()
            }
            
            for root in createdRootObjects {
                try await root.persistIfNeeded()
            }
            
            try await persistence.persist(roots: rootObjects)
        }
        
        static func makeTransaction<T>(
            persistence: DiskPersistence,
            lastTransaction: Transaction?,
            options: TransactionOptions,
            handler: @escaping (_ transaction: Transaction) async throws -> T
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
                return try await handler(transaction)
            }
            
            return (transaction, task)
        }
        
        func childTransaction<T>(
            options: TransactionOptions,
            handler: @escaping (_ transaction: Transaction) async throws -> T
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
                try self.checkIsActive()
                
                /// If the transaction is not read only, wait for the last transaction to properly finish before starting the next one.
                if !options.contains(.readOnly) {
                    _ = try? await lastChild?.task.value
                }
                return try await handler(transaction)
            }
            
            return (transaction, task)
        }
        
        func rootObject(for datastoreKey: DatastoreKey) async throws -> Datastore.RootObject? {
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
        
        func cursor(for cursor: any CursorProtocol) throws -> Cursor {
            guard cursor.persistence as? DiskPersistence === persistence
            else { throw DatastoreInterfaceError.unknownCursor }
            
            switch cursor {
            case let cursor as InstanceCursor:
                return .instance(cursor)
            case let cursor as InsertionCursor:
                return .insertion(cursor)
            default:
                throw DatastoreInterfaceError.unknownCursor
            }
        }
        
        nonisolated static var unsafeCurrentTransaction: Self? {
            TransactionTaskLocals.transaction.map({ $0 as! Self })
        }
    }
}

// MARK: - Datastore Interface

extension DiskPersistence.Transaction: DatastoreInterfaceProtocol {
    func register<Version, CodedType, IdentifierType, Access>(
        datastore: Datastore<Version, CodedType, IdentifierType, Access>
    ) async throws -> DatastoreDescriptor? {
        try checkIsActive()
        
        try await persistence.register(datastore: datastore)
        return try await datastoreDescriptor(for: datastore.key)
    }
    
    func datastoreDescriptor(
        for datastoreKey: DatastoreKey
    ) async throws -> DatastoreDescriptor? {
        try checkIsActive()
        
        let rootObject = try await rootObject(for: datastoreKey)
        return try await rootObject?.manifest.descriptor
    }
    
    func apply(descriptor: DatastoreDescriptor, for datastoreKey: DatastoreKey) async throws {
        try checkIsActive()
        
        if let rootObject = try await rootObject(for: datastoreKey) {
            var manifest = try await rootObject.manifest
            
            // TODO: Do a better merge of these descriptors, especially since size is something we want to preserve, amongst other properties
            guard manifest.descriptor != descriptor else { return }
            
            manifest.id = DatastoreRootIdentifier()
            manifest.modificationDate = Date()
            manifest.descriptor = descriptor
            
            let newRoot = DiskPersistence.Datastore.RootObject(
                datastore: rootObject.datastore,
                id: manifest.id,
                rootObject: manifest
            )
            rootObjects[datastoreKey] = newRoot
            createdRootObjects.insert(newRoot)
            deletedRootObjects.insert(rootObject)
            await newRoot.datastore.adopt(rootObject: newRoot)
            
            // TODO: Don't forget to create the new index objects too!
        } else {
            let (datastore, _) = try await persistence.persistenceDatastore(for: datastoreKey)
            
            /// Create index objects first so they are available when requested.
            let primaryManifestIdentifier = DatastoreIndexManifestIdentifier()
            
            let directIndexManifests = descriptor.directIndexes.map { (_, index) in
                DatastoreRootManifest.IndexInfo(
                    key: index.key,
                    id: DatastoreIndexIdentifier(name: index.key),
                    root: DatastoreIndexManifestIdentifier()
                )
            }
            
            let secondaryIndexManifests = descriptor.secondaryIndexes.map { (_, index) in
                DatastoreRootManifest.IndexInfo(
                    key: index.key,
                    id: DatastoreIndexIdentifier(name: index.key),
                    root: DatastoreIndexManifestIdentifier()
                )
            }
            
            let primaryIndex = DiskPersistence.Datastore.Index(
                datastore: datastore,
                id: .primary(manifest: primaryManifestIdentifier),
                manifest: DatastoreIndexManifest(
                    id: primaryManifestIdentifier,
                    orderedPages: []
                )
            )
            createdIndexes.insert(primaryIndex)
            await datastore.adopt(index: primaryIndex)
            
            for indexInfo in directIndexManifests {
                let index = DiskPersistence.Datastore.Index(
                    datastore: datastore,
                    id: .direct(index: indexInfo.id, manifest: indexInfo.root),
                    manifest: DatastoreIndexManifest(
                        id: indexInfo.root,
                        orderedPages: []
                    )
                )
                createdIndexes.insert(index)
                await datastore.adopt(index: index)
            }
            
            for indexInfo in secondaryIndexManifests {
                let index = DiskPersistence.Datastore.Index(
                    datastore: datastore,
                    id: .secondary(index: indexInfo.id, manifest: indexInfo.root),
                    manifest: DatastoreIndexManifest(
                        id: indexInfo.root,
                        orderedPages: []
                    )
                )
                createdIndexes.insert(index)
                await datastore.adopt(index: index)
            }
            
            /// Create the root object from the indexes that were created
            let manifest = DatastoreRootManifest(
                id: DatastoreRootIdentifier(),
                modificationDate: Date(),
                descriptor: descriptor,
                primaryIndexManifest: primaryManifestIdentifier,
                directIndexManifests: directIndexManifests,
                secondaryIndexManifests: secondaryIndexManifests
            )
            
            let newRoot = DiskPersistence.Datastore.RootObject(
                datastore: datastore,
                id: manifest.id,
                rootObject: manifest
            )
            rootObjects[datastoreKey] = newRoot
            createdRootObjects.insert(newRoot)
            await datastore.adopt(rootObject: newRoot)
        }
    }
}

// MARK: - Cursor Lookups

private func primaryIndexComparator<IdentifierType: Indexable>(lhs: IdentifierType, rhs: DatastorePageEntry) throws -> SortOrder {
    guard rhs.headers.count == 2
    else { throw DiskPersistenceError.invalidEntryFormat }
    
    let identifierBytes = rhs.headers[1]
    
    let entryIdentifier = try JSONDecoder.shared.decode(IdentifierType.self, from: Data(identifierBytes))
    
    return lhs.sortOrder(comparedTo: entryIdentifier)
}

extension DiskPersistence.Transaction {
    func primaryIndexCursor<IdentifierType: Indexable>(
        for identifier: IdentifierType,
        datastoreKey: DatastoreKey
    ) async throws -> (
        cursor: any InstanceCursorProtocol,
        instanceData: Data,
        versionData: Data
    ) {
        try checkIsActive()
        
        guard let rootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        let index = try await rootObject.primaryIndex
        
        let (cursor, entry) = try await index.entry(for: identifier, comparator: primaryIndexComparator)
        guard entry.headers.count == 2
        else { throw DiskPersistenceError.invalidEntryFormat }
        
        return (
            cursor: cursor,
            instanceData: Data(entry.content),
            versionData: Data(entry.headers[1])
        )
    }
    
    func primaryIndexCursor<IdentifierType: Indexable>(
        inserting identifier: IdentifierType,
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol {
        try checkIsActive()
        
        guard let rootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        let index = try await rootObject.primaryIndex
        
        return try await index.insertionCursor(for: identifier, comparator: primaryIndexComparator)
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
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
    
    func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
    
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        for index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws -> any InstanceCursorProtocol {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
    
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
}

// MARK: - Entry Manipulation

extension DiskPersistence.Transaction {
    func persistPrimaryIndexEntry<IdentifierType: Indexable>(
        versionData: Data,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor someCursor: some InsertionCursorProtocol,
        datastoreKey: DatastoreKey
    ) async throws {
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        let datastore = existingRootObject.datastore
        
        let existingIndex = try await existingRootObject.primaryIndex
        
        let entry = DatastorePageEntry(
            headers: [
                Bytes(versionData),
                Bytes(try JSONEncoder.shared.encode(identifierValue))
            ],
            content: Bytes(instanceData)
        )
        
        let (indexManifest, newPages, removedPages) = try await {
            switch try cursor(for: someCursor) {
            case .insertion(let cursor):
                return try await existingIndex.manifest(inserting: entry, at: cursor)
            case .instance(let cursor):
                return try await existingIndex.manifest(replacing: entry, at: cursor)
            }
        }()
        
        /// No change occured, bail early
        guard existingIndex.id.manifestID != indexManifest.id else { return }
        
        for newPage in newPages {
            createdPages.insert(newPage)
            await datastore.adopt(page: newPage)
        }
        deletedPages.formUnion(removedPages)
        
        let newPrimaryIndex = DiskPersistence.Datastore.Index(
            datastore: datastore,
            id: .primary(manifest: indexManifest.id),
            manifest: indexManifest
        )
        createdIndexes.insert(newPrimaryIndex)
        deletedIndexes.insert(existingIndex)
        await datastore.adopt(index: newPrimaryIndex)
        
        let rootManifest = try await existingRootObject.manifest(replacing: newPrimaryIndex.id)
        
        /// No change occured, bail early
        guard existingRootObject.id != rootManifest.id else { return }
        
        let newRootObject = DiskPersistence.Datastore.RootObject(
            datastore: existingRootObject.datastore,
            id: rootManifest.id,
            rootObject: rootManifest
        )
        createdRootObjects.insert(newRootObject)
        deletedRootObjects.insert(existingRootObject)
        await datastore.adopt(rootObject: newRootObject)
        rootObjects[datastoreKey] = newRootObject
    }
    
    func deletePrimaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        datastoreKey: DatastoreKey
    ) async throws {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
    
    func resetPrimaryIndex(
        datastoreKey: DatastoreKey
    ) async throws {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
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
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
    
    func deleteDirectIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
    
    func deleteDirectIndex(
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
    
    func persistSecondaryIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        indexValue: IndexType,
        identifierValue: IdentifierType,
        cursor: some InsertionCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
    
    func deleteSecondaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
    
    func deleteSecondaryIndex(
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws {
        try checkIsActive()
        
        preconditionFailure("Unimplemented")
    }
}

// MARK: - Helper Types

fileprivate protocol AnyDiskTransaction {}

fileprivate enum TransactionTaskLocals {
    @TaskLocal
    static var transaction: AnyDiskTransaction?
}
