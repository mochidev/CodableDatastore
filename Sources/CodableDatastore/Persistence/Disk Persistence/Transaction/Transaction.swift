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
        let options: UnsafeTransactionOptions
        let actionName: String?
        
        var rootObjects: [DatastoreKey : Datastore.RootObject] = [:]
        
        var createdRootObjects: Set<Datastore.RootObject> = []
        var createdIndexes: Set<Datastore.Index> = []
        var createdPages: Set<Datastore.Page> = []
        
        var deletedRootObjects: Set<Datastore.RootObject> = []
        var deletedIndexes: Set<Datastore.Index> = []
        var deletedPages: Set<Datastore.Page> = []
        
        var entryMutations: [(DatastoreKey, ObservedEvent<Data, ObservationEntry>)] = []
        var observerCache: [DatastoreKey : Bool] = [:]
        
        var isActive = false
        
        private init(
            persistence: DiskPersistence,
            parent: Transaction?,
            actionName: String?,
            options: UnsafeTransactionOptions
        ) {
            self.persistence = persistence
            self.parent = parent
            self.actionName = actionName
            self.options = options
        }
        
        private func attachTask<T>(
            options: UnsafeTransactionOptions,
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
            entryMutations: [(DatastoreKey, ObservedEvent<Data, ObservationEntry>)],
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
            
            self.entryMutations.append(contentsOf: entryMutations)
            
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
            defer {
                rootObjects.removeAll()
                entryMutations.removeAll()
                createdRootObjects.removeAll()
                createdIndexes.removeAll()
                createdPages.removeAll()
                deletedRootObjects.removeAll()
                deletedIndexes.removeAll()
                deletedPages.removeAll()
                childTransactions.removeAll()
            }
            
            if let parent {
                /// If the transaction is read-only, stop here without applying anything to the parent.
                guard !options.contains(.readOnly) else {
                    assert(entryMutations.isEmpty, "Entries were mutated in a read-only transaction!")
                    assert(createdRootObjects.isEmpty, "Root objects were created in a read-only transaction!")
                    assert(createdIndexes.isEmpty, "Indexes were created in a read-only transaction!")
                    assert(createdPages.isEmpty, "Pages were created in a read-only transaction!")
                    assert(deletedRootObjects.isEmpty, "Root objects were deleted in a read-only transaction!")
                    assert(deletedIndexes.isEmpty, "Indexes were deleted in a read-only transaction!")
                    assert(deletedPages.isEmpty, "Pages were deleted in a read-only transaction!")
                    return
                }
                try await parent.apply(
                    rootObjects: rootObjects,
                    entryMutations: entryMutations,
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
            
            let addedDatastoreRoots = Set(createdRootObjects.map(\.id))
            let removedDatastoreRoots = Set(deletedRootObjects.map(\.id))
            
            try await persistence.persist(
                actionName: actionName,
                roots: rootObjects,
                addedDatastoreRoots: addedDatastoreRoots,
                removedDatastoreRoots: removedDatastoreRoots
            )
            
            var datastores: [DatastoreKey : Datastore] = [:]
            for (datastoreKey, event) in entryMutations {
                let datastore: Datastore
                if let cachedDatastore = datastores[datastoreKey] {
                    datastore = cachedDatastore
                } else {
                    datastore = try await persistence.persistenceDatastore(for: datastoreKey).0
                    datastores[datastoreKey] = datastore
                }
                await datastore.emit(event)
            }
        }
        
        static func makeTransaction<T>(
            persistence: DiskPersistence,
            lastTransaction: Transaction?,
            actionName: String?,
            options: UnsafeTransactionOptions,
            handler: @escaping (_ transaction: Transaction, _ isDurable: Bool) async throws -> T
        ) async -> (Transaction, Task<T, Error>) {
            if let parent = Self.unsafeCurrentTransaction {
                let (child, task) = await parent.childTransaction(options: options, handler: handler)
                return (child, task)
            }
            
            let transaction = Transaction(
                persistence: persistence,
                parent: nil,
                actionName: actionName,
                options: options
            )
            
            let task = await transaction.attachTask(options: options) {
                /// If the transaction is not read only, wait for the last transaction to properly finish before starting the next one.
                if !options.contains(.readOnly) {
                    try? await lastTransaction?.task.value
                }
                return try await handler(transaction, options.isDisjoint(with: [.collateWrites, .readOnly]))
            }
            
            return (transaction, task)
        }
        
        func childTransaction<T>(
            options: UnsafeTransactionOptions,
            handler: @escaping (_ transaction: Transaction, _ isDurable: Bool) async throws -> T
        ) async -> (Transaction, Task<T, Error>) {
            assert(!self.options.contains(.readOnly) || options.contains(.readOnly), "A child transaction was declared read-write, even though its parent was read-only!")
            let transaction = Transaction(
                persistence: persistence,
                parent: self,
                actionName: nil,
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
                return try await handler(transaction, false)
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
        
        func hasObservers(for datastoreKey: DatastoreKey) async throws -> Bool {
            if let hasObservers = observerCache[datastoreKey] {
                return hasObservers
            }
            
            if let parent = parent {
                let hasObservers = try await parent.hasObservers(for: datastoreKey)
                observerCache[datastoreKey] = hasObservers
                return hasObservers
            }
            
            let (datastore, _) = try await persistence.persistenceDatastore(for: datastoreKey)
            let hasObservers = await datastore.hasObservers
            observerCache[datastoreKey] = hasObservers
            return hasObservers
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
    func register<Format: DatastoreFormat, Access>(
        datastore: Datastore<Format, Access>
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
        assert(!options.contains(.readOnly), "apply(descriptor:for:) called on a read-only transaction!")
        try checkIsActive()
        
        if let existingRootObject = try await rootObject(for: datastoreKey) {
            let datastore = existingRootObject.datastore
            
            let (rootManifest, newIndexes) = try await existingRootObject.manifest(applying: descriptor)
            
            /// No change occured, bail early
            guard existingRootObject.id != rootManifest.id else { return }
            
            for newIndex in newIndexes {
                createdIndexes.insert(newIndex)
                await datastore.adopt(index: newIndex)
            }
            
            let newRootObject = DiskPersistence.Datastore.RootObject(
                datastore: datastore,
                id: rootManifest.id,
                rootObject: rootManifest
            )
            createdRootObjects.insert(newRootObject)
            if createdRootObjects.contains(existingRootObject) {
                createdRootObjects.remove(existingRootObject)
            } else {
                deletedRootObjects.insert(existingRootObject)
            }
            await datastore.adopt(rootObject: newRootObject)
            rootObjects[datastoreKey] = newRootObject
        } else {
            let (datastore, _) = try await persistence.persistenceDatastore(for: datastoreKey)
            
            /// Create index objects first so they are available when requested.
            let primaryManifestIdentifier = DatastoreIndexManifestIdentifier()
            
            let directIndexManifests = descriptor.directIndexes.map { (_, index) in
                DatastoreRootManifest.IndexInfo(
                    name: index.name,
                    id: DatastoreIndexIdentifier(name: index.name),
                    root: DatastoreIndexManifestIdentifier()
                )
            }
            
            let secondaryIndexManifests = descriptor.referenceIndexes.map { (_, index) in
                DatastoreRootManifest.IndexInfo(
                    name: index.name,
                    id: DatastoreIndexIdentifier(name: index.name),
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
            
            var addedIndexes: Set<DatastoreRootManifest.IndexID> = []
            var addedIndexManifests: Set<DatastoreRootManifest.IndexManifestID> = []
            addedIndexes.insert(.primary)
            addedIndexManifests.insert(.primary(manifest: primaryManifestIdentifier))
            
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
                addedIndexes.insert(.direct(index: indexInfo.id))
                addedIndexManifests.insert(.direct(index: indexInfo.id, manifest: indexInfo.root))
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
                addedIndexes.insert(.secondary(index: indexInfo.id))
                addedIndexManifests.insert(.secondary(index: indexInfo.id, manifest: indexInfo.root))
                await datastore.adopt(index: index)
            }
            
            var descriptor = descriptor
            descriptor.size = 0
            
            let modificationDate = Date()
            /// Create the root object from the indexes that were created
            let manifest = DatastoreRootManifest(
                id: DatastoreRootIdentifier(date: modificationDate),
                modificationDate: modificationDate,
                descriptor: descriptor,
                primaryIndexManifest: primaryManifestIdentifier,
                directIndexManifests: directIndexManifests,
                secondaryIndexManifests: secondaryIndexManifests,
                addedIndexes: addedIndexes,
                addedIndexManifests: addedIndexManifests
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

private func directIndexComparator<IndexType: Indexable, IdentifierType: Indexable>(lhs: (index: IndexType, identifier: IdentifierType), rhs: DatastorePageEntry) throws -> SortOrder {
    guard rhs.headers.count == 3
    else { throw DiskPersistenceError.invalidEntryFormat }
    
    let indexBytes = rhs.headers[1]
    
    let indexedValue = try JSONDecoder.shared.decode(IndexType.self, from: Data(indexBytes))
    
    let sortOrder = lhs.index.sortOrder(comparedTo: indexedValue)
    guard sortOrder == .equal else { return sortOrder }
    
    let identifierBytes = rhs.headers[2]
    
    let entryIdentifier = try JSONDecoder.shared.decode(IdentifierType.self, from: Data(identifierBytes))
    
    return lhs.identifier.sortOrder(comparedTo: entryIdentifier)
}

private func secondaryIndexComparator<IndexType: Indexable, IdentifierType: Indexable>(lhs: (index: IndexType, identifier: IdentifierType), rhs: DatastorePageEntry) throws -> SortOrder {
    guard rhs.headers.count == 1
    else { throw DiskPersistenceError.invalidEntryFormat }
    
    let indexBytes = rhs.headers[0]
    
    let indexedValue = try JSONDecoder.shared.decode(IndexType.self, from: Data(indexBytes))
    
    let sortOrder = lhs.index.sortOrder(comparedTo: indexedValue)
    guard sortOrder == .equal else { return sortOrder }
    
    let identifierBytes = rhs.content
    
    let entryIdentifier = try JSONDecoder.shared.decode(IdentifierType.self, from: Data(identifierBytes))
    
    return lhs.identifier.sortOrder(comparedTo: entryIdentifier)
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
            versionData: Data(entry.headers[0])
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
        for indexValue: IndexType,
        identifier: IdentifierType,
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws -> (
        cursor: any InstanceCursorProtocol,
        instanceData: Data,
        versionData: Data
    ) {
        try checkIsActive()
        
        guard let rootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        guard let index = try await rootObject.directIndexes[indexName]
        else { throw DatastoreInterfaceError.indexNotFound }
        
        let (cursor, entry) = try await index.entry(for: (indexValue, identifier), comparator: directIndexComparator)
        guard entry.headers.count == 3
        else { throw DiskPersistenceError.invalidEntryFormat }
        
        return (
            cursor: cursor,
            instanceData: Data(entry.content),
            versionData: Data(entry.headers[0])
        )
    }
    
    func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting indexValue: IndexType,
        identifier: IdentifierType,
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol {
        try checkIsActive()
        
        guard let rootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        guard let index = try await rootObject.directIndexes[indexName]
        else { throw DatastoreInterfaceError.indexNotFound }
        
        return try await index.insertionCursor(for: (indexValue, identifier), comparator: directIndexComparator)
    }
    
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        for indexValue: IndexType,
        identifier: IdentifierType,
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws -> any InstanceCursorProtocol {
        try checkIsActive()
        
        guard let rootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        guard let index = try await rootObject.secondaryIndexes[indexName]
        else { throw DatastoreInterfaceError.indexNotFound }
        
        let (cursor, _) = try await index.entry(for: (indexValue, identifier), comparator: secondaryIndexComparator)
        
        return cursor
    }
    
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting indexValue: IndexType,
        identifier: IdentifierType,
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol {
        try checkIsActive()
        
        guard let rootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        guard let index = try await rootObject.secondaryIndexes[indexName]
        else { throw DatastoreInterfaceError.indexNotFound }
        
        return try await index.insertionCursor(for: (indexValue, identifier), comparator: secondaryIndexComparator)
    }
}

// MARK: - Range Lookups

private func primaryIndexBoundComparator<IdentifierType: Indexable>(lhs: (bound: RangeBoundExpression<IdentifierType>, order: RangeOrder), rhs: DatastorePageEntry) throws -> SortOrder {
    guard rhs.headers.count == 2
    else { throw DiskPersistenceError.invalidEntryFormat }
    
    if case .extent = lhs.bound { return SortOrder(lhs.order) }
    
    let identifierBytes = rhs.headers[1]
    let entryIdentifier = try JSONDecoder.shared.decode(IdentifierType.self, from: Data(identifierBytes))
    
    return lhs.bound.sortOrder(comparedTo: entryIdentifier, order: lhs.order)
}

private func directIndexBoundComparator<IndexType: Indexable>(lhs: (bound: RangeBoundExpression<IndexType>, order: RangeOrder), rhs: DatastorePageEntry) throws -> SortOrder {
    guard rhs.headers.count == 3
    else { throw DiskPersistenceError.invalidEntryFormat }
    
    if case .extent = lhs.bound { return SortOrder(lhs.order) }
    
    let indexBytes = rhs.headers[1]
    let indexedValue = try JSONDecoder.shared.decode(IndexType.self, from: Data(indexBytes))
    
    return lhs.bound.sortOrder(comparedTo: indexedValue, order: lhs.order)
}

private func secondaryIndexBoundComparator<IndexType: Indexable>(lhs: (bound: RangeBoundExpression<IndexType>, order: RangeOrder), rhs: DatastorePageEntry) throws -> SortOrder {
    guard rhs.headers.count == 1
    else { throw DiskPersistenceError.invalidEntryFormat }
    
    if case .extent = lhs.bound { return SortOrder(lhs.order) }
    
    let indexBytes = rhs.headers[0]
    let indexedValue = try JSONDecoder.shared.decode(IndexType.self, from: Data(indexBytes))
    
    return lhs.bound.sortOrder(comparedTo: indexedValue, order: lhs.order)
}

extension DiskPersistence.Transaction {
    func primaryIndexScan<IdentifierType: Indexable>(
        range: any IndexRangeExpression<IdentifierType>,
        datastoreKey: DatastoreKey,
        instanceConsumer: (_ versionData: Data, _ instanceData: Data) async throws -> ()
    ) async throws {
        try checkIsActive()
        
        guard let rootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        let index = try await rootObject.primaryIndex
        
        switch range.order {
        case .ascending:
            let startCursor = if range.lowerBoundExpression == .extent {
                await index.firstInsertionCursor
            } else {
                try await index.insertionCursor(
                    for: (range.lowerBoundExpression, .ascending),
                    comparator: primaryIndexBoundComparator
                )
            }
            
            try await index.forwardScanEntries(after: startCursor) { entry in
                if range.upperBoundExpression != .extent {
                    guard case .descending = try primaryIndexBoundComparator(
                        lhs: (bound: range.upperBoundExpression, order: .descending),
                        rhs: entry
                    )
                    else { return false }
                }
                
                let versionData = Data(entry.headers[0])
                let instanceData = Data(entry.content)
                
                try await instanceConsumer(versionData, instanceData)
                return true
            }
        case .descending:
            let startCursor = if range.upperBoundExpression == .extent {
                try await index.lastInsertionCursor
            } else {
                try await index.insertionCursor(
                    for: (range.upperBoundExpression, .descending),
                    comparator: primaryIndexBoundComparator
                )
            }
            
            try await index.backwardScanEntries(before: startCursor) { entry in
                if range.lowerBoundExpression != .extent {
                    guard case .ascending = try primaryIndexBoundComparator(
                        lhs: (bound: range.lowerBoundExpression, order: .ascending),
                        rhs: entry
                    )
                    else { return false }
                }
                
                let versionData = Data(entry.headers[0])
                let instanceData = Data(entry.content)
                
                try await instanceConsumer(versionData, instanceData)
                return true
            }
        }
    }
    
    func directIndexScan<IndexType: Indexable>(
        range: any IndexRangeExpression<IndexType>,
        indexName: IndexName,
        datastoreKey: DatastoreKey,
        instanceConsumer: (_ versionData: Data, _ instanceData: Data) async throws -> ()
    ) async throws {
        try checkIsActive()
        
        guard let rootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        guard let index = try await rootObject.directIndexes[indexName]
        else { throw DatastoreInterfaceError.indexNotFound }
        
        switch range.order {
        case .ascending:
            let startCursor = if range.lowerBoundExpression == .extent {
                await index.firstInsertionCursor
            } else {
                try await index.insertionCursor(
                    for: (range.lowerBoundExpression, .ascending),
                    comparator: directIndexBoundComparator
                )
            }
            
            try await index.forwardScanEntries(after: startCursor) { entry in
                if range.upperBoundExpression != .extent {
                    guard case .descending = try directIndexBoundComparator(
                        lhs: (bound: range.upperBoundExpression, order: .descending),
                        rhs: entry
                    )
                    else { return false }
                }
                
                let versionData = Data(entry.headers[0])
                let instanceData = Data(entry.content)
                
                try await instanceConsumer(versionData, instanceData)
                return true
            }
        case .descending:
            let startCursor = if range.upperBoundExpression == .extent {
                try await index.lastInsertionCursor
            } else {
                try await index.insertionCursor(
                    for: (range.upperBoundExpression, .descending),
                    comparator: directIndexBoundComparator
                )
            }
            
            try await index.backwardScanEntries(before: startCursor) { entry in
                if range.lowerBoundExpression != .extent {
                    guard case .ascending = try directIndexBoundComparator(
                        lhs: (bound: range.lowerBoundExpression, order: .ascending),
                        rhs: entry
                    )
                    else { return false }
                }
                
                let versionData = Data(entry.headers[0])
                let instanceData = Data(entry.content)
                
                try await instanceConsumer(versionData, instanceData)
                return true
            }
        }
    }
    
    func secondaryIndexScan<IndexType: Indexable, IdentifierType: Indexable>(
        range: any IndexRangeExpression<IndexType>,
        indexName: IndexName,
        datastoreKey: DatastoreKey,
        identifierConsumer: (_ identifier: IdentifierType) async throws -> ()
    ) async throws {
        try checkIsActive()
        
        guard let rootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        guard let index = try await rootObject.secondaryIndexes[indexName]
        else { throw DatastoreInterfaceError.indexNotFound }
        
        switch range.order {
        case .ascending:
            let startCursor = if range.lowerBoundExpression == .extent {
                await index.firstInsertionCursor
            } else {
                try await index.insertionCursor(
                    for: (range.lowerBoundExpression, .ascending),
                    comparator: secondaryIndexBoundComparator
                )
            }
            
            try await index.forwardScanEntries(after: startCursor) { entry in
                if range.upperBoundExpression != .extent {
                    guard case .descending = try secondaryIndexBoundComparator(
                        lhs: (bound: range.upperBoundExpression, order: .descending),
                        rhs: entry
                    )
                    else { return false }
                }
                
                let entryIdentifier = try JSONDecoder.shared.decode(IdentifierType.self, from: Data(entry.content))
                try await identifierConsumer(entryIdentifier)
                return true
            }
        case .descending:
            let startCursor = if range.upperBoundExpression == .extent {
                try await index.lastInsertionCursor
            } else {
                try await index.insertionCursor(
                    for: (range.upperBoundExpression, .descending),
                    comparator: secondaryIndexBoundComparator
                )
            }
            
            try await index.backwardScanEntries(before: startCursor) { entry in
                if range.lowerBoundExpression != .extent {
                    guard case .ascending = try secondaryIndexBoundComparator(
                        lhs: (bound: range.lowerBoundExpression, order: .ascending),
                        rhs: entry
                    )
                    else { return false }
                }
                
                let entryIdentifier = try JSONDecoder.shared.decode(IdentifierType.self, from: Data(entry.content))
                try await identifierConsumer(entryIdentifier)
                return true
            }
        }
    }
}

// MARK: - Entry Manipulation

extension DiskPersistence.Transaction {
    func persist(
        entry: DatastorePageEntry,
        at someCursor: some InsertionCursorProtocol,
        existingRootObject: DiskPersistence.Datastore.RootObject,
        existingIndex: DiskPersistence.Datastore.Index?,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "persist(entry:...) called on a read-only transaction!")
        guard let existingIndex
        else { throw DatastoreInterfaceError.indexNotFound }
        
        let datastore = existingRootObject.datastore
        
        /// Depending on the cursor type, insert or replace the entry in the index, capturing the new manifesr, added and removed pages, and change in the number of entries.
        let ((indexManifest, newPages, removedPages), newEntryCount) = try await {
            switch try cursor(for: someCursor) {
            case .insertion(let cursor):
                return (try await existingIndex.manifest(inserting: entry, at: cursor), 1)
            case .instance(let cursor):
                return (try await existingIndex.manifest(replacing: entry, at: cursor), 0)
            }
        }()
        
        /// No change occured, bail early
        guard existingIndex.id.manifestID != indexManifest.id else { return }
        
        for newPage in newPages {
            createdPages.insert(newPage)
            await datastore.adopt(page: newPage)
        }
        deletedPages.formUnion(removedPages)
        
        let newIndex = DiskPersistence.Datastore.Index(
            datastore: datastore,
            id: existingIndex.id.with(manifestID: indexManifest.id),
            manifest: indexManifest
        )
        createdIndexes.insert(newIndex)
        if createdIndexes.contains(existingIndex) {
            createdIndexes.insert(existingIndex)
        } else {
            deletedIndexes.insert(existingIndex)
        }
        await datastore.adopt(index: newIndex)
        
        var rootManifest = try await existingRootObject.manifest(replacing: newIndex.id)
        
        /// If the index we are modifying is the primary one, update the number of entries we are managing.
        if case .primary = newIndex.id {
            rootManifest.descriptor.size += newEntryCount
        }
        
        /// No change occured, bail early
        guard existingRootObject.id != rootManifest.id else { return }
        
        let newRootObject = DiskPersistence.Datastore.RootObject(
            datastore: existingRootObject.datastore,
            id: rootManifest.id,
            rootObject: rootManifest
        )
        createdRootObjects.insert(newRootObject)
        if createdRootObjects.contains(existingRootObject) {
            createdRootObjects.remove(existingRootObject)
        } else {
            deletedRootObjects.insert(existingRootObject)
        }
        await datastore.adopt(rootObject: newRootObject)
        rootObjects[datastoreKey] = newRootObject
    }
    
    func persistPrimaryIndexEntry<IdentifierType: Indexable>(
        versionData: Data,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor: some InsertionCursorProtocol,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "persistPrimaryIndexEntry(...) called on a read-only transaction!")
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        try await persist(
            entry: DatastorePageEntry(
                headers: [
                    Bytes(versionData),
                    Bytes(try JSONEncoder.shared.encode(identifierValue))
                ],
                content: Bytes(instanceData)
            ),
            at: cursor,
            existingRootObject: existingRootObject,
            existingIndex: try await existingRootObject.primaryIndex,
            datastoreKey: datastoreKey
        )
    }
    
    func delete(
        cursor: some InstanceCursorProtocol,
        existingRootObject: DiskPersistence.Datastore.RootObject,
        existingIndex: DiskPersistence.Datastore.Index?,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "delete(...) called on a read-only transaction!")
        guard let existingIndex
        else { throw DatastoreInterfaceError.indexNotFound }
        
        guard
            cursor.persistence as? DiskPersistence === persistence,
            let cursor = cursor as? DiskPersistence.InstanceCursor
        else { throw DatastoreInterfaceError.unknownCursor }
        
        let datastore = existingRootObject.datastore
        
        let (indexManifest, newPages, removedPages) = try await existingIndex.manifest(deletingEntryAt: cursor)
        
        /// No change occured, bail early
        guard existingIndex.id.manifestID != indexManifest.id else { return }
        
        for newPage in newPages {
            createdPages.insert(newPage)
            await datastore.adopt(page: newPage)
        }
        deletedPages.formUnion(removedPages)
        
        let newIndex = DiskPersistence.Datastore.Index(
            datastore: datastore,
            id: existingIndex.id.with(manifestID: indexManifest.id),
            manifest: indexManifest
        )
        createdIndexes.insert(newIndex)
        if createdIndexes.contains(existingIndex) {
            createdIndexes.insert(existingIndex)
        } else {
            deletedIndexes.insert(existingIndex)
        }
        await datastore.adopt(index: newIndex)
        
        var rootManifest = try await existingRootObject.manifest(replacing: newIndex.id)
        
        /// If the index we are modifying is the primary one, update the number of entries we are managing.
        if case .primary = newIndex.id {
            rootManifest.descriptor.size -= 1
        }
        
        /// No change occured, bail early
        guard existingRootObject.id != rootManifest.id else { return }
        
        let newRootObject = DiskPersistence.Datastore.RootObject(
            datastore: existingRootObject.datastore,
            id: rootManifest.id,
            rootObject: rootManifest
        )
        createdRootObjects.insert(newRootObject)
        if createdRootObjects.contains(existingRootObject) {
            createdRootObjects.remove(existingRootObject)
        } else {
            deletedRootObjects.insert(existingRootObject)
        }
        await datastore.adopt(rootObject: newRootObject)
        rootObjects[datastoreKey] = newRootObject
    }
    
    func deletePrimaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "deletePrimaryIndexEntry(...) called on a read-only transaction!")
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        try await delete(
            cursor: cursor,
            existingRootObject: existingRootObject,
            existingIndex: try await existingRootObject.primaryIndex,
            datastoreKey: datastoreKey
        )
    }
    
    func resetPrimaryIndex(
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "resetPrimaryIndex(...) called on a read-only transaction!")
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        let existingIndex = try await existingRootObject.primaryIndex
        
        let datastore = existingRootObject.datastore
        
        let (indexManifest, removedPages) = try await existingIndex.manifestDeletingAllEntries()
        
        /// No change occured, bail early
        guard existingIndex.id.manifestID != indexManifest.id else { return }
        
        deletedPages.formUnion(removedPages)
        
        let newIndex = DiskPersistence.Datastore.Index(
            datastore: datastore,
            id: existingIndex.id.with(manifestID: indexManifest.id),
            manifest: indexManifest
        )
        createdIndexes.insert(newIndex)
        if createdIndexes.contains(existingIndex) {
            createdIndexes.insert(existingIndex)
        } else {
            deletedIndexes.insert(existingIndex)
        }
        await datastore.adopt(index: newIndex)
        
        var rootManifest = try await existingRootObject.manifest(replacing: newIndex.id)
        
        /// Reset the number of entries we are managing.
        rootManifest.descriptor.size = 0
        
        /// No change occured, bail early
        guard existingRootObject.id != rootManifest.id else { return }
        
        let newRootObject = DiskPersistence.Datastore.RootObject(
            datastore: existingRootObject.datastore,
            id: rootManifest.id,
            rootObject: rootManifest
        )
        createdRootObjects.insert(newRootObject)
        if createdRootObjects.contains(existingRootObject) {
            createdRootObjects.remove(existingRootObject)
        } else {
            deletedRootObjects.insert(existingRootObject)
        }
        await datastore.adopt(rootObject: newRootObject)
        rootObjects[datastoreKey] = newRootObject
    }
    
    func persistDirectIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        versionData: Data,
        indexValue: IndexType,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor: some InsertionCursorProtocol,
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "persistDirectIndexEntry(...) called on a read-only transaction!")
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        try await persist(
            entry: DatastorePageEntry(
                headers: [
                    Bytes(versionData),
                    Bytes(try JSONEncoder.shared.encode(indexValue)),
                    Bytes(try JSONEncoder.shared.encode(identifierValue))
                ],
                content: Bytes(instanceData)
            ),
            at: cursor,
            existingRootObject: existingRootObject,
            existingIndex: try await existingRootObject.directIndexes[indexName],
            datastoreKey: datastoreKey
        )
    }
    
    func deleteDirectIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "deleteDirectIndexEntry(...) called on a read-only transaction!")
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        try await delete(
            cursor: cursor,
            existingRootObject: existingRootObject,
            existingIndex: try await existingRootObject.directIndexes[indexName],
            datastoreKey: datastoreKey
        )
    }
    
    func deleteDirectIndex(
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "deleteDirectIndex(...) called on a read-only transaction!")
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        guard let existingIndex = try await existingRootObject.directIndexes[indexName]
        else { throw DatastoreInterfaceError.indexNotFound }
        
        let datastore = existingRootObject.datastore
        
        deletedIndexes.insert(existingIndex)
        
        let rootManifest = try await existingRootObject.manifest(deleting: existingIndex.id)
        
        /// No change occured, bail early
        guard existingRootObject.id != rootManifest.id else { return }
        
        let newRootObject = DiskPersistence.Datastore.RootObject(
            datastore: existingRootObject.datastore,
            id: rootManifest.id,
            rootObject: rootManifest
        )
        createdRootObjects.insert(newRootObject)
        if createdRootObjects.contains(existingRootObject) {
            createdRootObjects.remove(existingRootObject)
        } else {
            deletedRootObjects.insert(existingRootObject)
        }
        await datastore.adopt(rootObject: newRootObject)
        rootObjects[datastoreKey] = newRootObject
    }
    
    func persistSecondaryIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        indexValue: IndexType,
        identifierValue: IdentifierType,
        cursor: some InsertionCursorProtocol,
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "persistSecondaryIndexEntry(...) called on a read-only transaction!")
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        try await persist(
            entry: DatastorePageEntry(
                headers: [
                    Bytes(try JSONEncoder.shared.encode(indexValue))
                ],
                content: Bytes(try JSONEncoder.shared.encode(identifierValue))
            ),
            at: cursor,
            existingRootObject: existingRootObject,
            existingIndex: try await existingRootObject.secondaryIndexes[indexName],
            datastoreKey: datastoreKey
        )
    }
    
    func deleteSecondaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "deleteSecondaryIndexEntry(...) called on a read-only transaction!")
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        try await delete(
            cursor: cursor,
            existingRootObject: existingRootObject,
            existingIndex: try await existingRootObject.secondaryIndexes[indexName],
            datastoreKey: datastoreKey
        )
    }
    
    func deleteSecondaryIndex(
        indexName: IndexName,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "deleteSecondaryIndex(...) called on a read-only transaction!")
        try checkIsActive()
        
        guard let existingRootObject = try await rootObject(for: datastoreKey)
        else { throw DatastoreInterfaceError.datastoreKeyNotFound }
        
        guard let existingIndex = try await existingRootObject.secondaryIndexes[indexName]
        else { throw DatastoreInterfaceError.indexNotFound }
        
        let datastore = existingRootObject.datastore
        
        deletedIndexes.insert(existingIndex)
        
        let rootManifest = try await existingRootObject.manifest(deleting: existingIndex.id)
        
        /// No change occured, bail early
        guard existingRootObject.id != rootManifest.id else { return }
        
        let newRootObject = DiskPersistence.Datastore.RootObject(
            datastore: existingRootObject.datastore,
            id: rootManifest.id,
            rootObject: rootManifest
        )
        createdRootObjects.insert(newRootObject)
        if createdRootObjects.contains(existingRootObject) {
            createdRootObjects.remove(existingRootObject)
        } else {
            deletedRootObjects.insert(existingRootObject)
        }
        await datastore.adopt(rootObject: newRootObject)
        rootObjects[datastoreKey] = newRootObject
    }
}

// MARK: - Observation

extension DiskPersistence {
    typealias EventObserver = AsyncStream<ObservedEvent<Data, ObservationEntry>>.Continuation
}

extension DiskPersistence.Transaction {
    func makeObserver<IdentifierType: Indexable>(
        identifierType: IdentifierType.Type,
        datastoreKey: DatastoreKey,
        bufferingPolicy limit: ObservationBufferingPolicy
    ) async throws -> AsyncCompactMapSequence<AsyncStream<ObservedEvent<Data, ObservationEntry>>, ObservedEvent<IdentifierType, ObservationEntry>> {
#if swift(>=5.9)
        let (stream, observer) = AsyncStream.makeStream(of: ObservedEvent<Data, ObservationEntry>.self, bufferingPolicy: .init(limit))
#else
        var observer: DiskPersistence.EventObserver!
        let stream = AsyncStream(ObservedEvent<Data, ObservationEntry>.self, bufferingPolicy: .init(limit)) { continuation in
            observer = continuation
        }
#endif
        
        let (datastore, _) = try await persistence.persistenceDatastore(for: datastoreKey)
        
        await datastore.register(observer: observer)
        
        return stream.compactMap { event in
            do {
                let decodedID = try JSONDecoder.shared.decode(IdentifierType.self, from: event.id)
                return event.with(id: decodedID)
            } catch {
                return nil
            }
        }
    }
    
    func emit<IdentifierType: Indexable>(
        event: ObservedEvent<IdentifierType, ObservationEntry>,
        datastoreKey: DatastoreKey
    ) async throws {
        assert(!options.contains(.readOnly), "emit(event:...) called on a read-only transaction!")
        try checkIsActive()
        
        guard try await hasObservers(for: datastoreKey) else { return }
        
        let id = try JSONEncoder.shared.encode(event.id)
        entryMutations.append((datastoreKey, event.with(id: id)))
    }
}

// MARK: - Helper Types

fileprivate protocol AnyDiskTransaction {}

fileprivate enum TransactionTaskLocals {
    @TaskLocal
    static var transaction: AnyDiskTransaction?
}
