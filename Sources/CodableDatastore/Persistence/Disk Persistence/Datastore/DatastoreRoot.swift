//
//  DatastoreRoot.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-22.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias DatastoreRootIdentifier = DatedIdentifier<DiskPersistence<ReadOnly>.Datastore.RootObject>

struct DatastoreRootReference: Codable, Hashable {
    var datastoreID: DatastoreIdentifier?
    var datastoreRootID: DatastoreRootIdentifier
    
    init(datastoreID: DatastoreIdentifier, datastoreRootID: DatastoreRootIdentifier) {
        self.datastoreID = datastoreID
        self.datastoreRootID = datastoreRootID
    }
    
    init(from decoder: any Decoder) throws {
        /// Attempt to decode a full object, otherwise fall back to a single value as it was prior to version 0.4 (2024-10-11)
        do {
            let container: KeyedDecodingContainer<CodingKeys> = try decoder.container(keyedBy: CodingKeys.self)
            self.datastoreID = try container.decodeIfPresent(DatastoreIdentifier.self, forKey: .datastoreID)
            self.datastoreRootID = try container.decode(DatastoreRootIdentifier.self, forKey: .datastoreRootID)
        } catch {
            self.datastoreID = nil
            self.datastoreRootID = try decoder.singleValueContainer().decode(DatastoreRootIdentifier.self)
        }
    }
}

extension DiskPersistence.Datastore {
    actor RootObject: Identifiable {
        let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: DatastoreRootIdentifier
        
        nonisolated var referenceID: DatastoreRootReference {
            DatastoreRootReference(datastoreID: datastore.id, datastoreRootID: id)
        }
        
        var _rootObject: DatastoreRootManifest?
        
        var isPersisted: Bool
        
        init(
            datastore: DiskPersistence<AccessMode>.Datastore,
            id: ID,
            rootObject: DatastoreRootManifest? = nil
        ) {
            self.datastore = datastore
            self.id = id
            self._rootObject = rootObject
            self.isPersisted = rootObject == nil
        }
        
        deinit {
            Task { [id, datastore] in
                await datastore.invalidate(id)
            }
        }
    }
}

// MARK: Hashable

extension DiskPersistence.Datastore.RootObject: Hashable {
    static func == (lhs: DiskPersistence<AccessMode>.Datastore.RootObject, rhs: DiskPersistence<AccessMode>.Datastore.RootObject) -> Bool {
        lhs === rhs
    }
    
    nonisolated func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

// MARK: - Common URL Accessors

extension DiskPersistence.Datastore.RootObject {
    /// The URL that points to the root object on disk.
    nonisolated var rootObjectURL: URL {
        datastore.rootURL(for: id)
    }
}

// MARK: - Persistence

extension DiskPersistence.Datastore.RootObject {
    private var rootObject: DatastoreRootManifest {
        get async throws {
            if let _rootObject { return _rootObject }
            
            let data = try Data(contentsOf: rootObjectURL)
            
            let root = try JSONDecoder.shared.decode(DatastoreRootManifest.self, from: data)
            
            isPersisted = true
            _rootObject = root
            
            await datastore.mark(identifier: id, asLoaded: true)
            
            return root
        }
    }
    
    func persistIfNeeded() async throws {
        guard !isPersisted else { return }
        guard let rootObject = _rootObject else {
            assertionFailure("Persisting a root that does not exist.")
            return
        }
        
        /// Make sure the directories exists first.
        try FileManager.default.createDirectory(at: datastore.rootURL, withIntermediateDirectories: true)
        
        /// Encode the provided manifest, and write it to disk.
        let data = try JSONEncoder.shared.encode(rootObject)
        try data.write(to: rootObjectURL, options: .atomic)
        isPersisted = true
        await datastore.mark(identifier: id, asLoaded: true)
    }
}

// MARK: - Manifest

extension DiskPersistence.Datastore.RootObject {
    var manifest: DatastoreRootManifest {
        get async throws { try await rootObject }
    }
}

// MARK: - Indexes

extension DiskPersistence.Datastore.RootObject {
    var primaryIndex: DiskPersistence.Datastore.Index {
        get async throws {
            let primaryIndexManifest = try await rootObject.primaryIndexManifest
            
            return await datastore.index(for: .primary(manifest: primaryIndexManifest))
        }
    }
    
    var directIndexes: [IndexName: DiskPersistence.Datastore.Index] {
        get async throws {
            var indexes: [IndexName: DiskPersistence.Datastore.Index] = [:]
            
            for indexInfo in try await rootObject.directIndexManifests {
                indexes[indexInfo.name] = await datastore.index(for: .direct(index: indexInfo.id, manifest: indexInfo.root))
            }
            
            return indexes
        }
    }
    
    var secondaryIndexes: [IndexName: DiskPersistence.Datastore.Index] {
        get async throws {
            var indexes: [IndexName: DiskPersistence.Datastore.Index] = [:]
            
            for indexInfo in try await rootObject.secondaryIndexManifests {
                indexes[indexInfo.name] = await datastore.index(for: .secondary(index: indexInfo.id, manifest: indexInfo.root))
            }
            
            return indexes
        }
    }
}

// MARK: - Mutations

extension DiskPersistence.Datastore.RootObject {
    func manifest(
        applying descriptor: DatastoreDescriptor
    ) async throws -> (
        manifest: DatastoreRootManifest,
        createdIndexes: Set<DiskPersistence.Datastore.Index>
        /// Note that indexes are not removed here.
    ) {
        let originalManifest = try await manifest
        var manifest = originalManifest
        
        manifest.descriptor.version = descriptor.version
        manifest.descriptor.instanceType = descriptor.instanceType
        manifest.descriptor.identifierType = descriptor.identifierType
        
        var createdIndexes: Set<DiskPersistence.Datastore.Index> = []
        
        if isPersisted {
            manifest.removedIndexes = []
            manifest.removedIndexManifests = []
            manifest.addedIndexes = []
            manifest.addedIndexManifests = []
        }
        
        for (_, indexDescriptor) in descriptor.directIndexes {
            let indexName = indexDescriptor.name
            let indexType = indexDescriptor.type
            var version = indexDescriptor.version
            
            if let originalVersion = originalManifest.descriptor.directIndexes[indexName]?.version {
                version = originalVersion
            } else {
                let indexInfo = DatastoreRootManifest.IndexInfo(
                    name: indexName,
                    id: DatastoreIndexIdentifier(name: indexName),
                    root: DatastoreIndexManifestIdentifier()
                )
                let index = DiskPersistence.Datastore.Index(
                    datastore: datastore,
                    id: .direct(index: indexInfo.id, manifest: indexInfo.root),
                    manifest: DatastoreIndexManifest(
                        id: indexInfo.root,
                        orderedPages: []
                    )
                )
                createdIndexes.insert(index)
                manifest.addedIndexes.insert(.direct(index: indexInfo.id))
                manifest.addedIndexManifests.insert(.direct(index: indexInfo.id, manifest: indexInfo.root))
                manifest.directIndexManifests.append(indexInfo)
            }
            
            manifest.descriptor.directIndexes[indexName] = DatastoreDescriptor.IndexDescriptor(
                version: version,
                name: indexName,
                type: indexType
            )
        }
        
        for (_, indexDescriptor) in descriptor.referenceIndexes {
            let indexName = indexDescriptor.name
            let indexType = indexDescriptor.type
            var version = indexDescriptor.version
            
            if let originalVersion = originalManifest.descriptor.referenceIndexes[indexName]?.version {
                version = originalVersion
            } else {
                let indexInfo = DatastoreRootManifest.IndexInfo(
                    name: indexName,
                    id: DatastoreIndexIdentifier(name: indexName),
                    root: DatastoreIndexManifestIdentifier()
                )
                let index = DiskPersistence.Datastore.Index(
                    datastore: datastore,
                    id: .secondary(index: indexInfo.id, manifest: indexInfo.root),
                    manifest: DatastoreIndexManifest(
                        id: indexInfo.root,
                        orderedPages: []
                    )
                )
                createdIndexes.insert(index)
                manifest.addedIndexes.insert(.secondary(index: indexInfo.id))
                manifest.addedIndexManifests.insert(.secondary(index: indexInfo.id, manifest: indexInfo.root))
                manifest.secondaryIndexManifests.append(indexInfo)
            }
            
            manifest.descriptor.referenceIndexes[indexName] = DatastoreDescriptor.IndexDescriptor(
                version: version,
                name: indexName,
                type: indexType
            )
        }
        
        if originalManifest.descriptor != manifest.descriptor {
            let modificationDate = Date()
            manifest.id = DatastoreRootIdentifier(date: modificationDate)
            manifest.modificationDate = modificationDate
            return (manifest: manifest, createdIndexes: createdIndexes)
        }
        return (manifest: originalManifest, createdIndexes: createdIndexes)
    }
    
    func manifest(
        replacing index: DiskPersistence.Datastore.Index.ID
    ) async throws -> DatastoreRootManifest {
        let manifest = try await manifest
        var updatedManifest = manifest
        
        var removedIndex: DatastoreRootManifest.IndexManifestID?
        var addedIndex: DatastoreRootManifest.IndexManifestID
        
        switch index {
        case .primary(let manifestID):
            removedIndex = .primary(manifest: updatedManifest.primaryIndexManifest)
            addedIndex = .primary(manifest: manifestID)
            updatedManifest.primaryIndexManifest = manifestID
        case .direct(let indexID, let manifestID):
            var oldRoot: DatastoreIndexManifestIdentifier?
            updatedManifest.directIndexManifests = manifest.directIndexManifests.map { indexInfo in
                var indexInfo = indexInfo
                if indexInfo.id == indexID {
                    oldRoot = indexInfo.root
                    indexInfo.root = manifestID
                }
                return indexInfo
            }
            
            removedIndex = oldRoot.map { .direct(index: indexID, manifest: $0) }
            addedIndex = .direct(index: indexID, manifest: manifestID)
        case .secondary(let indexID, let manifestID):
            var oldRoot: DatastoreIndexManifestIdentifier?
            updatedManifest.secondaryIndexManifests = updatedManifest.secondaryIndexManifests.map { indexInfo in
                var indexInfo = indexInfo
                if indexInfo.id == indexID {
                    oldRoot = indexInfo.root
                    indexInfo.root = manifestID
                }
                return indexInfo
            }
            
            removedIndex = oldRoot.map { .secondary(index: indexID, manifest: $0) }
            addedIndex = .secondary(index: indexID, manifest: manifestID)
        }
        
        if manifest != updatedManifest {
            let modificationDate = Date()
            updatedManifest.id = DatastoreRootIdentifier(date: modificationDate)
            updatedManifest.modificationDate = modificationDate
            
            if isPersisted {
                updatedManifest.removedIndexes = []
                updatedManifest.removedIndexManifests = []
                updatedManifest.addedIndexes = []
                updatedManifest.addedIndexManifests = []
            }
            
            if let removedIndex {
                if updatedManifest.addedIndexManifests.contains(removedIndex) {
                    updatedManifest.addedIndexManifests.remove(removedIndex)
                } else {
                    updatedManifest.removedIndexManifests.insert(removedIndex)
                }
            }
            updatedManifest.addedIndexManifests.insert(addedIndex)
        }
        return updatedManifest
    }
    
    func manifest(
        deleting index: DiskPersistence.Datastore.Index.ID
    ) async throws -> DatastoreRootManifest {
        let manifest = try await manifest
        var updatedManifest = manifest
        
        var removedIndex: DatastoreRootManifest.IndexManifestID
        var addedIndex: DatastoreRootManifest.IndexManifestID?
        
        switch index {
        case .primary(let manifestID):
            removedIndex = .primary(manifest: manifestID)
            /// Primary index must have _a_ root, so make a new one.
            let newManifestID = DatastoreIndexManifestIdentifier()
            addedIndex = .primary(manifest: newManifestID)
            updatedManifest.primaryIndexManifest = newManifestID
        case .direct(let indexID, let manifestID):
            removedIndex = .direct(index: indexID, manifest: manifestID)
            if let entryIndex = updatedManifest.directIndexManifests.firstIndex(where: { $0.id == indexID }) {
                let indexName = updatedManifest.directIndexManifests[entryIndex].name
                updatedManifest.directIndexManifests.remove(at: entryIndex)
                updatedManifest.descriptor.directIndexes.removeValue(forKey: indexName)
            }
        case .secondary(let indexID, let manifestID):
            removedIndex = .secondary(index: indexID, manifest: manifestID)
            if let entryIndex = updatedManifest.secondaryIndexManifests.firstIndex(where: { $0.id == indexID }) {
                let indexName = updatedManifest.secondaryIndexManifests[entryIndex].name
                updatedManifest.secondaryIndexManifests.remove(at: entryIndex)
                updatedManifest.descriptor.referenceIndexes.removeValue(forKey: indexName)
            }
        }
        
        if manifest != updatedManifest {
            let modificationDate = Date()
            updatedManifest.id = DatastoreRootIdentifier(date: modificationDate)
            updatedManifest.modificationDate = modificationDate
            
            if isPersisted {
                updatedManifest.removedIndexes = []
                updatedManifest.removedIndexManifests = []
                updatedManifest.addedIndexes = []
                updatedManifest.addedIndexManifests = []
            }
            
            if updatedManifest.addedIndexManifests.contains(removedIndex) {
                updatedManifest.addedIndexManifests.remove(removedIndex)
            } else {
                updatedManifest.removedIndexManifests.insert(removedIndex)
            }
            
            if let addedIndex {
                updatedManifest.addedIndexManifests.insert(addedIndex)
            }
        }
        return updatedManifest
    }
}

