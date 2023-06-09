//
//  DatastoreRoot.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-22.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias DatastoreRootIdentifier = DatedIdentifier<DiskPersistence<ReadOnly>.Datastore.RootObject>

extension DiskPersistence.Datastore {
    actor RootObject: Identifiable {
        unowned let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: DatastoreRootIdentifier
        
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
        datastore.rootURL.appendingPathComponent("\(id).json", isDirectory: false)
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
    
    var directIndexes: [String: DiskPersistence.Datastore.Index] {
        get async throws {
            var indexes: [String: DiskPersistence.Datastore.Index] = [:]
            
            for indexInfo in try await rootObject.directIndexManifests {
                indexes[indexInfo.key] = await datastore.index(for: .direct(index: indexInfo.id, manifest: indexInfo.root))
            }
            
            return indexes
        }
    }
    
    var secondaryIndexes: [String: DiskPersistence.Datastore.Index] {
        get async throws {
            var indexes: [String: DiskPersistence.Datastore.Index] = [:]
            
            for indexInfo in try await rootObject.secondaryIndexManifests {
                indexes[indexInfo.key] = await datastore.index(for: .secondary(index: indexInfo.id, manifest: indexInfo.root))
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
        manifest.descriptor.codedType = descriptor.codedType
        manifest.descriptor.identifierType = descriptor.identifierType
        
        var createdIndexes: Set<DiskPersistence.Datastore.Index> = []
        
        for (_, indexDescriptor) in descriptor.directIndexes {
            let key = indexDescriptor.key
            let indexType = indexDescriptor.indexType
            var version = indexDescriptor.version
            
            if let originalVersion = originalManifest.descriptor.directIndexes[key]?.version {
                version = originalVersion
            } else {
                let indexInfo = DatastoreRootManifest.IndexInfo(
                    key: key,
                    id: DatastoreIndexIdentifier(name: key),
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
            }
            
            manifest.descriptor.directIndexes[key] = DatastoreDescriptor.IndexDescriptor(
                version: version,
                key: key,
                indexType: indexType
            )
        }
        
        for (_, indexDescriptor) in descriptor.secondaryIndexes {
            let key = indexDescriptor.key
            let indexType = indexDescriptor.indexType
            var version = indexDescriptor.version
            
            if let originalVersion = originalManifest.descriptor.secondaryIndexes[key]?.version {
                version = originalVersion
            } else {
                let indexInfo = DatastoreRootManifest.IndexInfo(
                    key: key,
                    id: DatastoreIndexIdentifier(name: key),
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
            }
            
            manifest.descriptor.secondaryIndexes[key] = DatastoreDescriptor.IndexDescriptor(
                version: version,
                key: key,
                indexType: indexType
            )
        }
        
        if originalManifest.descriptor != manifest.descriptor {
            manifest.id = DatastoreRootIdentifier()
            manifest.modificationDate = Date()
        }
        return (manifest: manifest, createdIndexes: createdIndexes)
    }
    
    func manifest(
        replacing index: DiskPersistence.Datastore.Index.ID
    ) async throws -> DatastoreRootManifest {
        let manifest = try await manifest
        var updatedManifest = manifest
        
        switch index {
        case .primary(let manifestID):
            updatedManifest.primaryIndexManifest = manifestID
        case .direct(let indexID, let manifestID):
            updatedManifest.directIndexManifests = manifest.directIndexManifests.map { indexInfo in
                var indexInfo = indexInfo
                if indexInfo.id == indexID {
                    indexInfo.root = manifestID
                }
                return indexInfo
            }
        case .secondary(let indexID, let manifestID):
            updatedManifest.secondaryIndexManifests = updatedManifest.secondaryIndexManifests.map { indexInfo in
                var indexInfo = indexInfo
                if indexInfo.id == indexID {
                    indexInfo.root = manifestID
                }
                return indexInfo
            }
        }
        
        if manifest != updatedManifest {
            updatedManifest.id = DatastoreRootIdentifier()
            updatedManifest.modificationDate = Date()
        }
        return updatedManifest
    }
}

