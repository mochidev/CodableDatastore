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
    
    func persistIfNeeded() throws {
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
    }
}

// MARK: - Descriptor

extension DiskPersistence.Datastore.RootObject {
    var descriptor: DatastoreDescriptor {
        get async throws { try await rootObject.descriptor }
    }
}

// MARK: - Indexes

extension DiskPersistence.Datastore.RootObject {
    var primaryIndex: DiskPersistence.Datastore.Index {
        get async throws {
            let primaryIndexInfo = try await rootObject.primaryIndexManifest
            
            return await datastore.index(for: .primary(manifest: primaryIndexInfo.root))
        }
    }
    
    var directIndexes: [DatastoreIndexIdentifier: DiskPersistence.Datastore.Index] {
        get async throws {
            var indexes: [DatastoreIndexIdentifier: DiskPersistence.Datastore.Index] = [:]
            
            for (_, indexInfo) in try await rootObject.directIndexManifests {
                indexes[indexInfo.id] = await datastore.index(for: .direct(index: indexInfo.id, manifest: indexInfo.root))
            }
            
            return indexes
        }
    }
    
    var secondaryIndexes: [DatastoreIndexIdentifier: DiskPersistence.Datastore.Index] {
        get async throws {
            var indexes: [DatastoreIndexIdentifier: DiskPersistence.Datastore.Index] = [:]
            
            for (_, indexInfo) in try await rootObject.secondaryIndexManifests {
                indexes[indexInfo.id] = await datastore.index(for: .secondary(index: indexInfo.id, manifest: indexInfo.root))
            }
            
            return indexes
        }
    }
}

