//
//  DatastoreRootManifest.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-14.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// Versions supported by ``DiskPersisitence``.
///
/// These are used when dealing with format changes at the library level.
enum DatastoreRootManifestVersion: String, Codable {
    case alpha
}

struct DatastoreRootManifest: Codable, Equatable, Identifiable {
    /// The version of the root object, used when dealing with format changes at the library level.
    var version: DatastoreRootManifestVersion = .alpha
    
    /// The identifier for this root object.
    var id: DatastoreRootIdentifier
    
    /// The last modification date of the root object.
    var modificationDate: Date
    
    /// The descriptor of the datastore in its current state.
    var descriptor: DatastoreDescriptor
    
    /// A pointer to the primary index's root object.
    var primaryIndexManifest: DatastoreIndexManifestIdentifier
    
    /// A pointer to the direct indexes' root objects.
    var directIndexManifests: [IndexInfo] = []
    
    /// A pointer to the secondary indexes' root objects.
    var secondaryIndexManifests: [IndexInfo] = []
    
    /// The indexes that have been added in this iteration of the snapshot.
    var addedIndexes: Set<IndexID> = []
    
    /// The indexes that have been completely removed in this iteration of the snapshot.
    var removedIndexes: Set<IndexID> = []
    
    /// The datastore roots that have been added in this iteration of the snapshot.
    var addedIndexManifests: Set<IndexManifestID> = []
    
    /// The datastore roots that have been replaced in this iteration of the snapshot.
    var removedIndexManifests: Set<IndexManifestID> = []
}

extension DatastoreRootManifest {
    struct IndexInfo: Codable, Equatable, Identifiable {
        /// The key this index uses.
        var name: IndexName
        
        /// The identifier for the index on disk.
        var id: DatastoreIndexIdentifier
        
        /// The root object of the index.
        var root: DatastoreIndexManifestIdentifier
    }
    
    enum IndexID: Codable, Hashable {
        case primary
        case direct(index: DatastoreIndexIdentifier)
        case secondary(index: DatastoreIndexIdentifier)
    }
    
    enum IndexManifestID: Codable, Hashable {
        case primary(manifest: DatastoreIndexManifestIdentifier)
        case direct(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
        case secondary(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
        
        init<AccessMode>(_ id: DiskPersistence<AccessMode>.Datastore.Index.ID) {
            switch id {
            case .primary(let manifest):
                self = .primary(manifest: manifest)
            case .direct(let index, let manifest):
                self = .direct(index: index, manifest: manifest)
            case .secondary(let index, let manifest):
                self = .secondary(index: index, manifest: manifest)
            }
        }
    }
}
