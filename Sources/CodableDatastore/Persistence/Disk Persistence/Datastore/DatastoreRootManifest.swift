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
    var primaryIndexManifest: IndexInfo
    
    /// A pointer to the direct indexes' root objects.
    var directIndexManifests: [String: IndexInfo] = [:]
    
    /// A pointer to the secondary indexes' root objects.
    var secondaryIndexManifests: [String: IndexInfo] = [:]
}

extension DatastoreRootManifest {
    struct IndexInfo: Codable, Equatable, Identifiable {
        /// The key this index uses.
        var key: String
        
        /// The identifier for the index on disk.
        var id: DatastoreIdentifier
        
        /// The root object of the index.
        var root: DatastoreRootIdentifier
    }
}
