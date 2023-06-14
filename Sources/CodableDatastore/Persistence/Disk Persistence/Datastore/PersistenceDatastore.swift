//
//  PersistenceDatastore.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias DatastoreIdentifier = TypedIdentifier<DiskPersistence<ReadOnly>.Datastore>
typealias DatastoreRootIdentifier = DatedIdentifier<DiskPersistence<ReadOnly>.Datastore>

extension DiskPersistence {
    actor Datastore {
        let id: DatastoreIdentifier
        
        unowned let snapshot: Snapshot<AccessMode>
        
        var currentRootIdentifier: DatastoreRootIdentifier?
        var cachedDescriptor: DatastoreDescriptor?
        
        var lastUpdateDescriptorTask: Task<Any, Error>?
        
        init(
            id: DatastoreIdentifier,
            snapshot: Snapshot<AccessMode>
        ) {
            self.id = id
            self.snapshot = snapshot
        }
    }
}

// MARK: - Common URL Accessors
extension DiskPersistence.Datastore {
    /// The URL that points to the Snapshot directory.
    nonisolated var datastoreURL: URL {
        snapshot
            .datastoresURL
            .appendingPathComponent("\(id).datastore", isDirectory: true)
    }
    
    /// The URL that points to the Root directory.
    nonisolated var rootURL: URL {
        datastoreURL
            .appendingPathComponent("Root", isDirectory: true)
    }
    
    /// The URL that points to the DirectIndexes directory.
    nonisolated var directIndexesURL: URL {
        datastoreURL
            .appendingPathComponent("DirectIndexes", isDirectory: true)
    }
    
    /// The URL that points to the SecondaryIndexes directory.
    nonisolated var secondaryIndexesURL: URL {
        datastoreURL
            .appendingPathComponent("SecondaryIndexes", isDirectory: true)
    }
}
