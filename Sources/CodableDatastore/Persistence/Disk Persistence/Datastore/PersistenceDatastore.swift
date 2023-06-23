//
//  PersistenceDatastore.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias DatastoreIdentifier = TypedIdentifier<DiskPersistence<ReadOnly>.Datastore>

extension DiskPersistence {
    actor Datastore {
        let id: DatastoreIdentifier
        
        unowned let snapshot: Snapshot<AccessMode>
        
        var cachedRootObject: DatastoreRootManifest?
        
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

// MARK: - Root Object Management
extension DiskPersistence.Datastore {
    /// Load the root object from disk for the given identifier.
    func loadRootObject(for rootIdentifier: DatastoreRootIdentifier) throws -> DatastoreRootManifest {
        let rootObjectURL = rootURL.appendingPathComponent("\(rootIdentifier).json", isDirectory: false)
        
        let data = try Data(contentsOf: rootObjectURL)
        
        let root = try JSONDecoder.shared.decode(DatastoreRootManifest.self, from: data)
        
        cachedRootObject = root
        return root
    }
    
    /// Write the specified manifest to the store, and cache the results in ``DiskPersistence.Datastore/cachedRootObject``.
    func write(manifest: DatastoreRootManifest) throws where AccessMode == ReadWrite {
        /// Make sure the directories exists first.
        if cachedRootObject == nil {
            try FileManager.default.createDirectory(at: datastoreURL, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: directIndexesURL, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: secondaryIndexesURL, withIntermediateDirectories: true)
        }
        
        let rootObjectURL = rootURL.appendingPathComponent("\(manifest.id).json", isDirectory: false)
        
        /// Encode the provided manifest, and write it to disk.
        let data = try JSONEncoder.shared.encode(manifest)
        try data.write(to: rootObjectURL, options: .atomic)
        
        /// Update the cache since we know what it should be.
        cachedRootObject = manifest
    }
}
