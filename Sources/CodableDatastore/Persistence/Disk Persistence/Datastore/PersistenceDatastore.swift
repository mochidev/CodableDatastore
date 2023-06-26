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
        
        /// The root objects that are being tracked in memory.
        var trackedRootObjects: [RootObject.ID : RootObject] = [:]
        var trackedIndex: [Index.ID : Index] = [:]
        var trackedPages: [Page.ID : Page] = [:]
        
        /// The root objects on the file system that are actively loaded in memory.
        var loadedRootObjects: Set<RootObject.ID> = []
        var loadedIndex: Set<Index.ID> = []
        var loadedPages: Set<Page.ID> = []
        
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
    func rootObject(for identifier: RootObject.ID) -> RootObject {
        if let rootObject = trackedRootObjects[identifier] {
            return rootObject
        }
        let rootObject = RootObject(datastore: self, id: identifier)
        trackedRootObjects[identifier] = rootObject
        return rootObject
    }
    
    func mark(identifier: RootObject.ID, asLoaded: Bool) {
        if asLoaded {
            loadedRootObjects.insert(identifier)
        } else {
            loadedRootObjects.remove(identifier)
        }
    }
    
    func index(for identifier: Index.ID) -> Index {
        if let index = trackedIndex[identifier] {
            return index
        }
        let index = Index(datastore: self, id: identifier)
        trackedIndex[identifier] = index
        return index
    }
    
    func mark(identifier: Index.ID, asLoaded: Bool) {
        if asLoaded {
            loadedIndex.insert(identifier)
        } else {
            loadedIndex.remove(identifier)
        }
    }
    
    func page(for identifier: Page.ID) -> Page {
        if let page = trackedPages[identifier] {
            return page
        }
        let page = Page(datastore: self, id: identifier)
        trackedPages[identifier] = page
        return page
    }
    
    func mark(identifier: Page.ID, asLoaded: Bool) {
        if asLoaded {
            loadedPages.insert(identifier)
        } else {
            loadedPages.remove(identifier)
        }
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
