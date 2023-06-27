//
//  DatastoreIndex.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-23.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias DatastoreIndexIdentifier = TypedIdentifier<DiskPersistence<ReadOnly>.Datastore.Index>

extension DiskPersistence.Datastore {
    actor Index: Identifiable {
        unowned let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: ID
        
        var _manifest: DatastoreIndexManifest?
        
        var isPersisted: Bool
        
        init(
            datastore: DiskPersistence<AccessMode>.Datastore,
            id: ID,
            manifest: DatastoreIndexManifest? = nil
        ) {
            self.datastore = datastore
            self.id = id
            self._manifest = manifest
            self.isPersisted = manifest == nil
        }
    }
}

// MARK: - Helper Types

extension DiskPersistence.Datastore.Index {
    enum ID: Hashable {
        case primary(manifest: DatastoreIndexManifestIdentifier)
        case direct(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
        case secondary(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
        
        var manifestID: DatastoreIndexManifestIdentifier {
            switch self {
            case .primary(let id),
                 .direct(_, let id),
                 .secondary(_, let id):
                return id
            }
        }
    }
}

// MARK: - Common URL Accessors

extension DiskPersistence.Datastore.Index {
    /// The URL that points to the index.
    nonisolated var indexURL: URL {
        switch id {
        case .primary:
            return datastore
                .directIndexesURL
                .appendingPathComponent("Primary.datastoreindex", isDirectory: true)
        case .direct(let indexID, _):
            return datastore
                .directIndexesURL
                .appendingPathComponent("\(indexID).datastoreindex", isDirectory: true)
        case .secondary(let indexID, _):
            return datastore
                .secondaryIndexesURL
                .appendingPathComponent("\(indexID).datastoreindex", isDirectory: true)
        }
    }
    
    /// The URL that points to the Manifest directory.
    nonisolated var manifestsURL: URL {
        indexURL.appendingPathComponent("Manifest", isDirectory: true)
    }
    
    /// The URL that points to the Manifest directory.
    nonisolated var pagesURL: URL {
        indexURL.appendingPathComponent("Pages", isDirectory: true)
    }
    
    /// The URL that points to the manifest.
    nonisolated var manifestURL: URL {
        switch id {
        case .primary(let manifestID):
            return datastore
                .directIndexesURL
                .appendingPathComponent("Primary.datastoreindex", isDirectory: true)
                .appendingPathComponent("Manifest", isDirectory: true)
                .appendingPathComponent("\(manifestID).indexmanifest", isDirectory: false)
        case .direct(let indexID, let manifestID):
            return datastore
                .directIndexesURL
                .appendingPathComponent("\(indexID).datastoreindex", isDirectory: true)
                .appendingPathComponent("Manifest", isDirectory: true)
                .appendingPathComponent("\(manifestID).indexmanifest", isDirectory: false)
        case .secondary(let indexID, let manifestID):
            return datastore
                .secondaryIndexesURL
                .appendingPathComponent("\(indexID).datastoreindex", isDirectory: true)
                .appendingPathComponent("Manifest", isDirectory: true)
                .appendingPathComponent("\(manifestID).indexmanifest", isDirectory: false)
        }
    }
}

// MARK: - Persistence

extension DiskPersistence.Datastore.Index {
    private var manifest: DatastoreIndexManifest {
        get async throws {
            if let _manifest { return _manifest }
            
            let manifest = try await DatastoreIndexManifest(contentsOf: manifestURL, id: id.manifestID)
            
            isPersisted = true
            _manifest = manifest
            
            await datastore.mark(identifier: id, asLoaded: true)
            
            return manifest
        }
    }
    
    func persistIfNeeded() throws {
        guard !isPersisted else { return }
        guard let manifest = _manifest else {
            assertionFailure("Persisting a manifest that does not exist.")
            return
        }
        
        /// Make sure the directories exists first.
        try FileManager.default.createDirectory(at: manifestsURL, withIntermediateDirectories: true)
        
        /// Encode the provided manifest, and write it to disk.
        let data = Data(manifest.bytes)
        try data.write(to: manifestURL, options: .atomic)
    }
}
