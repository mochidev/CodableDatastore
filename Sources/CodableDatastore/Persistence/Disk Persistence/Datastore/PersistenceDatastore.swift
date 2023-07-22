//
//  PersistenceDatastore.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias DatastoreIdentifier = TypedIdentifier<DiskPersistence<ReadOnly>.Datastore>

struct WeakValue<T: AnyObject> {
    weak var value: T?
    
    init(_ value: T) {
        self.value = value
    }
}

extension DiskPersistence {
    actor Datastore {
        let id: DatastoreIdentifier
        
        unowned let snapshot: Snapshot<AccessMode>
        
        var cachedRootObject: DatastoreRootManifest?
        
        var lastUpdateDescriptorTask: Task<Any, Error>?
        
        /// The root objects that are being tracked in memory.
        var trackedRootObjects: [RootObject.ID : WeakValue<RootObject>] = [:]
        var trackedIndexes: [Index.ID : WeakValue<Index>] = [:]
        var trackedPages: [Page.ID : WeakValue<Page>] = [:]
        
        /// The root objects on the file system that are actively loaded in memory.
        var loadedRootObjects: Set<RootObject.ID> = []
        var loadedIndexes: Set<Index.ID> = []
        var loadedPages: Set<Page.ID> = []
        
        typealias ObserverID = Int
        fileprivate var nextObserverID: ObserverID = 0
        var observers: [ObserverID : EventObserver] = [:]
        
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
    
    nonisolated func indexURL(for indexID: Index.ID) -> URL {
        switch indexID {
        case .primary:
            return directIndexesURL
                .appendingPathComponent("Primary.datastoreindex", isDirectory: true)
        case .direct(let indexID, _):
            return directIndexesURL
                .appendingPathComponent("\(indexID).datastoreindex", isDirectory: true)
        case .secondary(let indexID, _):
            return secondaryIndexesURL
                .appendingPathComponent("\(indexID).datastoreindex", isDirectory: true)
        }
    }
    
    nonisolated func manifestsURL(for indexID: Index.ID) -> URL {
        indexURL(for: indexID)
            .appendingPathComponent("Manifest", isDirectory: true)
    }
    
    nonisolated func pagesURL(for indexID: Index.ID) -> URL {
        indexURL(for: indexID)
            .appendingPathComponent("Pages", isDirectory: true)
    }
}

// MARK: - Root Object Management

extension DiskPersistence.Datastore {
    func rootObject(for identifier: RootObject.ID) -> RootObject {
        if let rootObject = trackedRootObjects[identifier]?.value {
            return rootObject
        }
//        print("🤷 Cache Miss: Root \(identifier)")
        let rootObject = RootObject(datastore: self, id: identifier)
        trackedRootObjects[identifier] = WeakValue(rootObject)
        Task { await snapshot.persistence.cache(rootObject) }
        return rootObject
    }
    
    func adopt(rootObject: RootObject) {
        trackedRootObjects[rootObject.id] = WeakValue(rootObject)
        Task { await snapshot.persistence.cache(rootObject) }
    }
    
    func invalidate(_ identifier: RootObject.ID) {
        trackedRootObjects.removeValue(forKey: identifier)
    }
    
    func mark(identifier: RootObject.ID, asLoaded: Bool) {
        if asLoaded {
            loadedRootObjects.insert(identifier)
        } else {
            loadedRootObjects.remove(identifier)
        }
    }
    
    func index(for identifier: Index.ID) -> Index {
        if let index = trackedIndexes[identifier]?.value {
            return index
        }
//        print("🤷 Cache Miss: Index \(identifier)")
        let index = Index(datastore: self, id: identifier)
        trackedIndexes[identifier] = WeakValue(index)
        Task { await snapshot.persistence.cache(index) }
        return index
    }
    
    func adopt(index: Index) {
        trackedIndexes[index.id] = WeakValue(index)
        Task { await snapshot.persistence.cache(index) }
    }
    
    func invalidate(_ identifier: Index.ID) {
        trackedIndexes.removeValue(forKey: identifier)
    }
    
    func mark(identifier: Index.ID, asLoaded: Bool) {
        if asLoaded {
            loadedIndexes.insert(identifier)
        } else {
            loadedIndexes.remove(identifier)
        }
    }
    
    func page(for identifier: Page.ID) -> Page {
        if let page = trackedPages[identifier.withoutManifest]?.value {
            return page
        }
//        print("🤷 Cache Miss: Page \(identifier.page)")
        let page = Page(datastore: self, id: identifier)
        trackedPages[identifier.withoutManifest] = WeakValue(page)
        Task { await snapshot.persistence.cache(page) }
        return page
    }
    
    func adopt(page: Page) {
        trackedPages[page.id.withoutManifest] = WeakValue(page)
        Task { await snapshot.persistence.cache(page) }
    }
    
    func invalidate(_ identifier: Page.ID) {
        trackedPages.removeValue(forKey: identifier.withoutManifest)
    }
    
    func mark(identifier: Page.ID, asLoaded: Bool) {
        if asLoaded {
            loadedPages.insert(identifier.withoutManifest)
        } else {
            loadedPages.remove(identifier.withoutManifest)
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

// MARK: - Observations

extension DiskPersistence.Datastore {
    func register(
        observer: DiskPersistence.EventObserver
    ) {
        let id = nextObserverID
        nextObserverID += 1
        observers[id] = observer
        observer.onTermination = { _ in
            Task {
                await self.unregisterObserver(for: id)
            }
        }
    }
    
    private func unregisterObserver(for id: ObserverID) {
        observers.removeValue(forKey: id)
    }
    
    func emit(
        _ event: ObservedEvent<Data, ObservationEntry>
    ) {
        for (_, observer) in observers {
            observer.yield(event)
        }
    }
    
    var hasObservers: Bool { !observers.isEmpty }
}
