//
//  DiskPersistence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A ``DiskPersistence``-specific error.
public enum DiskPersistenceError: LocalizedError {
    /// The specified URL for the persistence store is not a file URL.
    case notFileURL
    
    /// A default store could not be created due to a missing main bundle ID.
    case missingBundleID
    
    /// A default store could not be created due to a missing application support directory.
    case missingAppSupportDirectory
    
    public var errorDescription: String? {
        switch self {
        case .notFileURL:
            return "The persistence store cannot be saved to the specified URL."
        case .missingBundleID:
            return "The persistence store cannot be saved to the default URL as it is not running in the context of an app."
        case .missingAppSupportDirectory:
            return "The persistence store cannot be saved to the default URL as an Application Support directory could built for this system."
        }
    }
}

public actor DiskPersistence<AccessMode: _AccessMode>: Persistence {
    /// The location of this persistence.
    let storeURL: URL
    
    var cachedStoreInfo: StoreInfo?
    
    /// Initialize a ``DiskPersistence`` with a read-write URL.
    ///
    /// Use this initializer when creating a persistence from the main process that will access it, such as your app. To access the same persistence from another process, use ``init(readOnlyURL:)`` instead.
    ///
    /// - Throws: The URL must be a file URL. If it isn't, a ``DiskPersistenceError/notFileURL`` error will be thrown.
    public init(readWriteURL: URL) throws where AccessMode == ReadWrite {
        guard readWriteURL.isFileURL else {
            throw DiskPersistenceError.notFileURL
        }
        
        storeURL = readWriteURL
    }
    
    /// Initialize a ``DiskPersistence`` with a read-only URL.
    ///
    /// Use this initializer when you want to access a persistence that is owned by another primary process, which is commonly the case with extensions of apps. This gives you a safe read-only view of the persistence store with no risk of losing data should the main app be active at the same time.
    public init(readOnlyURL: URL) where AccessMode == ReadOnly {
        storeURL = readOnlyURL
    }
    
    /// The default URL to use for disk persistences.
    static var defaultURL: URL {
        // TODO: Make non-throwing: https://github.com/mochidev/CodableDatastore/issues/15
        get throws {
            guard let appName = Bundle.main.bundleIdentifier else {
                throw DiskPersistenceError.missingBundleID
            }
            
            let persistenceName = "DefaultStore.persistencestore"
            
#if os(Linux)
            guard let applicationSupportDirectory = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else {
                throw DiskPersistenceError.missingAppSupportDirectory
            }
            return applicationSupportDirectory
                .appendingPathComponent(appName, isDirectory: true)
                .appendingPathComponent(persistenceName, isDirectory: true)
#else
            if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 8, *) {
                return URL
                    .applicationSupportDirectory
                    .appending(component: appName, directoryHint: .isDirectory)
                    .appending(component: persistenceName, directoryHint: .isDirectory)
            } else {
                guard let applicationSupportDirectory = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else {
                    throw DiskPersistenceError.missingAppSupportDirectory
                }
                return applicationSupportDirectory
                    .appendingPathComponent(appName, isDirectory: true)
                    .appendingPathComponent(persistenceName, isDirectory: true)
            }
#endif
        }
    }
}

extension DiskPersistence where AccessMode == ReadWrite {
    /// The default persistence for the read-write store of an app.
    public static func defaultStore() throws -> DiskPersistence<AccessMode> {
        // TODO: Make this a static property: https://github.com/mochidev/CodableDatastore/issues/17
        let url = try defaultURL
        
        // This is safe since default URL is always a file URL.
        return try! DiskPersistence(readWriteURL: url)
    }
}

extension DiskPersistence where AccessMode == ReadOnly {
    /// The default persistence for the read-only store of an app.
    public static func readOnlyDefaultStore() throws -> DiskPersistence<AccessMode> {
        // TODO: Make this a static property: https://github.com/mochidev/CodableDatastore/issues/17
        return try DiskPersistence(readOnlyURL: defaultURL)
    }
}

extension DiskPersistence {
    var snapshotsURL: URL {
        storeURL.appendingPathComponent("Snapshots", isDirectory: true)
    }
    
    var backupsURL: URL {
        storeURL.appendingPathComponent("Backups", isDirectory: true)
    }
    
    var storeInfoURL: URL {
        storeURL.appendingPathComponent("Info.json", isDirectory: false)
    }
}

extension DiskPersistence {
    /// Load the store info from disk, or create a suitable starting value if such a file does not exist.
    func loadStoreInfo() throws -> StoreInfo {
        do {
            let data = try Data(contentsOf: storeInfoURL)
            
            let storeInfoDecoder = JSONDecoder()
            storeInfoDecoder.dateDecodingStrategy = .iso8601WithMilliseconds
            let storeInfo = try storeInfoDecoder.decode(StoreInfo.self, from: data)
            
            cachedStoreInfo = storeInfo
            return storeInfo
        } catch URLError.fileDoesNotExist, CocoaError.fileReadNoSuchFile {
            return StoreInfo(modificationDate: Date())
        } catch let error as NSError where error.domain == NSPOSIXErrorDomain && error.code == Int(POSIXError.ENOENT.rawValue) {
            return StoreInfo(modificationDate: Date())
        } catch {
            throw error
        }
    }
    
    /// Write the specified store info to the store, and cache the results in ``DiskPersistence/cachedStoreInfo``.
    func write(_ storeInfo: StoreInfo) throws {
        let storeInfoEncoder = JSONEncoder()
        storeInfoEncoder.dateEncodingStrategy = .iso8601WithMilliseconds
        let data = try storeInfoEncoder.encode(storeInfo)
        try data.write(to: storeInfoURL, options: .atomic)
        cachedStoreInfo = storeInfo
    }
}

extension DiskPersistence where AccessMode == ReadWrite {
    /// Create the persistence store if necessary.
    ///
    /// It is useful to call this if you wish for stub directories to be created immediately before a data store
    /// is actually written to the disk.
    public func createPersistenceIfNecessary() async throws {
        try FileManager.default.createDirectory(at: storeURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: snapshotsURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: backupsURL, withIntermediateDirectories: true)
        
        // Load the store info, so we can see if we'll need to write it or not.
        let storeInfo = try loadStoreInfo()
        // If the cached store info is nil, we didn't have one already, so write the one we got back to disk.
        if (cachedStoreInfo == nil) {
            try write(storeInfo)
        }
    }
}

extension DiskPersistence: _Persistence {
    public func withTransaction(_ transaction: (DiskPersistence) -> ()) async throws {
        
    }
}
