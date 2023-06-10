//
//  DiskPersistenceError.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-09.
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

/// A ``DiskPersistence``-specific internal error.
public enum DiskPersistenceInternalError: LocalizedError {
    /// A request to update store info failed as an update was made in an inconsistent state
    case nestedStoreWrite
    
    /// A request to update snapshot manifest failed as an update was made in an inconsistent state
    case nestedSnapshotWrite
    
    public var errorDescription: String? {
        switch self {
        case .nestedStoreWrite:
            return "An internal error caused the store to be modified while it was being modified. Please report reproduction steps if found!"
        case .nestedSnapshotWrite:
            return "An internal error caused a snapshot to be modified while it was being modified. Please report reproduction steps if found!"
        }
    }
}
