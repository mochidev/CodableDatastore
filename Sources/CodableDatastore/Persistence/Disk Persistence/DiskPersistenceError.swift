//
//  DiskPersistenceError.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-09.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A ``DiskPersistence``-specific error.
public enum DiskPersistenceError: LocalizedError, Equatable {
    /// The specified URL for the persistence store is not a file URL.
    case notFileURL
    
    /// A default store could not be created due to a missing main bundle ID.
    case missingBundleID
    
    /// A default store could not be created due to a missing application support directory.
    case missingAppSupportDirectory
    
    /// The index manifest was in a format that could not be understood.
    ///
    /// - TODO: Offer advice to try re-building the index if possible.
    case invalidIndexManifestFormat
    
    /// The page was in a format that could not be understood.
    case invalidPageFormat
    
    /// The entry was in a format that could not be understood.
    case invalidEntryFormat
    
    /// The persistence is read only and cannot be written to.
    case cannotWrite
    
    /// The persistence of another object is not in common with the persistence being used.
    case wrongPersistence
    
    public var errorDescription: String? {
        switch self {
        case .notFileURL:
            "The persistence store cannot be saved to the specified URL."
        case .missingBundleID:
            "The persistence store cannot be saved to the default URL as it is not running in the context of an app."
        case .missingAppSupportDirectory:
            "The persistence store cannot be saved to the default URL as an Application Support directory could built for this system."
        case .invalidIndexManifestFormat:
            "The index manifest was in a format that could not be understood."
        case .invalidPageFormat:
            "The page was in a format that could not be understood."
        case .invalidEntryFormat:
            "The entry was in a format that could not be understood."
        case .cannotWrite:
            "The persistence is read only and cannot be written to."
        case .wrongPersistence:
            "The persistence of another object is not in common with the persistence being used."
        }
    }
}

/// A ``DiskPersistence``-specific internal error.
public enum DiskPersistenceInternalError: LocalizedError {
    /// A request to update store info failed as an update was made in an inconsistent state.
    case nestedStoreWrite
    
    /// A request to update snapshot manifest failed as an update was made in an inconsistent state.
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
