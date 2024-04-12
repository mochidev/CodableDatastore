//
//  DatastoreError.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-18.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A ``Datastore``-specific error.
public enum DatastoreError: LocalizedError {
    case missingIndex
    
    /// A decoder was missing for the specified version.
    case missingDecoder(version: String)
    
    /// The persisted version is incompatible with the one supported by the datastore.
    case incompatibleVersion(version: String?)
    
    public var errorDescription: String? {
        switch self {
        case .missingIndex:
            return "The specified index was not properly declared on this datastore. Please double check your implementation of `DatastoreFormat.generateIndexRepresentations()`."
        case .missingDecoder(let version):
            return "The decoder for version \(version) is missing."
        case .incompatibleVersion(.some(let version)):
            return "The persisted version \(version) is newer than the one supported by the datastore."
        case .incompatibleVersion(.none):
            return "The persisted version is incompatible with the one supported by the datastore."
        }
    }
}
