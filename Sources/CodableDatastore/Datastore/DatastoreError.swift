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
    /// A decoder was missing for the specified version.
    case missingDecoder(version: String)
    
    public var errorDescription: String? {
        switch self {
        case .missingDecoder(let version):
            return "The decoder for version \(version) is missing."
        }
    }
}
