//
//  PersistenceError.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-13.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A ``Persistence``-specific error.
public enum PersistenceError: LocalizedError {
    /// The datastore has already been registered with another persistence.
    case multipleRegistrations
    
    /// The datastore has already been registered with this persistence.
    case alreadyRegistered
    
    /// An existing datastore that can write to the persistence has already been registered for this key.
    case duplicateWriters
    
    /// The requested instance could not be found with the specified identifier.
    case instanceNotFound
    
    /// The requested insertion cursor conflicts with an already existing identifier.
    case instanceAlreadyExists
    
    public var errorDescription: String? {
        switch self {
        case .multipleRegistrations:
            return "The datastore has already been registered with another persistence. Make sure to only register a datastore with a single persistence."
        case .alreadyRegistered:
            return "The datastore has already been registered with this persistence. Make sure to not call register multiple times per persistence."
        case .duplicateWriters:
            return "An existing datastore that can write to the persistence has already been registered for this key. Only one writer is suppored per key."
        case .instanceNotFound:
            return "The requested instance could not be found with the specified identifier."
        case .instanceAlreadyExists:
            return "The requested insertion cursor conflicts with an already existing identifier."
        }
    }
}
