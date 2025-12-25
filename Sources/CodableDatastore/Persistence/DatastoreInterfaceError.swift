//
//  DatastoreInterfaceError.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-13.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// An error that may be returned from ``DatastoreInterfaceProtocol`` methods.
public enum DatastoreInterfaceError: LocalizedError {
    /// The datastore has already been registered with another persistence.
    case multipleRegistrations
    
    /// The datastore has already been registered with this persistence.
    case alreadyRegistered
    
    /// The datastore was not found and has likely not been registered with this persistence.
    case datastoreNotFound
    
    /// An existing datastore that can write to the persistence has already been registered for this key.
    case duplicateWriters
    
    /// The requested instance could not be found with the specified identifier.
    case instanceNotFound
    
    /// The requested insertion cursor conflicts with an already existing identifier.
    case instanceAlreadyExists
    
    /// The datastore being manipulated does not yet exist in the persistence.
    case datastoreKeyNotFound
    
    /// The index being manipulated does not yet exist in the datastore.
    case indexNotFound
    
    /// The transaction was accessed outside of its activity window.
    case transactionInactive
    
    /// The transaction was started in the context of another persistence, disqualifying consistency guarantees.
    case transactingWithinExternalPersistence
    
    /// The cursor does not match the one provided by the persistence.
    case unknownCursor
    
    /// The cursor no longer refers to fresh data.
    case staleCursor
    
    public var errorDescription: String? {
        switch self {
        case .multipleRegistrations:
            "The datastore has already been registered with another persistence. Make sure to only register a datastore with a single persistence."
        case .alreadyRegistered:
            "The datastore has already been registered with this persistence. Make sure to not call register multiple times per persistence."
        case .datastoreNotFound:
            "The datastore was not found and has likely not been registered with this persistence."
        case .duplicateWriters:
            "An existing datastore that can write to the persistence has already been registered for this key. Only one writer is suppored per key."
        case .instanceNotFound:
            "The requested instance could not be found with the specified identifier."
        case .instanceAlreadyExists:
            "The requested insertion cursor conflicts with an already existing identifier."
        case .datastoreKeyNotFound:
            "The datastore being manipulated does not yet exist in the persistence."
        case .indexNotFound:
            "The index being manipulated does not yet exist in the datastore."
        case .transactionInactive:
            "The transaction was accessed outside of its activity window. Please make sure the transaction wasn't escaped."
        case .transactingWithinExternalPersistence:
            "The transaction was started in the context of another persistence, disqualifying consistency guarantees. Wrap calls to this persistence's datastores and transactions in Task.detached to acknowledge this risk."
        case .unknownCursor:
            "The cursor does not match the one provided by the persistence."
        case .staleCursor:
            "The cursor no longer refers to fresh data. Please make sure to use them as soon as possible and not interspaced with other writes."
        }
    }
}
