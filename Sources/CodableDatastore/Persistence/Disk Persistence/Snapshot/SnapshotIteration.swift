//
//  SnapshotIteration.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-15.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias SnapshotIterationIdentifier = DatedIdentifier<SnapshotIteration>

/// Versions supported by ``DiskPersisitence``.
///
/// These are used when dealing with format changes at the library level.
enum SnapshotIterationVersion: String, Codable {
    case alpha
}

/// A struct to store information about a ``DiskPersistence``'s snapshot on disk.
struct SnapshotIteration: Codable, Equatable, Identifiable {
    /// The version of the snapshot iteration, used when dealing with format changes at the library level.
    var version: SnapshotIterationVersion = .alpha
    
    var id: SnapshotIterationIdentifier
    
    /// The date this iteration was created.
    var creationDate: Date
    
    /// The iteration this one replaces.
    var precedingIteration: SnapshotIterationIdentifier?
    
    /// The iterations that replace this one.
    ///
    /// If changes branched at this point in time, there may be more than one iteration to choose from. In this case, the first entry will be the oldest successor, while the last entry will be the most recent.
    var successiveIterations: [SnapshotIterationIdentifier] = []
    
    /// The name of the action used that can be presented in a user interface.
    var actionName: String?
    
    /// The known datastores for this snapshot, and their roots.
    var dataStores: [String : DatastoreInfo] = [:]
    
    /// The datastores that have been added in this iteration of the snapshot.
    var addedDatastores: Set<DatastoreIdentifier> = []
    
    /// The datastores that have been completely removed in this iteration of the snapshot.
    var removedDatastores: Set<DatastoreIdentifier> = []
    
    /// The datastore roots that have been added in this iteration of the snapshot.
    var addedDatastoreRoots: Set<DatastoreRootIdentifier> = []
    
    /// The datastore roots that have been replaced in this iteration of the snapshot.
    var removedDatastoreRoots: Set<DatastoreRootIdentifier> = []
}

extension SnapshotIteration {
    struct DatastoreInfo: Codable, Equatable, Identifiable {
        /// The key this datastore uses.
        var key: DatastoreKey
        
        /// The identifier the datastore was saved under.
        var id: DatastoreIdentifier
        
        /// The root object for the datastore.
        var root: DatastoreRootIdentifier?
    }
}

extension SnapshotIteration {
    /// Initialize a snapshot iteration with a date
    /// - Parameter date: The date to base the identifier and creation date off of.
    init(date: Date = Date()) {
        self.init(id: SnapshotIterationIdentifier(date: date), creationDate: date)
    }
    
    /// Internal method to check if an instance should be persisted based on iff it changed significantly from a previous iteration
    /// - Parameter existingInstance: The previous iteration to check
    /// - Returns: `true` if the iteration should be persisted, `false` if it represents the same data from `existingInstance`.
    func isMeaningfullyChanged(from existingInstance: Self?) -> Bool {
        guard
            dataStores == existingInstance?.dataStores
        else { return true }
        return false
    }
}
