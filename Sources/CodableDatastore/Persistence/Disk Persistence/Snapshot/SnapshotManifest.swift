//
//  SnapshotManifest.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-08.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// Versions supported by ``DiskPersisitence``.
///
/// These are used when dealing with format changes at the library level.
enum SnapshotManifestVersion: String, Codable, Sendable {
    case alpha
}

/// A struct to store information about a ``DiskPersistence``'s snapshot on disk.
struct SnapshotManifest: Codable, Equatable, Identifiable, Sendable {
    /// The version of the snapshot, used when dealing with format changes at the library level.
    var version: SnapshotManifestVersion = .alpha
    
    var id: SnapshotIdentifier
    
    /// The last modification date of the snaphot.
    var modificationDate: Date
    
    var currentIteration: SnapshotIterationIdentifier?
}
