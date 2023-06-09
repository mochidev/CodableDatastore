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
enum SnapshotManifestVersion: String, Codable {
    case alpha
}

/// A struct to store information about a ``DiskPersistence``'s snapshot on disk.
struct SnapshotManifest: Codable, Equatable {
    /// The version of the snapshot, used when dealing with format changes at the library level.
    var version: SnapshotManifestVersion = .alpha
    
    /// The known datastores for this snapshot, and their roots.
    var dataStores: [String] = []
}
