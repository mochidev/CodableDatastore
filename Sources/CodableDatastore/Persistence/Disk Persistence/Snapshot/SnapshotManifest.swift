//
//  SnapshotManifest.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-08.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
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
