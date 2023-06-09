//
//  Snapshot.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-09.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias SnapshotIdentifier = Identifier<Snapshot<ReadOnly>>

/// A type that manages access to a snapshot on disk.
actor Snapshot<AccessMode: _AccessMode> {
    var identifer: SnapshotIdentifier
    
    init(
        identifier: SnapshotIdentifier,
        persistence: DiskPersistence<AccessMode>
    ) {
        self.identifer = identifier
    }
}
