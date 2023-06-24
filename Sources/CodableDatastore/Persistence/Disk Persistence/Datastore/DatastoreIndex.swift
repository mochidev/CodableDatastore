//
//  DatastoreIndex.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-23.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias DatastoreIndexIdentifier = TypedIdentifier<DiskPersistence<ReadOnly>.Datastore.Index>
typealias DatastoreIndexManifestIdentifier = DatedIdentifier<DiskPersistence<ReadOnly>.Datastore.Index>

enum DatastoreIndexRootIdentifier: Hashable {
    case primary(manifest: DatastoreIndexManifestIdentifier)
    case direct(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
    case secondary(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
}

extension DiskPersistence.Datastore {
    actor Index: Identifiable {
        unowned let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: DatastoreIndexRootIdentifier
        
        init(
            datastore: DiskPersistence<AccessMode>.Datastore,
            id: DatastoreIndexRootIdentifier
        ) {
            self.datastore = datastore
            self.id = id
        }
    }
}
