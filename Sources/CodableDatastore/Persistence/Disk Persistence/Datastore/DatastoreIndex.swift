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

extension DiskPersistence.Datastore {
    actor Index: Identifiable {
        enum ID: Hashable {
            case primary(manifest: DatastoreIndexManifestIdentifier)
            case direct(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
            case secondary(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
        }
        
        unowned let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: ID
        
        init(
            datastore: DiskPersistence<AccessMode>.Datastore,
            id: ID
        ) {
            self.datastore = datastore
            self.id = id
        }
    }
}
