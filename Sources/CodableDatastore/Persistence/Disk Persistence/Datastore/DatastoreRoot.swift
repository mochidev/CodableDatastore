//
//  DatastoreRoot.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-22.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias DatastoreRootIdentifier = DatedIdentifier<DiskPersistence<ReadOnly>.Datastore.RootObject>

extension DiskPersistence.Datastore {
    actor RootObject: Identifiable {
        unowned let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: DatastoreRootIdentifier
        
        init(
            datastore: DiskPersistence<AccessMode>.Datastore,
            id: DatastoreRootIdentifier
        ) {
            self.datastore = datastore
            self.id = id
        }
    }
}
