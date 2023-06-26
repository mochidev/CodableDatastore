//
//  DatastorePage.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-23.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

typealias DatastorePageIdentifier = DatedIdentifier<DiskPersistence<ReadOnly>.Datastore.Page>

extension DiskPersistence.Datastore {
    actor Page: Identifiable {
        struct ID: Hashable {
            let index: Index.ID
            let page: DatastorePageIdentifier
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
