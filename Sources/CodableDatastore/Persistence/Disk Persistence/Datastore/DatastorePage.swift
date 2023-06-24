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
        unowned let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: (index: Index.ID, page: DatastorePageIdentifier)
        
        init(
            datastore: DiskPersistence<AccessMode>.Datastore,
            indexID: Index.ID,
            pageID: DatastorePageIdentifier
        ) {
            self.datastore = datastore
            self.id = (index: indexID, page: pageID)
        }
    }
}
