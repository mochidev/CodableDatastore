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

// MARK: - Helper Types

extension DiskPersistence.Datastore.Page {
    struct ID: Hashable {
        let index: DiskPersistence.Datastore.Index.ID
        let page: DatastorePageIdentifier
    }
}

// MARK: - Common URL Accessors

extension DiskPersistence.Datastore.Page {
    /// The URL that points to the page.
    nonisolated var pageURL: URL {
        let baseURL = datastore.pagesURL(for: id.index)
        
        guard let components = try? id.page.components else { preconditionFailure("Components could not be determined for Page.") }
        
        return baseURL
            .appendingPathComponent(components.year, isDirectory: true)
            .appendingPathComponent(components.monthDay, isDirectory: true)
            .appendingPathComponent(components.hourMinute, isDirectory: true)
            .appendingPathComponent("\(id).datastorepage", isDirectory: false)
    }
}
