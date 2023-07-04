//
//  DiskCursor.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

extension DiskPersistence {
    struct InstanceCursor: InstanceCursorProtocol {
        var persistence: DiskPersistence
        var datastore: Datastore
        var index: Datastore.Index
        var blocks: [CursorBlock]
    }
    
    struct InsertionCursor: InsertionCursorProtocol {
        var persistence: DiskPersistence
        var datastore: Datastore
        var index: Datastore.Index
        var insertAfter: CursorBlock?
    }
    
    struct CursorBlock {
        var pageIndex: Int
        var page: DiskPersistence.Datastore.Page
        var blockIndex: Int
    }
}
