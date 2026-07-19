//
//  DiskCursor.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-03.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
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
        
        /// The location to insert a new item. If nil, it should be located in the first position of the datastore.
        var insertAfter: CursorBlock?
    }
    
    enum Cursor {
        case instance(InstanceCursor)
        case insertion(InsertionCursor)
    }
    
    struct CursorBlock {
        var pageIndex: Int
        var page: DiskPersistence.Datastore.Page
        var blockIndex: Int
    }
}
