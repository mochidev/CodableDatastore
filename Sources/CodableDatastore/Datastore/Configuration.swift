//
//  Configuration.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-10.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

public struct Configuration: Sendable {
    /// The size of a single page of data on disk and in memory.
    ///
    /// Applications that deal with large objects may want to consider increasing this appropriately,
    /// but it should always be a multiple of the disk's file block size.
    ///
    /// Increasing the page size will increase the amount of memory a data store will use as pages
    /// must be loaded in their entirety to load and decode objects from them.
    public var pageSize: Int
    
    public init(
        pageSize: Int = 64*1024
    ) {
        self.pageSize = pageSize
    }
    
    static let minimumPageSize = 4*1024
    static let defaultPageSize = 4*1024
    static let maximumPageSize = 1024*1024*1024
}
