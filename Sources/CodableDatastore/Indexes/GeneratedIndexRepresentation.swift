//
//  GeneratedIndexRepresentation.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-04-10.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

/// A helper type for passing around metadata about an index.
public struct GeneratedIndexRepresentation<Instance: Sendable>: Sendable {
    /// The name the index should be serialized under.
    public var indexName: IndexName
    
    /// The index itself, which can be queried accordingly.
    public var index: any IndexRepresentation<Instance>
    
    /// If the index is direct or referential in nature.
    public var storage: IndexStorage
    
    /// Initialize a new generated index representation.
    public init(indexName: IndexName, index: any IndexRepresentation<Instance>, storage: IndexStorage) {
        self.indexName = indexName
        self.index = index
        self.storage = storage
    }
}
