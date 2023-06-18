//
//  Persistence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A persistence used to group multiple data stores into a common store.
public protocol Persistence<AccessMode>: _Persistence {
    associatedtype AccessMode: _AccessMode
    func perform(_ transaction: (_ persistence: Self) -> ()) async throws
}

/// An internal list of requirements for a persistence.
///
/// Although this type is provided so other packages can implement their own persistences,
/// none of these methods should be called directly.
public protocol _Persistence {
    /// Register a ``Datastore`` with a ``Persistence`` so that it can be informed of changes made to the persistence.
    ///
    /// A datastore should only be registered once to a single persistence.
    /// - Parameter datastore: The datastore to register.
    /// - Returns: A descriptor of the datastore as the persistence knows it.
    func register<Version, CodedType, IdentifierType, AccessMode>(datastore: Datastore<Version, CodedType, IdentifierType, AccessMode>) async throws -> DatastoreDescriptor?
    
    /// Load the descriptor of a ``Datastore``.
    /// - Parameter datastore: The datastore to query.
    /// - Returns: A descriptor of the datastore as the persistence knows it.
    func datastoreDescriptor<Version, CodedType, IdentifierType, AccessMode>(for datastore: Datastore<Version, CodedType, IdentifierType, AccessMode>) async throws -> DatastoreDescriptor?
    
    /// Load a cursor for the specified identifier in the primary index of the specified datastore key.
    ///
    /// - Throws: This method will throw if an instance for the specified identifier could not be found.
    /// - Parameters:
    ///   - identifier: The identifier of the instance to load.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    /// - Returns: The cursor along with the data it contains.
    func primaryIndexCursor<IdentifierType: Indexable>(
        for identifier: IdentifierType,
        datastoreKey: String
    ) async throws -> (cursor: any InstanceCursor, data: Data)
    
    /// Load a cursor for inserting the specified identifier in the primary index of the specified datastore key.
    ///
    /// - Throws: This method will throw if an instance for the specified identifier already exists.
    /// - Parameters:
    ///   - identifier: The identifier of an instance to insert.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    /// - Returns: A cursor ideal for inserting the specified item.
    func primaryIndexCursor<IdentifierType: Indexable>(
        inserting identifier: IdentifierType,
        datastoreKey: String
    ) async throws -> any InsertionCursor
    
    /// Load a cursor for the specified indexedValue in a direct index of the specified datastore key.
    ///
    /// - Throws: This method will throw if an instance for the specified identifier could not be found.
    /// - Parameters:
    ///   - indexedValue: The indexed value to search against.
    ///   - identifier: The identifier of the instance to load.
    ///   - indexName: The name of the direct index to search in.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    /// - Returns: The cursor along with the data it contains.
    func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        for indexedValue: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> (cursor: any InstanceCursor, data: Data)
    
    /// Load a cursor for inserting the specified indexedValue in a direct index of the specified datastore key.
    ///
    /// - Throws: This method will throw if an instance for the specified identifier already exists.
    /// - Parameters:
    ///   - indexedValue: The indexed value to search against.
    ///   - identifier: The identifier of an instance to insert.
    ///   - indexName: The name of the direct index to search in.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    /// - Returns: A cursor ideal for inserting the specified item.
    func directIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> any InsertionCursor
    
    /// Load a cursor for the specified indexedValue in a secondary index of the specified datastore key.
    ///
    /// - Throws: This method will throw if an instance for the specified identifier could not be found.
    /// - Parameters:
    ///   - indexedValue: The indexed value to search against.
    ///   - identifier: The identifier of the instance to load.
    ///   - indexName: The name of the secondary index to search in.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    /// - Returns: The cursor within the index.
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        for index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> any InstanceCursor
    
    /// Load a cursor for inserting the specified indexedValue in a secondary index of the specified datastore key.
    ///
    /// - Throws: This method will throw if an instance for the specified identifier already exists.
    /// - Parameters:
    ///   - indexedValue: The indexed value to search against.
    ///   - identifier: The identifier of an instance to insert.
    ///   - indexName: The name of the secondary index to search in.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    /// - Returns: A cursor ideal for inserting the specified item.
    func secondaryIndexCursor<IndexType: Indexable, IdentifierType: Indexable>(
        inserting index: IndexType,
        identifier: IdentifierType,
        indexName: String,
        datastoreKey: String
    ) async throws -> any InsertionCursor
    
    func withTransaction(_ transaction: (_ persistence: Self) -> ()) async throws
}

extension _Persistence {
    public func perform(_ transaction: (_ persistence: Self) -> ()) async throws {
        try await withTransaction(transaction)
    }
}
