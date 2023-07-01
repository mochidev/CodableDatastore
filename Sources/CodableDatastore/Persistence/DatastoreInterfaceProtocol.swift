//
//  DatastoreInterfaceProtocol.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-29.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A interface a ``Datastore`` uses to communicate with a ``Persistence``.
/// 
/// This protocol is provided so others can implement new persistences modelled after the ones provided by ``CodableDatastore``. You should never call any of these methods directly.
public protocol DatastoreInterfaceProtocol {
    /// Register a ``Datastore`` with a ``Persistence`` so that it can be informed of changes made to the persistence.
    ///
    /// A datastore should only be registered once to a single persistence.
    /// - Parameter datastore: The datastore to register.
    /// - Returns: A descriptor of the datastore as the persistence knows it.
    func register<Version, CodedType, IdentifierType, AccessMode>(datastore: Datastore<Version, CodedType, IdentifierType, AccessMode>) async throws -> DatastoreDescriptor?
    
    /// Load the descriptor of a ``Datastore``.
    /// - Parameter datastore: The datastore to query.
    /// - Returns: A descriptor of the datastore as the persistence knows it.
    func datastoreDescriptor(for datastoreKey: DatastoreKey) async throws -> DatastoreDescriptor?
    
    /// Apply a descriptor for a given datastore.
    ///
    /// - Note: The persistence may choose which values to keep and which to ignore. For instance, an index version may be ignored if the index already exists.
    /// - Parameters:
    ///   - descriptor: A descriptor of the Datastore as it should exist.
    ///   - datastoreKey: The key of the datastore the descriptor belongs to.
    func apply(descriptor: DatastoreDescriptor, for datastoreKey: DatastoreKey) async throws
    
    /// Load a cursor for the specified identifier in the primary index of the specified datastore key.
    ///
    /// - Throws: ``DatastoreInterfaceError/instanceNotFound``  if an instance for the specified identifier could not be found.
    /// - Parameters:
    ///   - identifier: The identifier of the instance to load.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    /// - Returns: The cursor along with the data it contains.
    func primaryIndexCursor<IdentifierType: Indexable>(
        for identifier: IdentifierType,
        datastoreKey: DatastoreKey
    ) async throws -> (
        cursor: any InstanceCursorProtocol,
        instanceData: Data,
        versionData: Data
    )
    
    /// Load a cursor for inserting the specified identifier in the primary index of the specified datastore key.
    ///
    /// - Throws: ``DatastoreInterfaceError/instanceAlreadyExists`` if an instance for the specified identifier already exists.
    /// - Parameters:
    ///   - identifier: The identifier of an instance to insert.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    /// - Returns: A cursor ideal for inserting the specified item.
    func primaryIndexCursor<IdentifierType: Indexable>(
        inserting identifier: IdentifierType,
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol
    
    /// Load a cursor for the specified indexedValue in a direct index of the specified datastore key.
    ///
    /// - Throws: ``DatastoreInterfaceError/instanceNotFound`` if an instance for the specified identifier could not be found.
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
        datastoreKey: DatastoreKey
    ) async throws -> (
        cursor: any InstanceCursorProtocol,
        instanceData: Data,
        versionData: Data
    )
    
    /// Load a cursor for inserting the specified indexedValue in a direct index of the specified datastore key.
    ///
    /// - Throws: ``DatastoreInterfaceError/instanceAlreadyExists`` if an instance for the specified identifier already exists.
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
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol
    
    /// Load a cursor for the specified indexedValue in a secondary index of the specified datastore key.
    ///
    /// - Throws: ``DatastoreInterfaceError/instanceNotFound`` if an instance for the specified identifier could not be found.
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
        datastoreKey: DatastoreKey
    ) async throws -> any InstanceCursorProtocol
    
    /// Load a cursor for inserting the specified indexedValue in a secondary index of the specified datastore key.
    ///
    /// - Throws: ``DatastoreInterfaceError/instanceAlreadyExists`` if an instance for the specified identifier already exists.
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
        datastoreKey: DatastoreKey
    ) async throws -> any InsertionCursorProtocol
    
    /// Create or update an entry in the primary index of a data store.
    ///
    /// This should emit .created and .updated observations to the data stores.
    /// - Parameters:
    ///   - versionData: The version data associated with the entry.
    ///   - identifierValue: The identifier the entry is keyed under.
    ///   - instanceData: The data that represented the encoded entry.
    ///   - cursor: The location to insert the entry.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    func persistPrimaryIndexEntry<IdentifierType: Indexable>(
        versionData: Data,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor: some InsertionCursorProtocol,
        datastoreKey: DatastoreKey
    ) async throws
    
    /// Delete an entry from the primary index of the data store.
    ///
    /// This should emit .deleted observations to the data stores.
    /// - Parameters:
    ///   - cursor: The location of the entry to delete.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    func deletePrimaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        datastoreKey: DatastoreKey
    ) async throws
    
    /// Reset the primary index of a data store.
    ///
    /// It is expected that the caller of this method will re-iterate through and add all entries back in, though perhaps with different identifiers or in a different order.
    /// - Parameter datastoreKey: The key of the datastore the index belongs to.
    func resetPrimaryIndex(
        datastoreKey: DatastoreKey
    ) async throws
    
    /// Create an entry in a direct index of a data store.
    /// - Parameters:
    ///   - versionData: The version data associated with the entry.
    ///   - indexValue: The value the entry is sorted under.
    ///   - identifierValue: The identifier the entry is keyed under.
    ///   - instanceData: The data that represented the encoded entry.
    ///   - cursor: The location to insert the entry.
    ///   - indexName: The name of the index.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    func persistDirectIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        versionData: Data,
        indexValue: IndexType,
        identifierValue: IdentifierType,
        instanceData: Data,
        cursor: some InsertionCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws
    
    /// Delete an entry from a direct index of the data store.
    /// - Parameters:
    ///   - cursor: The location of the entry to delete.
    ///   - indexName: The name of the index.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    func deleteDirectIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws
    
    /// Delete a direct index of the data store.
    ///
    /// It is expected that the caller of this method will re-iterate through and add all entries back in, though perhaps with different indexed values or in a different order.
    /// - Parameters:
    ///   - indexName: The name of the index.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    func deleteDirectIndex(
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws
    
    /// Create an entry in a secondary index of a data store.
    /// - Parameters:
    ///   - indexValue: The value the entry is sorted under.
    ///   - identifierValue: The identifier the entry is keyed under.
    ///   - cursor: The location to insert the entry.
    ///   - indexName: The name of the index.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    func persistSecondaryIndexEntry<IndexType: Indexable, IdentifierType: Indexable>(
        indexValue: IndexType,
        identifierValue: IdentifierType,
        cursor: some InsertionCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws
    
    /// Delete an entry from a secondary index of the data store.
    /// - Parameters:
    ///   - cursor: The location of the entry to delete.
    ///   - indexName: The name of the index.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    func deleteSecondaryIndexEntry(
        cursor: some InstanceCursorProtocol,
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws
    
    /// Delete a direct index of the data store.
    ///
    /// It is expected that the caller of this method will re-iterate through and add all entries back in, though perhaps with different indexed values or in a different order.
    /// - Parameters:
    ///   - indexName: The name of the index.
    ///   - datastoreKey: The key of the datastore the index belongs to.
    func deleteSecondaryIndex(
        indexName: String,
        datastoreKey: DatastoreKey
    ) async throws
}
