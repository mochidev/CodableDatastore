//
//  Persistence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A persistence used to group multiple data stores into a common store.
public protocol Persistence<AccessMode> {
    associatedtype AccessMode: _AccessMode
    
    /// Perform a transaction on the persistence with the specified options.
    /// - Parameters:
    ///   - actionName: The name of the action to use in an undo operation. Only the names of top-level transactions are recorded. We recommend you use the Base localization name here, as you may no longer have the localized version in future versions of your app.
    ///   - options: The options to use while building the transaction.
    ///   - transaction: A closure representing the transaction with which to perform operations on. You should not escape the provided transaction.
    func _withTransaction<T>(
        actionName: String?,
        options: UnsafeTransactionOptions,
        transaction: @escaping @Sendable (_ transaction: DatastoreInterfaceProtocol, _ isDurable: Bool) async throws -> T
    ) async throws -> T
}

extension Persistence {
    /// Perform a set of operations as a single transaction.
    ///
    /// Within the transaction block, perform operations on multiple ``Datastore``s such that if any one of them were to fail, none will be persisted, and if all of them succeed, they will be persisted atomically.
    ///
    /// Transactions can be nested, though child transactions will only be persisted to disk once the top-most parent finishes successfully. However, a parent can wrap a child transaction in a try-catch block to recover from any errors that may have occurred.
    ///
    /// - Warning: Performing changes to a datastore that is not part of the persistence this is called on is unsupported and will result in an error.
    /// - Parameters:
    ///   - actionName: The name of the action to use in an undo operation. Only the names of top-level transactions are recorded. We recommend you use the Base localization name here, as you may no longer have the localized version in future versions of your app.
    ///   - options: A set of options to use when performing the transaction.
    ///   - transaction: A closure with the set of operations to perform. Parameters include a reference to the persistence, and a flag indicating if the transaction is durable.
    /// - SeeAlso: ``Persistence/perform(actionName:options:transaction:)-1tpvd``   
    public func perform<T>(
        actionName: String? = nil,
        options: TransactionOptions = [],
        transaction: @escaping (_ persistence: Self, _ isDurable: Bool) async throws -> T
    ) async throws -> T {
        try await _withTransaction(
            actionName: actionName,
            options: UnsafeTransactionOptions(options)
        ) { _, isDurable in
            try await transaction(self, isDurable)
        }
    }
    
    /// Perform a set of operations as a single transaction.
    ///
    /// Within the transaction block, perform operations on multiple ``Datastore``s such that if any one of them were to fail, none will be persisted, and if all of them succeed, they will be persisted atomically.
    ///
    /// Transactions can be nested, though child transactions will only be persisted to disk once the top-most parent finishes successfully. However, a parent can wrap a child transaction in a try-catch block to recover from any errors that may have occurred.
    ///
    /// - Warning: Performing changes to a datastore that is not part of the persistence this is called on is unsupported and will result in an error.
    /// - Parameters:
    ///   - actionName: The name of the action to use in an undo operation. Only the names of top-level transactions are recorded.
    ///   - options: A set of options to use when performing the transaction.
    ///   - transaction: A closure with the set of operations to perform.
    /// - SeeAlso: ``Persistence/perform(actionName:options:transaction:)-5476l``
    public func perform<T>(
        actionName: String? = nil,
        options: TransactionOptions = [],
        transaction: @escaping () async throws -> T
    ) async throws -> T {
        try await _withTransaction(
            actionName: actionName,
            options: UnsafeTransactionOptions(options)
        ) { _, isDurable in
            try await transaction()
        }
    }
}
