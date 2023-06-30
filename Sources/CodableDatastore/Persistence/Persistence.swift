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
    
    /// The interface a datastore can use to communicate fundamental operations with the persistence.
    ///
    /// - Warning: Calling methods on this object can lead to undefined behavior.
    nonisolated var _datastoreInterface: any DatastoreInterfaceProtocol { get }
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
    ///   - options: A set of options to use when performing the transaction.
    ///   - transaction: A closure witht he set of operations to perform.
    public func perform<T>(options: TransactionOptions = [], transaction: @escaping (_ persistence: Self) async throws -> T) async throws -> T {
        try await _datastoreInterface.withTransaction(options: options) { _ in
            try await transaction(self)
        }
    }
}
