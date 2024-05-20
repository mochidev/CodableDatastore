//
//  TransactionOptions.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-20.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A set of options that the caller of a transaction can specify.
public struct TransactionOptions: OptionSet, Sendable {
    public let rawValue: UInt64
    
    public init(rawValue: UInt64) {
        self.rawValue = rawValue & 0b00000111
    }
    
    /// The transaction is read only, and can be performed concurrently with other transactions.
    public static let readOnly = Self(rawValue: 1 << 0)
    
    /// The transaction can return before it has successfully written to disk, allowing subsequent writes to be queued and written all at once. If an error occurs when it is time to actually persist the changes, the state of the persistence will not reflect the changes made in a transaction.
    public static let collateWrites = Self(rawValue: 1 << 1)
    
    /// The transaction is idempotent and does not modify any other kind of state, and can be retried when it encounters an inconsistency. This allows a transaction to concurrently operate with other writes, which may be necessary in a disptributed environment.
    public static let idempotent = Self(rawValue: 1 << 2)
}

/// A set of options that the caller of a transaction can specify.
///
/// These options are generally unsafe to use improperly, and should generally not be used.
public struct UnsafeTransactionOptions: OptionSet, Sendable {
    public let rawValue: UInt64
    
    public init(rawValue: UInt64) {
        self.rawValue = rawValue
    }
    
    public init(_ transactionOptions: TransactionOptions) {
        self.rawValue = transactionOptions.rawValue
    }
    
    /// The transaction is read only, and can be performed concurrently with other transactions.
    public static let readOnly = Self(.readOnly)
    
    /// The transaction can return before it has successfully written to disk, allowing subsequent writes to be queued and written all at once. If an error occurs when it is time to actually persist the changes, the state of the persistence will not reflect the changes made in a transaction.
    public static let collateWrites = Self(.collateWrites)
    
    /// The transaction is idempotent and does not modify any other kind of state, and can be retried when it encounters an inconsistency. This allows a transaction to concurrently operate with other writes, which may be necessary in a disptributed environment.
    public static let idempotent = Self(.idempotent)
    
    /// The transaction should skip emitting observations. This is useful when the transaction must enumerate and modify the entire data set, which would cause each modified entry to be kept in memory for the duration of the transaction.
    public static let skipObservations = Self(rawValue: 1 << 16)
    
    /// The transaction should persist to storage even if it is a child transaction. Note that this must be the _first_ non-readonly child transaction of a parent transaction to succeed.
    public static let enforceDurability = Self(rawValue: 1 << 17)
}
