//
//  TransactionOptions.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-20.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A set of options that the caller of a transaction can specify.
public struct TransactionOptions: OptionSet {
    public let rawValue: Int
    
    public init(rawValue: Int) {
        self.rawValue = rawValue
    }
    
    /// The transaction is read only, and can be performed concurrently with other transactions.
    public static let readOnly = TransactionOptions(rawValue: 1 << 0)
    
    /// The transaction can return before it has successfully written to disk, allowing subsequent writes to be queued and written all at once. If an error occurs when it is time to actually persist the changes, the state of the persistence will not reflect the changes made in a transaction.
    public static let collateWrites = TransactionOptions(rawValue: 1 << 1)
    
    /// The transaction is idempotent and does not modify any other kind of state, and can be retried when it encounters an inconsistency. This allows a transaction to concurrently operate with other writes, which may be necessary in a disptributed environment.
    public static let idempotent = TransactionOptions(rawValue: 1 << 2)
}
