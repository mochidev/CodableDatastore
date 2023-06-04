//
//  Persistence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

/// A persistence used to group multiple data stores into a common store.
public protocol Persistence {
    func perform(_ transaction: (_ persistence: Self) -> ()) async throws
}

/// An internal list of requirements for a persistence.
///
/// Although this type is provided so other packages can implement their own persistences,
/// none of these methods should be called directly.
public protocol _Persistence: Persistence {
    func withTransaction(_ transaction: (_ persistence: Self) -> ()) async throws
}

extension _Persistence {
    public func perform(_ transaction: (_ persistence: Self) -> ()) async throws {
        try await withTransaction(transaction)
    }
}
