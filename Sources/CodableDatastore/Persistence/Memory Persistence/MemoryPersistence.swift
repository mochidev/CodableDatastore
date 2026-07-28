//
//  MemoryPersistence.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Foundation

public actor MemoryPersistence: Persistence {
    public typealias AccessMode = ReadWrite
}

extension MemoryPersistence {
    public func _withTransaction<T: Sendable>(
        actionName: String?,
        options: UnsafeTransactionOptions,
        transaction: sending (_ transaction: any DatastoreInterfaceProtocol, _ isDurable: Bool) async throws -> T
    ) async throws -> T {
        preconditionFailure("Unimplemented")
    }
}
