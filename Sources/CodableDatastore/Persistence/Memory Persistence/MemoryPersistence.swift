//
//  MemoryPersistence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

public actor MemoryPersistence: Persistence {
    public typealias AccessMode = ReadWrite
}

extension MemoryPersistence {
    public func _withTransaction<T>(options: TransactionOptions, transaction: @escaping (_ transaction: DatastoreInterfaceProtocol) async throws -> T) async throws -> T {
        preconditionFailure("Unimplemented")
    }
}
