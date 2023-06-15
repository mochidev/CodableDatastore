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

extension MemoryPersistence: _Persistence {
    public func register<V, C, I, A>(
        datastore: Datastore<V, C, I, A>
    ) async throws -> DatastoreDescriptor? {
        preconditionFailure("Unimplemented")
    }
    
    public func datastoreDescriptor<V, C, I, A>(
        for datastore: Datastore<V, C, I, A>
    ) async throws -> DatastoreDescriptor? {
        preconditionFailure("Unimplemented")
    }
    
    public func withTransaction(_ transaction: (MemoryPersistence) -> ()) async throws {
        preconditionFailure("Unimplemented")
    }
}
