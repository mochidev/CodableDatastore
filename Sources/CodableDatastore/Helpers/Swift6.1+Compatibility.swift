//
//  Swift6.1+Compatibility.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2026-08-31.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

#if compiler(<6.2)
extension Task {
    @discardableResult
    init(
        name: String?,
        priority: TaskPriority? = nil,
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping @isolated(any) () async -> Success
    ) where Failure == Never {
        self.init(priority: priority, operation: operation)
    }
    
    @discardableResult
    init(
        name: String?,
        priority: TaskPriority? = nil,
        @_inheritActorContext @_implicitSelfCapture operation: sending @escaping @isolated(any) () async throws -> Success
    ) where Failure == any Error {
        self.init(priority: priority, operation: operation)
    }
    
    @discardableResult
    public static func detached(
        name: String?,
        priority: TaskPriority? = nil,
        operation: sending @escaping @isolated(any) () async -> Success
    ) -> Task<Success, Failure> where Failure == Never {
        Task.detached(priority: priority, operation: operation)
    }
    
    @discardableResult
    public static func detached(
        name: String?,
        priority: TaskPriority? = nil,
        operation: sending @escaping @isolated(any) () async throws -> Success
    ) -> Task<Success, Failure> where Failure == any Error {
        Task.detached(priority: priority, operation: operation)
    }
}

public func extendLifetime<T>(_ x: borrowing T) where T : ~Copyable {
    withExtendedLifetime(x) {}
}
#endif
