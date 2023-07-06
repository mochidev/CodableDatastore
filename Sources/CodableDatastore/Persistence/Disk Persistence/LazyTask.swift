//
//  LazyTask.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-05.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

struct LazyTask<T> {
    let factory: () async -> T
    
    var value: T {
        get async {
            await factory()
        }
    }
}

struct LazyThrowingTask<T> {
    let factory: () async throws -> T
    
    var value: T {
        get async throws {
            try await factory()
        }
    }
}
