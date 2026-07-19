//
//  Task+Unresolved.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2025-12-22.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

extension Task where Success == Void, Failure == Never {
    static func makeUnresolved() async -> (
        task: Task<Success, Failure>,
        continuation: CheckedContinuation<Success, Failure>
    ) {
        var task: Task<Success, Failure>?
        let continuation = await withCheckedContinuation { factoryContinuation in
            task = Task {
                await withCheckedContinuation { taskContinuation in
                    factoryContinuation.resume(returning: taskContinuation)
                }
            }
        }
        return (task!, continuation)
    }
}
