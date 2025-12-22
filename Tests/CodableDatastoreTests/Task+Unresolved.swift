//
//  Task+Unresolved.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 12/22/25.
//  Copyright © 2023-25 Mochi Development, Inc. All rights reserved.
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
