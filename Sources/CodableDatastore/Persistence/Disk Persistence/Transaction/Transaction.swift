//
//  Transaction.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-21.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

extension DiskPersistence {
    actor Transaction: AnyTransaction {
        let persistence: DiskPersistence
        
        unowned let parent: Transaction?
        var childTransactions: [Transaction] = []
        
        private(set) var task: Task<Void, Error>!
        let options: TransactionOptions
        
        private init(
            persistence: DiskPersistence,
            parent: Transaction?,
            options: TransactionOptions
        ) {
            self.persistence = persistence
            self.parent = parent
            self.options = options
        }
        
        private func attachTask<T>(
            options: TransactionOptions,
            handler: @escaping () async throws -> T
        ) async -> Task<T, Error> {
            let task = Task {
                let returnValue = try await TransactionTaskLocals.$transaction.withValue(self) {
                    try await handler()
                }
                
                /// If we don't care to collate our writes, go ahead and wait for the persistence to stick
                if !options.contains(.collateWrites) {
                    try await self.persist()
                }
                
                return returnValue
            }
            
            self.task = Task {
                _ = try await task.value
                
                /// If we previously skipped persisting, go ahead and do so now.
                if options.contains(.collateWrites) {
                    try await self.persist()
                }
            }
            
            return task
        }
        
        private func persist() async throws {
            
        }
        
        static func makeTransaction<T>(
            persistence: DiskPersistence,
            lastTransaction: Transaction?,
            options: TransactionOptions,
            handler: @escaping () async throws -> T
        ) async -> (Transaction, Task<T, Error>) {
            if let parent = TransactionTaskLocals.transaction {
                let (child, task) = await parent.childTransaction(options: options, handler: handler)
                return (child as! Self, task)
            }
            
            let transaction = Transaction(
                persistence: persistence,
                parent: nil,
                options: options
            )
            
            let task = await transaction.attachTask(options: options) {
                /// If the transaction is not read only, wait for the last transaction to properly finish before starting the next one.
                if !options.contains(.readOnly) {
                    try? await lastTransaction?.task.value
                }
                return try await handler()
            }
            
            return (transaction, task)
        }
        
        func childTransaction<T>(
            options: TransactionOptions,
            handler: @escaping () async throws -> T
        ) async -> (Transaction, Task<T, Error>) {
            let transaction = Transaction(
                persistence: persistence,
                parent: self,
                options: options
            )
            
            /// Get the last non-concurrent transaction from the list. Note that disk persistence currently does not support concurrent idempotent transactions.
            let lastChild = childTransactions.last { !$0.options.contains(.readOnly) }
            childTransactions.append(transaction)
            
            let task = await transaction.attachTask(options: options) {
                /// If the transaction is not read only, wait for the last transaction to properly finish before starting the next one.
                if !options.contains(.readOnly) {
                    _ = try? await lastChild?.task.value
                }
                return try await handler()
            }
            
            return (transaction, task)
        }
    }
}

protocol AnyTransaction {
    func childTransaction<T>(
        options: TransactionOptions,
        handler: @escaping () async throws -> T
    ) async -> (Self, Task<T, Error>)
}

private enum TransactionTaskLocals {
    @TaskLocal
    static var transaction: AnyTransaction?
}
