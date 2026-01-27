//
//  AsyncThrowingBackpressureStream.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-10.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A stream that limits reads based on the speed results are consumed at.
///
/// A backpressure stream is consumed within a _reading task_, usually in a `for try await` loop. Separately, it is fed within an internal _writing task_ inherited during initialization. These may share the same parent task depending on the use case.
///
/// Writes to the stream can be made one at a time until they are consumed by the reading task, usually via an iterator. If a write is not consumed, the writing task is suspended until the reading task is ready to consume the event. To this effect, a backpressure stream may hold onto at most a single pending event while waiting for a read to take place. Similarly, if a read happens before a write is ready, the reading task will be suspended, while the write will be processed immediately allowing a follow-up write to be made.
///
/// The reading task may be cancelled at any time, immediately ending the loop, and propagaing the cancellation to the writing child task, stopping any more values from being provided to the stream.
struct AsyncThrowingBackpressureStream<Element: Sendable>: Sendable {
    fileprivate actor StateMachine {
        var pendingWriteEvents: [(CheckedContinuation<Void, Error>, Result<Element?, Error>)] = []
        var pendingReadContinuation: CheckedContinuation<Element?, Error>?
        var wasCancelled = false
        
        func provide(_ result: Result<Element?, Error>, in continuation: CheckedContinuation<Void, Error>) {
            /// If reads were cancelled, propagate the cancellation to the provider without saving the result.
            guard !wasCancelled else {
                continuation.resume(throwing: CancellationError())
                return
            }
            
            /// Ideally, no more than one pending event should be queued up, as a second event means backpressure isn't working.
            precondition(pendingWriteEvents.isEmpty, "More than one event has been queued on the stream.")
            
            /// If a read is currently pending, signal that a new result has been provided.
            if let pendingReadContinuation {
                self.pendingReadContinuation = nil
                pendingReadContinuation.resume(with: result)
                continuation.resume()
            } else {
                /// If we aren't ready for events, queue the event and suspend the task until events are ready. This will stop more values from being provided (ie. the backpressure at work).
                pendingWriteEvents.append((continuation, result))
            }
        }
        
        /// Cancel any reads by immediately signalling that no events are available to any pending read.
        private func cancelPendingRead() {
            wasCancelled = true
            if let pendingReadContinuation {
                self.pendingReadContinuation = nil
                pendingReadContinuation.resume(throwing: CancellationError())
            }
        }
        
        /// Consume the next value on the read task.
        ///
        /// There are two scenarios to consider here:
        /// - A read happens before a write.
        /// - A write happens before a read.
        ///
        /// In the first case, a continuation is saved and the read task is suspended. In the second case, a read is popped off the from of the pending write events and returned immediately.
        func consumeNext() async throws -> Element? {
            if Task.isCancelled {
                wasCancelled = true
            }
            
            return try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    guard !pendingWriteEvents.isEmpty else {
                        /// If there are no pending events, suspend the reading task until one is signaled.
                        guard !wasCancelled else {
                            /// If the task was cancelled, stop here without waiting for the signal — the provider will be cancelled as soon as they try to provide their first value.
                            continuation.resume(throwing: CancellationError())
                            return
                        }
                        pendingReadContinuation = continuation
                        return
                    }
                    
                    /// Otherwise, pop the first entry off the stack and return it.
                    let (providerContinuation, result) = pendingWriteEvents.removeFirst()
                    
                    /// Return the reading task with the result we have from the write queue.
                    continuation.resume(with: result)
                    
                    /// Determine if the provider should continue providing values or if it should be stopped here.
                    if wasCancelled {
                        providerContinuation.resume(throwing: CancellationError())
                    } else {
                        providerContinuation.resume()
                    }
                }
            } onCancel: {
                Task { await cancelPendingRead() }
            }
        }
        
        deinit {
            if let pendingReadContinuation {
                pendingReadContinuation.resume(throwing: CancellationError())
            }
            for (providerContinuation, _) in pendingWriteEvents {
                providerContinuation.resume(throwing: CancellationError())
            }
        }
    }
    
    struct Continuation: Sendable {
        private weak var stateMachine: StateMachine?
        
        fileprivate init(stateMachine: StateMachine) {
            self.stateMachine = stateMachine
        }
        
        func yield(_ value: Element) async throws {
            do {
                try await withCheckedThrowingContinuation { continuation in
                    guard let stateMachine else {
                        continuation.resume(throwing: CancellationError())
                        return
                    }
                    Task {
                        await stateMachine.provide(.success(value), in: continuation)
                    }
                } as Void
            } catch {
                throw error
            }
        }
        
        fileprivate func finish(throwing error: Error? = nil) async throws {
            try await withCheckedThrowingContinuation { continuation in
                guard let stateMachine else { continuation.resume(throwing: CancellationError())
                    return
                }
                Task {
                    if let error {
                        await stateMachine.provide(.failure(error), in: continuation)
                    } else {
                        await stateMachine.provide(.success(nil), in: continuation)
                    }
                }
            } as Void
        }
    }
    
    private var stateMachine: StateMachine
    
    init(provider: @Sendable @escaping (Continuation) async throws -> ()) {
        stateMachine = StateMachine()
        
        let continuation = Continuation(stateMachine: stateMachine)
        Task {
            do {
                try await provider(continuation)
                try await continuation.finish()
            } catch {
                try await continuation.finish(throwing: error)
            }
        }
    }
}

extension AsyncThrowingBackpressureStream: AsyncInstances {
    struct AsyncIterator: AsyncIteratorProtocol {
        fileprivate var stateMachine: StateMachine
        
        func next() async throws -> Element? {
            try await stateMachine.consumeNext()
        }
        
        /// Used only for testing.
        internal var wasCancelled: Bool {
            get async { await stateMachine.wasCancelled }
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(stateMachine: stateMachine)
    }
}
