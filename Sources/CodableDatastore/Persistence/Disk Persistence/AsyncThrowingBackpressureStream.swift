//
//  AsyncThrowingBackpressureStream.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-10.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

struct AsyncThrowingBackpressureStream<Element: Sendable>: Sendable {
    fileprivate actor StateMachine {
        var pendingEvents: [(CheckedContinuation<Void, Error>, Result<Element?, Error>)] = []
        var eventsReadyContinuation: CheckedContinuation<Element?, Error>?
        var wasCancelled = false
        
        func provide(_ result: Result<Element?, Error>) async throws {
            guard !wasCancelled else { throw CancellationError() }
            
            try await withCheckedThrowingContinuation { continuation in
                precondition(pendingEvents.isEmpty, "More than one event has bee queued on the stream.")
                if let eventsReadyContinuation {
                    self.eventsReadyContinuation = nil
                    eventsReadyContinuation.resume(with: result)
                    continuation.resume()
                } else {
                    pendingEvents.append((continuation, result))
                }
            }
        }
        
        func consumeNext() async throws -> Element? {
            if Task.isCancelled {
                wasCancelled = true
            }
            
            return try await withCheckedThrowingContinuation { continuation in
                guard !pendingEvents.isEmpty else {
                    eventsReadyContinuation = continuation
                    return
                }
                let (providerContinuation, result) = pendingEvents.removeFirst()
                continuation.resume(with: result)
                if wasCancelled {
                    providerContinuation.resume(throwing: CancellationError())
                } else {
                    providerContinuation.resume()
                }
            }
        }
        
        deinit {
            if let eventsReadyContinuation {
                eventsReadyContinuation.resume(throwing: CancellationError())
            }
        }
    }
    
    struct Continuation: Sendable {
        private weak var stateMachine: StateMachine?
        
        fileprivate init(stateMachine: StateMachine) {
            self.stateMachine = stateMachine
        }
        
        func yield(_ value: Element) async throws {
            guard let stateMachine else { throw CancellationError() }
            try await stateMachine.provide(.success(value))
        }
        
        fileprivate func finish(throwing error: Error? = nil) async throws {
            guard let stateMachine else { throw CancellationError() }
            if let error {
                try await stateMachine.provide(.failure(error))
            } else {
                try await stateMachine.provide(.success(nil))
            }
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

extension AsyncThrowingBackpressureStream: TypedAsyncSequence {
    struct AsyncIterator: AsyncIteratorProtocol {
        fileprivate var stateMachine: StateMachine
        
        func next() async throws -> Element? {
            try await stateMachine.consumeNext()
        }
    }
    
    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(stateMachine: stateMachine)
    }
}
