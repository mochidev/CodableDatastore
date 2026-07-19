//
//  AsyncThrowingBackpressureStreamTests.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2026-01-23.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import XCTest
@testable import CodableDatastore

final class AsyncThrowingBackpressureStreamTests: XCTestCase {
    func testStreamForwardsResults() async throws {
        let stream = AsyncThrowingBackpressureStream<Int> { continuation in
            try await continuation.yield(0)
            try await continuation.yield(1)
            try await continuation.yield(2)
            try await continuation.yield(3)
            try await continuation.yield(4)
        }
        
        let results = try await stream.collectInstances(upTo: .infinity)
        
        XCTAssertEqual(results, [0, 1, 2, 3, 4])
    }
    
    func testReadTaskSuspendsWriteTask() async throws {
        let (writeContinuations, readProvider) = AsyncStream.makeStream(of: (Int, CheckedContinuation<Void, Never>).self)
        
        let stream = AsyncThrowingBackpressureStream<Int> { continuation in
            try await continuation.yield(0)
            await withCheckedContinuation { continuation in
                readProvider.yield((0, continuation))
            }
            try await continuation.yield(1)
            await withCheckedContinuation { continuation in
                readProvider.yield((1, continuation))
            }
            try await continuation.yield(2)
            await withCheckedContinuation { continuation in
                readProvider.yield((2, continuation))
            }
            try await continuation.yield(3)
            await withCheckedContinuation { continuation in
                readProvider.yield((3, continuation))
            }
            try await continuation.yield(4)
            await withCheckedContinuation { continuation in
                readProvider.yield((4, continuation))
            }
        }
        
        let iterator = stream.makeAsyncIterator()
        var consumer = writeContinuations.makeAsyncIterator()
        
        var result = try await iterator.next()
        XCTAssertEqual(result, 0)
        var accumulatedResult = await consumer.next()!
        accumulatedResult.1.resume()
        XCTAssertEqual(accumulatedResult.0, 0)
        
        result = try await iterator.next()
        XCTAssertEqual(result, 1)
        accumulatedResult = await consumer.next()!
        accumulatedResult.1.resume()
        XCTAssertEqual(accumulatedResult.0, 1)
        
        result = try await iterator.next()
        XCTAssertEqual(result, 2)
        accumulatedResult = await consumer.next()!
        accumulatedResult.1.resume()
        XCTAssertEqual(accumulatedResult.0, 2)
        
        result = try await iterator.next()
        XCTAssertEqual(result, 3)
        accumulatedResult = await consumer.next()!
        accumulatedResult.1.resume()
        XCTAssertEqual(accumulatedResult.0, 3)
        
        result = try await iterator.next()
        XCTAssertEqual(result, 4)
        accumulatedResult = await consumer.next()!
        accumulatedResult.1.resume()
        XCTAssertEqual(accumulatedResult.0, 4)
        
        result = try await iterator.next()
        XCTAssertEqual(result, nil)
    }
    
    func testWriteTaskNeverProgressesWhenReadsDoNotHappen() async throws {
        let (writeContinuations, readProvider) = AsyncStream.makeStream(of: (Int, CheckedContinuation<Void, Never>).self)
        
        let stream = AsyncThrowingBackpressureStream<Int> { continuation in
            try await continuation.yield(0)
            await withCheckedContinuation { continuation in
                readProvider.yield((0, continuation))
            }
            try await continuation.yield(1)
            XCTFail()
        }
        
        let iterator = stream.makeAsyncIterator()
        var consumer = writeContinuations.makeAsyncIterator()
        
        let result = try await iterator.next()
        XCTAssertEqual(result, 0)
        let accumulatedResult = await consumer.next()!
        accumulatedResult.1.resume()
        XCTAssertEqual(accumulatedResult.0, 0)
        
        try await Task.sleep(for: .seconds(1))
    }
    
    func testWriteTaskNeverProgressesWhenReadsAreCancelled() async throws {
        let expectation = expectation(description: "Writes were cancelled")
        
        let task = Task {
            let stream = AsyncThrowingBackpressureStream<Int> { continuation in
                try await continuation.yield(0)
                do {
                    try await continuation.yield(1)
                    XCTFail()
                } catch {
                    XCTAssertEqual(error is CancellationError, true)
                    expectation.fulfill()
                    throw error
                }
            }
            
            let iterator = stream.makeAsyncIterator()
            let result = try await iterator.next()
            XCTAssertEqual(result, 0)
            
            withUnsafeCurrentTask { task in
                task?.cancel()
            }
            
            do {
                /// Perform two reads, because we can't control if the write happens before this happens (in which case the first read will succeed) or if it happens after (in which the first read will fail). Either way, the second read will always fail and return nil.
                _ = try await iterator.next()
                _ = try await iterator.next()
                XCTFail()
            } catch {
                XCTAssertEqual(error is CancellationError, true)
            }
        }
        
        try? await task.value
        
        await fulfillment(of: [expectation], timeout: 10)
    }
    
    func testReadingNotSuspendedWhenCancelledBeforeWrite() async throws {
        let (writeContinuations, readProvider) = AsyncStream.makeStream(of: CheckedContinuation<Void, Never>.self)
        
        let expectation = expectation(description: "Writes were cancelled")
        
        let task = Task {
            let stream = AsyncThrowingBackpressureStream<Int> { continuation in
                try await continuation.yield(0)
                await withCheckedContinuation { continuation in
                    readProvider.yield(continuation)
                }
                do {
                    try await continuation.yield(1)
                    XCTFail()
                } catch {
                    XCTAssertEqual(error is CancellationError, true)
                    expectation.fulfill()
                    throw error
                }
            }
            
            let iterator = stream.makeAsyncIterator()
            let result = try await iterator.next()
            XCTAssertEqual(result, 0)
            
            withUnsafeCurrentTask { task in
                task?.cancel()
            }
            
            do {
                /// This read is guaranteed to happen before the write, which is blocked below. It should _never_ stall until the write is made.
                _ = try await iterator.next()
                XCTFail()
            } catch {
                /// Let the write happen strictly after the read, in its own task so signaling doesn't "see" the cancellation.
                XCTAssertEqual(error is CancellationError, true)
                await Task { await writeContinuations.first(where: { _ in true })!.resume() }.value
            }
        }
        
        try? await task.value
        
        await fulfillment(of: [expectation], timeout: 10)
    }
    
    func testWritingUnsuspendsWhenReadsCancelledButNeverMade() async throws {
        let (writeContinuations, readProvider) = AsyncStream.makeStream(of: CheckedContinuation<Void, Never>.self)
        
        let expectation = expectation(description: "Writes were cancelled")
        
        let task = Task {
            var stream: AsyncThrowingBackpressureStream<Int>? = AsyncThrowingBackpressureStream<Int> { continuation in
                try await continuation.yield(0)
                await withCheckedContinuation { continuation in
                    readProvider.yield(continuation)
                }
                do {
                    try await continuation.yield(1)
                    XCTFail()
                    expectation.fulfill()
                } catch {
                    XCTAssertEqual(error is CancellationError, true)
                    expectation.fulfill()
                    throw error
                }
            }
            
            let iterator = stream!.makeAsyncIterator()
            let result = try await iterator.next()
            XCTAssertEqual(result, 0)
            
            withUnsafeCurrentTask { task in
                task?.cancel()
            }
            
            /// Let the write happen strictly after cancellation, in its own task so signaling doesn't "see" the cancellation.
            await Task { await writeContinuations.first(where: { _ in true })!.resume() }.value
            
            /// The stream can't be marked as cancelled if another read never happens.
            let wasCancelled = await iterator.wasCancelled
            XCTAssertEqual(wasCancelled, false)
            
            stream = nil
        }
        
        try? await task.value
        readProvider.finish()
        
        await fulfillment(of: [expectation], timeout: 10)
    }
}
