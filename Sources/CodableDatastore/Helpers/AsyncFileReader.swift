//
//  AsyncFileReader.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2026-09-04.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Bytes
import Foundation
import QuestionableConcurrency

/// An asynchronous, low-latency file reader that consumes the file as fast as possible, but allows multiple readers to read it at their own pace.
class AsyncFileReader: @unchecked Sendable {
    static let chunkSize = 1024
    
    let url: URL
    var readerTask: Task<Void, Never>! = nil
    
    var resultsLock = UnfairLock()
    
    var writeHead: Int = 0
    var byteChunks: [Bytes] = []
    var finalResult: Result<Void, any Error>?
    var continuations: [CheckedContinuation<Void, Never>] = []
    
    init(contentsOf url: URL) {
        self.url = url
        self.readerTask = Task(name: "CodableDatastore.AsyncFileReader.init(contentsOf:) - File: \(url.lastPathComponent)") { await self.startReading() }
    }
    
    func startReading() async {
        do {
            #if canImport(Darwin)
            if #available(macOS 12.0, iOS 15, watchOS 8, tvOS 15, *) {
                for try await byte in url.resourceBytes {
                    resultsLock.withLock {
                        let chunkIndex = writeHead/Self.chunkSize
                        let indexInChunk = writeHead % Self.chunkSize
                        
                        /// If there isn't enough space, allocate a new chunk.
                        if chunkIndex >= byteChunks.count {
                            byteChunks.append(Bytes(repeating: 0, count: Self.chunkSize))
                        }
                        
                        /// Write the byte and advance the head.
                        byteChunks[chunkIndex][indexInChunk] = byte
                        
                        writeHead += 1
                        
                        /// If anyone is waiting on us, let them know.
                        if !continuations.isEmpty {
                            for continuation in continuations {
                                continuation.resume()
                            }
                            continuations.removeAll(keepingCapacity: true)
                        }
                    }
                }
            } else {
                let data = try Data(contentsOf: url)
                for byte in data {
                    resultsLock.withLock {
                        let chunkIndex = writeHead/Self.chunkSize
                        let indexInChunk = writeHead % Self.chunkSize
                        
                        /// If there isn't enough space, allocate a new chunk.
                        if chunkIndex >= byteChunks.count {
                            byteChunks.append(Bytes(repeating: 0, count: Self.chunkSize))
                        }
                        
                        /// Write the byte and advance the head.
                        byteChunks[chunkIndex][indexInChunk] = byte
                        
                        writeHead += 1
                        
                        /// If anyone is waiting on us, let them know.
                        if !continuations.isEmpty {
                            for continuation in continuations {
                                continuation.resume()
                            }
                            continuations.removeAll(keepingCapacity: true)
                        }
                    }
                }
            }
            #else
            let data = try Data(contentsOf: url)
            for byte in data {
                resultsLock.withLock {
                    let chunkIndex = writeHead/Self.chunkSize
                    let indexInChunk = writeHead % Self.chunkSize
                    
                    /// If there isn't enough space, allocate a new chunk.
                    if chunkIndex >= byteChunks.count {
                        byteChunks.append(Bytes(repeating: 0, count: Self.chunkSize))
                    }
                    
                    /// Write the byte and advance the head.
                    byteChunks[chunkIndex][indexInChunk] = byte
                    
                    writeHead += 1
                    
                    /// If anyone is waiting on us, let them know.
                    if !continuations.isEmpty {
                        for continuation in continuations {
                            continuation.resume()
                        }
                        continuations.removeAll(keepingCapacity: true)
                    }
                }
            }
            #endif
            
            /// Let the reader know the sequence is done.
            resultsLock.withLock {
                finalResult = .success(())
                /// If anyone is waiting on us, let them know.
                if !continuations.isEmpty {
                    for continuation in continuations {
                        continuation.resume()
                    }
                    continuations.removeAll(keepingCapacity: true)
                }
            }
        } catch {
            /// Let the reader know the sequence failed.
            resultsLock.withLock {
                finalResult = .failure(error)
                /// If anyone is waiting on us, let them know.
                if !continuations.isEmpty {
                    for continuation in continuations {
                        continuation.resume()
                    }
                    continuations.removeAll(keepingCapacity: true)
                }
            }
        }
    }
    
    func byte(for readHead: Int) async throws -> Byte? {
        /// If we overtake the write head, check if we are done, otherwise suspend until there are more bytes
        resultsLock.unsafeLock()
        if readHead < writeHead {
            resultsLock.unsafeUnlock()
        } else {
            if let finalResult {
                resultsLock.unsafeUnlock()
                /// Return nil or throw the error we encountered while reading.
                try finalResult.get()
                return nil
            }
            
            await withCheckedContinuation { continuation in
                continuations.append(continuation)
                resultsLock.unsafeUnlock()
            }
            
            /// Check one more time if we are actually at the end and an error was thrown or if we finished.
            let (writeHead, finalResult) = resultsLock.withLock { (self.writeHead, self.finalResult) }
            if readHead >= writeHead, let finalResult {
                try finalResult.get()
                return nil
            }
        }
        
        let chunkIndex = readHead/Self.chunkSize
        let indexInChunk = readHead % Self.chunkSize
        return resultsLock.withLock { byteChunks[chunkIndex][indexInChunk] }
    }
}

extension AsyncFileReader: AsyncSequence {
    nonisolated func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(fileReader: self)
    }
    
    struct AsyncIterator: AsyncIteratorProtocol {
        let fileReader: AsyncFileReader
        var readHead: Int = 0
        
        mutating func next() async throws -> Byte? {
            let readHead = readHead
            self.readHead += 1
            return try await fileReader.byte(for: readHead)
        }
        
        mutating func next(isolation actor: isolated (any Actor)?) async throws(any Error) -> Byte? {
            let readHead = readHead
            self.readHead += 1
            return try await fileReader.byte(for: readHead)
        }
    }
}
