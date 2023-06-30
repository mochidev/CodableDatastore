//
//  DatastorePage.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-23.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation
import AsyncSequenceReader
import Bytes

typealias DatastorePageIdentifier = DatedIdentifier<DiskPersistence<ReadOnly>.Datastore.Page>

extension DiskPersistence.Datastore {
    actor Page: Identifiable {
        unowned let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: ID
        
        var blocksReaderTask: Task<MultiplexedAsyncSequence<AnyReadableSequence<DatastorePageEntryBlock>>, Error>?
        
        var isPersisted: Bool
        
        init(
            datastore: DiskPersistence<AccessMode>.Datastore,
            id: ID,
            blocks: [DatastorePageEntryBlock]? = nil
        ) {
            self.datastore = datastore
            self.id = id
            self.blocksReaderTask = blocks.map { blocks in
                Task {
                    MultiplexedAsyncSequence(base: AnyReadableSequence(blocks))
                }
            }
            self.isPersisted = blocks == nil
        }
    }
}

// MARK: - Helper Types

extension DiskPersistence.Datastore.Page {
    struct ID: Hashable {
        let index: DiskPersistence.Datastore.Index.ID
        let page: DatastorePageIdentifier
    }
}

// MARK: - Common URL Accessors

extension DiskPersistence.Datastore.Page {
    /// The URL that points to the page.
    nonisolated var pageURL: URL {
        let baseURL = datastore.pagesURL(for: id.index)
        
        guard let components = try? id.page.components else { preconditionFailure("Components could not be determined for Page.") }
        
        return baseURL
            .appendingPathComponent(components.year, isDirectory: true)
            .appendingPathComponent(components.monthDay, isDirectory: true)
            .appendingPathComponent(components.hourMinute, isDirectory: true)
            .appendingPathComponent("\(id).datastorepage", isDirectory: false)
    }
}

// MARK: - Persistence

extension DiskPersistence.Datastore.Page {
    private var readableSequence: AnyReadableSequence<Byte> {
        get throws {
#if canImport(Darwin)
            if #available(macOS 12.0, iOS 15, watchOS 8, tvOS 15, *) {
                return AnyReadableSequence(pageURL.resourceBytes)
            } else {
                return AnyReadableSequence(try Data(contentsOf: pageURL))
            }
#else
            return AnyReadableSequence(try Data(contentsOf: pageURL))
#endif
        }
    }
    
    private var blocks: MultiplexedAsyncSequence<AnyReadableSequence<DatastorePageEntryBlock>> {
        get async throws {
            if let blocksReaderTask {
                return try await blocksReaderTask.value
            }
            
            let readerTask = Task {
                let sequence = try readableSequence
                
                var iterator = sequence.makeAsyncIterator()
                
                try await iterator.check(utf8: "PAGE\n")
                
                /// Pages larger than 1 GB are unsupported.
                let transformation = try await iterator.collect(max: 1024*1024*1024) { sequence in
                    sequence.iteratorMap { iterator in
                        guard let block = try await iterator.next(DatastorePageEntryBlock.self)
                        else { throw DiskPersistenceError.invalidPageFormat }
                        return block
                    }
                }
                
                if let transformation {
                    return MultiplexedAsyncSequence(base: AnyReadableSequence(transformation))
                } else {
                    return MultiplexedAsyncSequence(base: AnyReadableSequence([]))
                }
            }
            isPersisted = true
            blocksReaderTask = readerTask
            await datastore.mark(identifier: id, asLoaded: true)
            
            return try await readerTask.value
        }
    }
}

actor MultiplexedAsyncSequence<Base: AsyncSequence>: AsyncSequence {
    typealias Element = Base.Element
    
    private var cachedEntries: [Task<Element?, Error>] = []
    private var baseIterator: Base.AsyncIterator
    
    struct AsyncIterator: AsyncIteratorProtocol {
        let base: MultiplexedAsyncSequence
        var index: Array.Index = 0
        
        mutating func next() async throws -> Element? {
            let index = index
            self.index += 1
            return try await base[index]
        }
    }
    
    private subscript(_ index: Int) -> Element? {
        get async throws {
            if index < cachedEntries.count {
                return try await cachedEntries[index].value
            }
            
            precondition(index == cachedEntries.count, "\(index) is out of bounds.")
            
            let lastTask: Task<Element?, Error>? = cachedEntries.last
            
            let newTask = Task {
                /// Make sure previous iteration finished before sourcing the next one.
                _ = try? await lastTask?.value
                
                /// Grab the next iteration, and save a reference back to it. This is only safe since we chain the requests behind previous ones.
                var iteratorCopy = baseIterator
                let nextEntry = try await iteratorCopy.next()
                baseIterator = iteratorCopy
                
                return nextEntry
            }
            cachedEntries.append(newTask)
            return try await newTask.value
        }
    }
    
    nonisolated func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: self)
    }
    
    init(base: Base) {
        baseIterator = base.makeAsyncIterator()
    }
}
