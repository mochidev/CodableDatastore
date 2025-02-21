//
//  DatastorePage.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-23.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation
import AsyncSequenceReader
import Bytes

typealias DatastorePageIdentifier = DatedIdentifier<DiskPersistence<ReadOnly>.Datastore.Page>

extension DiskPersistence.Datastore {
    actor Page: Identifiable {
        let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: PersistenceDatastorePageID
        
        var blocksReaderTask: Task<MultiplexedAsyncSequence<AnyReadableSequence<DatastorePageEntryBlock>>, Error>?
        
        var isPersisted: Bool
        
        init(
            datastore: DiskPersistence<AccessMode>.Datastore,
            id: PersistenceDatastorePageID,
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
        
        deinit {
            Task { [id, datastore] in
                await datastore.invalidate(id)
            }
        }
    }
}

// MARK: Hashable

extension DiskPersistence.Datastore.Page: Hashable {
    static func == (lhs: DiskPersistence<AccessMode>.Datastore.Page, rhs: DiskPersistence<AccessMode>.Datastore.Page) -> Bool {
        lhs === rhs
    }
    
    nonisolated func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

// MARK: - Helper Types

typealias PersistenceDatastorePageID = DiskPersistence<ReadOnly>.Datastore.PageID

extension DiskPersistence<ReadOnly>.Datastore {
    struct PageID: Hashable {
        let index: PersistenceDatastoreIndexID
        let page: DatastorePageIdentifier
        
        var withoutManifest: Self {
            Self.init(
                index: index.with(manifestID: .init(rawValue: "")),
                page: page
            )
        }
    }
}

// MARK: - Common URL Accessors

extension DiskPersistence.Datastore.Page {
    /// The URL that points to the page.
    nonisolated var pageURL: URL {
        datastore.pageURL(for: id)
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
    
    private nonisolated func performRead(sequence: AnyReadableSequence<Byte>) async throws -> MultiplexedAsyncSequence<AnyReadableSequence<DatastorePageEntryBlock>> {
        var iterator = sequence.makeAsyncIterator()
        
        try await iterator.check(Self.header)
        
        /// Pages larger than 1 GB are unsupported.
        let transformation = try await iterator.collect(max: Configuration.maximumPageSize) { sequence in
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
    
    var blocks: MultiplexedAsyncSequence<AnyReadableSequence<DatastorePageEntryBlock>> {
        get async throws {
            if let blocksReaderTask {
                return try await blocksReaderTask.value
            }
            
            let readerTask = Task {
                try await performRead(sequence: try readableSequence)
            }
            isPersisted = true
            blocksReaderTask = readerTask
            await datastore.mark(identifier: id, asLoaded: true)
            
            return try await readerTask.value
        }
    }
    
    func persistIfNeeded() async throws {
        guard !isPersisted else { return }
        let blocks = try await Array(blocks)
        let bytes = blocks.reduce(into: Self.header) { $0.append(contentsOf: $1.bytes) }
        
        let pageURL = pageURL
        /// Make sure the directories exists first.
        try FileManager.default.createDirectory(at: pageURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        
        /// Write the bytes for the page to disk.
        try Data(bytes).write(to: pageURL, options: .atomic)
        isPersisted = true
        await datastore.mark(identifier: id, asLoaded: true)
    }
    
    static var header: Bytes { "PAGE\n".utf8Bytes }
    static var headerSize: Int { header.count }
}

actor MultiplexedAsyncSequence<Base: AsyncSequence & Sendable>: AsyncSequence where Base.Element: Sendable, Base.AsyncIterator: Sendable, Base.AsyncIterator.Element: Sendable {
    typealias Element = Base.Element
    
    private var cachedEntries: [Task<Element?, Error>] = []
    private var baseIterator: Base.AsyncIterator?
    
    struct AsyncIterator: AsyncIteratorProtocol & Sendable {
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
                let (nextEntry, iteratorCopy) = try await nextBase(iterator: baseIterator)
                baseIterator = iteratorCopy
                
                return nextEntry
            }
            cachedEntries.append(newTask)
            return try await newTask.value
        }
    }
    
    /// Return the next base iterator to use along with the current entry, or nil if we've reached the end, so we don't retail the open file handles in our memory caches.
    nonisolated func nextBase(iterator: Base.AsyncIterator?) async throws -> (Element?, Base.AsyncIterator?) {
        var iteratorCopy = iterator
        let nextEntry = try await iteratorCopy?.next()
        return (nextEntry, nextEntry.flatMap { _ in iteratorCopy })
    }
    
    nonisolated func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: self)
    }
    
    init(base: Base) {
        baseIterator = base.makeAsyncIterator()
    }
}

extension RangeReplaceableCollection {
    init<S: AsyncSequence>(_ sequence: S) async throws where S.Element == Element {
        self = try await sequence.reduce(into: Self.init()) { @Sendable partialResult, element in
            partialResult.append(element)
        }
    }
}
