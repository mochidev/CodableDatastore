//
//  DatastoreIndex.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-23.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation
import Bytes

typealias DatastoreIndexIdentifier = TypedIdentifier<DiskPersistence<ReadOnly>.Datastore.Index>

extension DiskPersistence.Datastore {
    actor Index: Identifiable {
        unowned let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: ID
        
        var _manifest: DatastoreIndexManifest?
        var manifestTask: Task<DatastoreIndexManifest, Error>?
        
        var cachedOrderedPages: Task<[LazyTask<DiskPersistence.Datastore.Page>], Error>?
        
        var isPersisted: Bool
        
        init(
            datastore: DiskPersistence<AccessMode>.Datastore,
            id: ID,
            manifest: DatastoreIndexManifest? = nil
        ) {
            self.datastore = datastore
            self.id = id
            self._manifest = manifest
            self.isPersisted = manifest == nil
        }
    }
}

// MARK: Hashable

extension DiskPersistence.Datastore.Index: Hashable {
    static func == (lhs: DiskPersistence<AccessMode>.Datastore.Index, rhs: DiskPersistence<AccessMode>.Datastore.Index) -> Bool {
        lhs === rhs
    }
    
    nonisolated func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

// MARK: - Helper Types

extension DiskPersistence.Datastore.Index {
    enum ID: Hashable {
        case primary(manifest: DatastoreIndexManifestIdentifier)
        case direct(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
        case secondary(index: DatastoreIndexIdentifier, manifest: DatastoreIndexManifestIdentifier)
        
        var manifestID: DatastoreIndexManifestIdentifier {
            switch self {
            case .primary(let id),
                 .direct(_, let id),
                 .secondary(_, let id):
                return id
            }
        }
        
        func with(manifestID: DatastoreIndexManifestIdentifier) -> Self {
            switch self {
            case .primary: return .primary(manifest: manifestID)
            case .direct(let indexID, _): return .direct(index: indexID, manifest: manifestID)
            case .secondary(let indexID, _): return .secondary(index: indexID, manifest: manifestID)
            }
        }
    }
}

// MARK: - Common URL Accessors

extension DiskPersistence.Datastore.Index {
    /// The URL that points to the manifest.
    nonisolated var manifestURL: URL {
        datastore
            .manifestsURL(for: id)
            .appendingPathComponent("\(id.manifestID).indexmanifest", isDirectory: false)
    }
}

// MARK: - Persistence

extension DiskPersistence.Datastore.Index {
    private var manifest: DatastoreIndexManifest {
        get async throws {
            if let manifestTask { return try await manifestTask.value }
            
            let loader = Task {
                if let _manifest { return _manifest }
                
                let manifest = try await DatastoreIndexManifest(contentsOf: manifestURL, id: id.manifestID)
                
                isPersisted = true
                _manifest = manifest
                
                await datastore.mark(identifier: id, asLoaded: true)
                
                return manifest
            }
            manifestTask = loader
            return try await loader.value
        }
    }
    
    func persistIfNeeded() async throws {
        guard !isPersisted else { return }
        guard let manifest = _manifest else {
            assertionFailure("Persisting a manifest that does not exist.")
            return
        }
        
        /// Make sure the directories exists first.
        try FileManager.default.createDirectory(at: datastore.manifestsURL(for: id), withIntermediateDirectories: true)
        
        /// Encode the provided manifest, and write it to disk.
        let data = Data(manifest.bytes)
        try data.write(to: manifestURL, options: .atomic)
        isPersisted = true
        await datastore.mark(identifier: id, asLoaded: true)
    }
}

// MARK: - Pages

extension DiskPersistence.Datastore.Index {
    var orderedPages: [LazyTask<DiskPersistence.Datastore.Page>] {
        get async throws {
            if let cachedOrderedPages { return try await cachedOrderedPages.value }
            let task = Task {
                try await manifest.orderedPageIDs.map { pageID in
                    LazyTask { await self.datastore.page(for: .init(index: self.id, page: pageID)) }
                }
            }
            cachedOrderedPages = task
            return try await task.value
        }
    }
    
    /// Return the page index where a proposed entry would reside on, wether it exists or not.
    ///
    /// This page would have at least one entry with which to achor itself to. For instance, if a page is missing any anchorable information (ie. its header is on a previous page), it won't be returned, instead opting for a page before or after it.
    ///
    /// This means that if a page is returned, and the first complete entry appears mid-way on the page, but a new entry were to be positioned before it, the caller can assume it would reside _after_ any imcomplete entries, but _before_ the first complete one.
    ///
    /// If the returned page contains the start of an entry which would be located before the proposed entry, it is up to the caller to scan forward until that entry is finished and insert the proposed entry after that point.
    ///
    /// ### Examples
    ///
    /// Below are some examples of how this algorithm is expected to perform.
    ///
    /// `5` in `[0, 1, 2]`:
    /// ```
    /// [0, 1, 2]
    ///  0 + 3/2 -> 1.5 -> 1
    ///     1 <= 5 ✓
    ///    [1, 2]
    ///     1 + 2/2 -> 2
    ///        2 <= 5 ✓
    ///       [2]
    /// ```
    ///
    /// `2` in `[0, 1, 2]`:
    /// ```
    /// [0, 1, 2]
    ///  0 + 3/2 -> 1.5 -> 1
    ///     1 <= 2 ✓
    ///    [1, 2]
    ///     1 + 2/2 -> 2
    ///        2 <= 2 ✓
    ///       [2]
    /// ```
    ///
    /// `1.1` in `[0, 1, 2]`:
    /// ```
    /// [0, 1, 2]
    ///  0 + 3/2 -> 1.5 -> 1
    ///     1 <= 1.1 ✓
    ///    [1, 2]
    ///     1 + 2/2 -> 2
    ///        2 <= 1.1 ×
    ///    [1]
    /// ```
    ///
    /// `1` in `[0, 1, 2]`:
    /// ```
    /// [0, 1, 2]
    ///  0 + 3/2 -> 1.5 -> 1
    ///     1 <= 1 ✓
    ///    [1, 2]
    ///     1 + 2/2 -> 2
    ///        2 <= 1 ×
    ///    [1]
    /// ```
    ///
    /// `0.5` in `[0, 1, 2]`:
    /// ```
    /// [0, 1, 2]
    ///  0 + 3/2 -> 1.5 -> 1
    ///     1 <= 0.5 ×
    /// [0]
    /// ```
    ///
    /// `0` in `[0, 1, 2]`:
    /// ```
    /// [0, 1, 2]
    ///  0 + 3/2 -> 1.5 -> 1
    ///     1 <= 0 ×
    /// [0]
    /// ```
    ///
    /// `-1` in `[0, 1, 2]`:
    /// ```
    /// [0, 1, 2]
    ///  0 + 3/2 -> 1.5 -> 1
    ///     1 <= -1 ×
    /// [0]
    /// ```
    ///
    /// `6` in `[0, 1, 2, 3, 4, 5]`:
    /// ```
    /// [0, 1, 2, 3, 4, 5]
    ///           ^
    ///           3 <= 6 ✓
    ///          [3, 4, 5]
    ///              ^
    ///              4 <= 6 ✓
    ///             [4, 5]
    ///                 ^
    ///                 5 <= 6 ✓
    ///                [5]
    /// ```
    ///
    /// `3.5` in `[0, 1, 2, 3, 4, 5]`:
    /// ```
    /// [0, 1, 2, 3, 4, 5]
    ///           ^
    ///           3 <= 3.5 ✓
    ///          [3, 4, 5]
    ///              ^
    ///              4 <= 3.5 ×
    ///          [3]
    /// ```
    ///
    /// `2.1` in `[0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2]`:
    /// ```
    /// [0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2]
    ///                 >-----------^
    ///                             2 <= 2.1 ✓
    ///                            [2, 2]
    ///                                >×
    ///                            [2] // Caller should scan forward at this point
    /// ```
    ///
    /// `1.1` in `[0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2]`:
    /// ```
    /// [0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2]
    ///                 >-----------^
    ///                             2 <= 1.1 ×
    /// [0, 1, 1, 1, 1, 1, 1, 1, 1]
    ///              >------------×
    /// [0, 1, 1, 1]
    ///        >---×
    /// [0, 1]
    ///     ^--^--^--^--^--^--^--^ // Scanning will stop after enough header data for the entry is aquired, usually after a single page or two.
    ///     1 <= 1.1 ✓
    ///    [1]
    /// ```
    ///
    /// `0.1` in `[0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2]`:
    /// ```
    /// [0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2]
    ///                 >-----------^
    ///                             2 <= 0.1 ×
    /// [0, 1, 1, 1, 1, 1, 1, 1, 1]
    ///              >------------×
    /// [0, 1, 1, 1]
    ///        >---×
    /// [0, 1]
    ///     ^--^--^--^--^--^--^--^
    ///     1 <= 0.1 ×
    /// [0]
    /// ```
    /// - Parameters:
    ///   - proposedEntry: The entry to use in comparison with other persisted entries.
    ///   - pages: A collection of pages to check against.
    ///   - comparator: A comparator to determine order and equality between the proposed entry and a persisted one.
    /// - Returns: The index within the pages collection where the entry would reside.
    func pageIndex<T>(
        for proposedEntry: T,
        in pages: [LazyTask<DiskPersistence.Datastore.Page>],
        comparator: (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
    ) async throws -> Int? {
        var slice = pages[...]
        
        /// Cursor should point to making the first page.
        guard !slice.isEmpty
        else { return nil }
        
        /// Loosely based off of https://stackoverflow.com/questions/26678362/how-do-i-insert-an-element-at-the-correct-position-into-a-sorted-array-in-swift/70645571#70645571
        /// Continue the process until we have a slice with a single entry in it.
        while slice.count > 1 {
            /// Grab the middle index of our slice. We keep the original and a mutable variant that can scan ahead for ranges of pages.
            let originalMiddle = slice.index(slice.startIndex, offsetBy: slice.count/2)
            var middle = slice.index(slice.startIndex, offsetBy: slice.count/2)
            
            var bytesForFirstEntry: Bytes?
            var firstEntryOfPage: DatastorePageEntry?
            
            /// Start checking the page at the middle index, continuing to scan until we build up enough of an entry to compare to.
            pageIterator: for page in pages[middle...] {
                let blocks = try await page.value.blocks
                
                /// Start scanning the page block-by-block, continuing to scan until we build up enough of an entry to compare to.
                for try await block in blocks {
                    switch block {
                    case .complete(let bytes):
                        /// We have a complete entry, lets use it and stop scanning
                        firstEntryOfPage = try DatastorePageEntry(bytes: bytes, isPartial: false)
                        break pageIterator
                    case .head(let bytes):
                        /// We are starting an entry, but will need to go to the next page.
                        bytesForFirstEntry = bytes
                    case .slice(let bytes):
                        /// In the first position, lets skip it.
                        guard bytesForFirstEntry != nil else { continue }
                        /// In the final position, lets save and continue.
                        bytesForFirstEntry?.append(contentsOf: bytes)
                    case .tail(let bytes):
                        /// In the first position, lets skip it.
                        guard bytesForFirstEntry != nil else { continue }
                        /// In the final position, lets save and stop.
                        bytesForFirstEntry?.append(contentsOf: bytes)
                        firstEntryOfPage = try DatastorePageEntry(bytes: bytesForFirstEntry!, isPartial: false)
                        break pageIterator
                    }
                    
                    /// If we have some bytes, attempt to decode them into an entry.
                    if let bytesForFirstEntry {
                        firstEntryOfPage = try? DatastorePageEntry(bytes: bytesForFirstEntry, isPartial: false)
                    }
                    
                    /// If we have an entry, stop scanning as we can go ahead and operate on it.
                    if firstEntryOfPage != nil { break pageIterator }
                }
                
                /// If we had to advance a page and didn't yet start accumulating data, move our middle since it would be pointless to check that page again if the proposed entry was ordered after the persisted one we found.
                if bytesForFirstEntry == nil, firstEntryOfPage == nil {
                    middle = slice.index(middle, offsetBy: 1)
                    /// If we've gone past the slice, stop here.
                    guard middle < slice.endIndex
                    else { break }
                }
            }
            
            guard bytesForFirstEntry != nil || firstEntryOfPage != nil else {
                /// If we didn't encounter a single start sequence, a real one must be located before this point, so don't bother checking _any_ of the pages we scanned through a second time.
                slice = slice[..<originalMiddle]
                continue
            }
            
            /// If we don't have a first entry by now, stop here.
            guard let firstEntryOfPage
            else { throw DiskPersistenceError.invalidPageFormat }
            
            if try comparator(proposedEntry, firstEntryOfPage) == .ascending {
                /// If the proposed entry is strictly before the first of the page, repeat the search prior to this page.
                slice = slice[..<middle]
            } else {
                /// If the proposed entry is equal to the first of the page, or comes after it, use the later half to repeat the search.
                slice = slice[middle...]
            }
        }
        return slice.startIndex
    }
    
    func entry<T>(
        for proposedEntry: T,
        comparator: (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
    ) async throws -> (
        cursor: DiskPersistence.InstanceCursor,
        entry: DatastorePageEntry
    ) {
        try await entry(for: proposedEntry, in: try await orderedPages, comparator: comparator)
    }
    
    func entry<T>(
        for proposedEntry: T,
        in pages: [LazyTask<DiskPersistence.Datastore.Page>],
        comparator: (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
    ) async throws -> (
        cursor: DiskPersistence.InstanceCursor,
        entry: DatastorePageEntry
    ) {
        /// Get the page the entry should reside on
        guard let startingPageIndex = try await pageIndex(for: proposedEntry, in: pages, comparator: comparator)
        else { throw DatastoreInterfaceError.instanceNotFound }
        
        
        var bytesForEntry: Bytes?
        var isEntryComplete = false
        var blocksForEntry: [DiskPersistence.CursorBlock] = []
        var pageIndex = startingPageIndex
        
        pageIterator: for page in pages[startingPageIndex...] {
            defer { pageIndex += 1 }
            let page = await page.value
            let blocks = try await page.blocks
            var blockIndex = 0
            
            for try await block in blocks {
                defer { blockIndex += 1 }
                switch block {
                case .complete(let bytes):
                    /// We have a complete entry, lets use it and stop scanning
                    bytesForEntry = bytes
                    isEntryComplete = true
                case .head(let bytes):
                    /// We are starting an entry, but will need to go to the next page.
                    bytesForEntry = bytes
                case .slice(let bytes):
                    /// In the first position, lets skip it.
                    guard bytesForEntry != nil else { continue }
                    /// In the final position, lets save and continue.
                    bytesForEntry?.append(contentsOf: bytes)
                case .tail(let bytes):
                    /// In the first position, lets skip it.
                    guard bytesForEntry != nil else { continue }
                    /// In the final position, lets save and stop.
                    bytesForEntry?.append(contentsOf: bytes)
                    isEntryComplete = true
                }
                
                blocksForEntry.append(DiskPersistence.CursorBlock(
                    pageIndex: pageIndex,
                    page: page,
                    blockIndex: blockIndex
                ))
                
                if let bytes = bytesForEntry, isEntryComplete {
                    let entry = try DatastorePageEntry(bytes: bytes, isPartial: false)
                    
                    switch try comparator(proposedEntry, entry) {
                    case .descending:
                        /// Move on to the next entry.
                        break
                    case .equal:
                        /// We found the entry, so return it.
                        return (
                            cursor: DiskPersistence.InstanceCursor(
                                persistence: datastore.snapshot.persistence,
                                datastore: datastore,
                                index: self,
                                blocks: blocksForEntry
                            ),
                            entry: entry
                        )
                    case .ascending:
                        /// We must have passed the entry, which could only happen if it didn't exist.
                        throw DatastoreInterfaceError.instanceNotFound
                    }
                    
                    isEntryComplete = false
                    bytesForEntry = nil
                    blocksForEntry = []
                }
            }
        }
        
        /// If we got this far, we didn't encounter the entry, and must have passed every entry along the way.
        throw DatastoreInterfaceError.instanceNotFound
    }
    
    func insertionCursor<T>(
        for proposedEntry: T,
        comparator: (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
    ) async throws -> DiskPersistence.InsertionCursor {
        try await insertionCursor(for: proposedEntry, in: try await orderedPages, comparator: comparator)
    }
    
    func insertionCursor<T>(
        for proposedEntry: T,
        in pages: [LazyTask<DiskPersistence.Datastore.Page>],
        comparator: (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
    ) async throws -> DiskPersistence.InsertionCursor {
        /// Get the page the entry should reside on
        guard let startingPageIndex = try await pageIndex(for: proposedEntry, in: pages, comparator: comparator)
        else {
            return DiskPersistence.InsertionCursor(
                persistence: datastore.snapshot.persistence,
                datastore: datastore,
                index: self,
                insertAfter: nil
            )
        }
        
        var bytesForEntry: Bytes?
        var isEntryComplete = false
        var previousBlock: DiskPersistence.CursorBlock? = nil
        var currentBlock: DiskPersistence.CursorBlock? = nil
        var pageIndex = startingPageIndex
        
        pageIterator: for page in pages[startingPageIndex...] {
            defer { pageIndex += 1 }
            let page = await page.value
            let blocks = try await page.blocks
            var blockIndex = 0
            
            for try await block in blocks {
                defer { blockIndex += 1 }
                switch block {
                case .complete(let bytes):
                    /// We have a complete entry, lets use it and stop scanning
                    bytesForEntry = bytes
                    isEntryComplete = true
                case .head(let bytes):
                    /// We are starting an entry, but will need to go to the next page.
                    bytesForEntry = bytes
                case .slice(let bytes):
                    /// In the first position, lets skip it.
                    guard bytesForEntry != nil else { continue }
                    /// In the final position, lets save and continue.
                    bytesForEntry?.append(contentsOf: bytes)
                case .tail(let bytes):
                    /// In the first position, lets skip it.
                    guard bytesForEntry != nil else { continue }
                    /// In the final position, lets save and stop.
                    bytesForEntry?.append(contentsOf: bytes)
                    isEntryComplete = true
                }
                
                currentBlock = DiskPersistence.CursorBlock(
                    pageIndex: pageIndex,
                    page: page,
                    blockIndex: blockIndex
                )
                defer { previousBlock = currentBlock }
                
                if let bytes = bytesForEntry, isEntryComplete {
                    let entry = try DatastorePageEntry(bytes: bytes, isPartial: false)
                    
                    switch try comparator(proposedEntry, entry) {
                    case .descending:
                        /// Move on to the next entry.
                        break
                    case .equal:
                        /// We found an exact matching entry, stop here.
                        throw DatastoreInterfaceError.instanceAlreadyExists
                    case .ascending:
                        /// We just passed the proposed entry's location, so return the previous block.
                        return DiskPersistence.InsertionCursor(
                            persistence: datastore.snapshot.persistence,
                            datastore: datastore,
                            index: self,
                            insertAfter: previousBlock
                        )
                    }
                    
                    isEntryComplete = false
                    bytesForEntry = nil
                }
            }
        }
        
        /// If we got this far, we didn't encounter anything sorted after the proposed entry, so it must go last in the index.
        return DiskPersistence.InsertionCursor(
            persistence: datastore.snapshot.persistence,
            datastore: datastore,
            index: self,
            insertAfter: previousBlock
        )
    }
}

// MARK: - Mutations

extension DiskPersistence.Datastore.Index {
    func manifest(
        inserting entry: DatastorePageEntry,
        at insertionCursor: DiskPersistence.InsertionCursor,
        targetPageSize: Int = 32*1024
    ) async throws -> (
        manifest: DatastoreIndexManifest,
        createdPages: [DiskPersistence.Datastore.Page]
    ) {
        let actualPageSize = max(targetPageSize, 4*1024) - DiskPersistence.Datastore.Page.headerSize
        
        guard
            insertionCursor.datastore === datastore,
            insertionCursor.index === self
        else { throw DatastoreInterfaceError.staleCursor }
        
        var manifest = try await manifest
        
        /// Prepare the list of existing pages, removing old entries.
        var orderedPageInfos = manifest.orderedPageIDs.map { DatastoreIndexManifest.PageInfo.existing($0) }
        
        let newIndexID = id.with(manifestID: DatastoreIndexManifestIdentifier())
        var createdPages: [DiskPersistence.Datastore.Page] = []
        
        switch (insertionCursor.insertAfter, orderedPageInfos.isEmpty) {
        /// We have no pages yet, so simply create blocks out of the entry and create a page with each.
        case (.none, true):
            let newPageBlocks = entry.blocks(
                remainingPageSpace: actualPageSize,
                maxPageSpace: actualPageSize
            ).map { [$0] }
            for pageBlocks in newPageBlocks {
                assert(pageBlocks.encodedSize <= actualPageSize)
                let page = DiskPersistence.Datastore.Page(
                    datastore: datastore,
                    id: .init(index: newIndexID, page: DatastorePageIdentifier()),
                    blocks: pageBlocks
                )
                createdPages.append(page)
                orderedPageInfos.append(.added(page.id.page))
            }
            
        /// Insert the new entries before any existing entries. If the page after these entries is small enough to fix, include it.
        case (.none, false):
            /// First, separate the new entry into the necessary amount of pages it'll need.
            var newPageBlocks = entry.blocks(
                remainingPageSpace: actualPageSize,
                maxPageSpace: actualPageSize
            ).map { [$0] }
            
            /// Load the first page to see how large it is compared to the amount of space we have left on our final new page
            let firstPage = await datastore.page(for: .init(index: id, page: orderedPageInfos[0].id))
            let firstPageBlocks = try await firstPage.blocks.reduce(into: [DatastorePageEntryBlock]()) { $0.append($1) }
            
            /// Calculate how much space remains on the final new page, and insert the existing blocks if they all fit.
            /// Note that we are guaranteed to have at least one new page.
            let remainingSpace = actualPageSize - newPageBlocks.last!.encodedSize
            var insertionIndex = 0
            if firstPageBlocks.encodedSize <= remainingSpace {
                /// Mark the page we are removing accordingly
                orderedPageInfos[0] = .removed(orderedPageInfos[0].id)
                insertionIndex += 1
                
                /// Import the blocks into the last page.
                newPageBlocks[newPageBlocks.count - 1].append(contentsOf: firstPageBlocks)
            }
            
            /// Create the page objects for the newly-segmented blocks.
            for pageBlocks in newPageBlocks {
                defer { insertionIndex += 1 }
                assert(pageBlocks.encodedSize <= actualPageSize)
                let page = DiskPersistence.Datastore.Page(
                    datastore: datastore,
                    id: .init(index: newIndexID, page: DatastorePageIdentifier()),
                    blocks: pageBlocks
                )
                createdPages.append(page)
                orderedPageInfos.insert(.added(page.id.page), at: insertionIndex)
            }
        case (.some(_), _): break
        }
        
        if !createdPages.isEmpty {
            manifest.id = newIndexID.manifestID
            manifest.orderedPages = orderedPageInfos
        }
        
        return (manifest: manifest, createdPages: createdPages)
    }
    
    func manifest(
        replacing entry: DatastorePageEntry,
        at instanceCursor: DiskPersistence.InstanceCursor,
        targetPageSize: Int = 32*1024
    ) async throws -> (
        manifest: DatastoreIndexManifest,
        createdPages: [DiskPersistence.Datastore.Page]
    ) {
        preconditionFailure("Unimplemented")
    }
}
