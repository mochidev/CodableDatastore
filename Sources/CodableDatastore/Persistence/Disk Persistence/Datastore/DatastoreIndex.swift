//
//  DatastoreIndex.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-23.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

#if canImport(Darwin)
import Foundation
#else
@preconcurrency import Foundation
#endif
import Bytes

typealias DatastoreIndexIdentifier = TypedIdentifier<DiskPersistence<ReadOnly>.Datastore.Index>

extension DiskPersistence.Datastore {
    actor Index: Identifiable {
        let datastore: DiskPersistence<AccessMode>.Datastore
        
        let id: ID
        
        var _manifest: DatastoreIndexManifest?
        var manifestTask: Task<DatastoreIndexManifest, Error>?
        
        var cachedOrderedPages: Task<[LazyTask<DiskPersistence.Datastore.Page>?], Error>?
        
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
        
        deinit {
            Task { [id, datastore] in
                await datastore.invalidate(id)
            }
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
        
        var indexID: DatastoreRootManifest.IndexID {
            switch self {
            case .primary(_):           .primary
            case .direct(let id, _):    .direct(index: id)
            case .secondary(let id, _): .secondary(index: id)
            }
        }
        
        var manifestID: DatastoreIndexManifestIdentifier {
            switch self {
            case .primary(let id):      id
            case .direct(_, let id):    id
            case .secondary(_, let id): id
            }
        }
        
        func with(manifestID: DatastoreIndexManifestIdentifier) -> Self {
            switch self {
            case .primary:                      .primary(manifest: manifestID)
            case .direct(let indexID, _):       .direct(index: indexID, manifest: manifestID)
            case .secondary(let indexID, _):    .secondary(index: indexID, manifest: manifestID)
            }
        }
        
        init(_ id: DatastoreRootManifest.IndexManifestID) {
            switch id {
            case .primary(let manifest):                self = .primary(manifest: manifest)
            case .direct(let index, let manifest):      self = .direct(index: index, manifest: manifest)
            case .secondary(let index, let manifest):   self = .secondary(index: index, manifest: manifest)
            }
        }
    }
}

// MARK: - Common URL Accessors

extension DiskPersistence.Datastore.Index {
    /// The URL that points to the manifest.
    nonisolated var manifestURL: URL {
        datastore.manifestURL(for: id)
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
        try FileManager.default.createDirectory(at: datastore.manifestsURL(for: id.indexID), withIntermediateDirectories: true)
        
        /// Encode the provided manifest, and write it to disk.
        let data = Data(manifest.bytes)
        try data.write(to: manifestURL, options: .atomic)
        isPersisted = true
        await datastore.mark(identifier: id, asLoaded: true)
    }
}

// MARK: - Page Lookups

extension DiskPersistence.Datastore.Index {
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
    ///   - requiresCompleteEntries: Set to `true` if the comparator requires a complete entry to operate with.
    ///   - pageBuilder: A closure that provides a cached Page object for the loaded page.
    ///   - comparator: A comparator to determine order and equality between the proposed entry and a persisted one.
    /// - Returns: The index within the pages collection where the entry would reside.
    func pageIndex<T>(
        for proposedEntry: T,
        in pages: [DatastoreIndexManifest.PageInfo],
        requiresCompleteEntries: Bool,
        pageBuilder: @Sendable (_ pageID: DatastorePageIdentifier) async -> DiskPersistence.Datastore.Page,
        comparator: @Sendable (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
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
            pageIterator: for pageInfo in pages[middle...] {
                switch pageInfo {
                case .removed: break
                case .added(let pageID), .existing(let pageID):
                    let page = await pageBuilder(pageID)
                    let blocks = try await page.blocks
                    
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
                            firstEntryOfPage = try? DatastorePageEntry(bytes: bytesForFirstEntry, isPartial: true)
                        }
                        
                        /// If we have an entry, stop scanning as we can go ahead and operate on it. Also make sure that we have a complete entry if one is required by rejecting partial entries when the flag is set.
                        if let firstEntryOfPage, !(requiresCompleteEntries && firstEntryOfPage.isPartial) {
                            break pageIterator
                        }
                    }
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
        requiresCompleteEntries: Bool,
        comparator: @Sendable (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
    ) async throws -> (
        cursor: DiskPersistence.InstanceCursor,
        entry: DatastorePageEntry
    ) {
        try await entry(
            for: proposedEntry,
            in: try await manifest.orderedPages,
            requiresCompleteEntries: requiresCompleteEntries,
            pageBuilder: { await datastore.page(for: .init(index: self.id, page: $0)) },
            comparator: comparator
        )
    }
    
    func entry<T>(
        for proposedEntry: T,
        in pages: [DatastoreIndexManifest.PageInfo],
        requiresCompleteEntries: Bool,
        pageBuilder: @Sendable (_ pageID: DatastorePageIdentifier) async -> DiskPersistence.Datastore.Page,
        comparator: @Sendable (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
    ) async throws -> (
        cursor: DiskPersistence.InstanceCursor,
        entry: DatastorePageEntry
    ) {
        /// Get the page the entry should reside on
        guard
            let startingPageIndex = try await pageIndex(
                for: proposedEntry,
                in: pages,
                requiresCompleteEntries: requiresCompleteEntries,
                pageBuilder: pageBuilder,
                comparator: comparator
            )
        else { throw DatastoreInterfaceError.instanceNotFound }
        
        var bytesForEntry: Bytes?
        var isEntryComplete = false
        var blocksForEntry: [DiskPersistence.CursorBlock] = []
        var pageIndex = startingPageIndex
        
        pageIterator: for pageInfo in pages[startingPageIndex...] {
            defer { pageIndex += 1 }
            
            let page: DiskPersistence.Datastore.Page
            switch pageInfo {
            case .removed: continue
            case .existing(let pageID), .added(let pageID):
                page = await pageBuilder(pageID)
            }
            
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
    
    var firstInsertionCursor: DiskPersistence.InsertionCursor {
        get {
            DiskPersistence.InsertionCursor(
                persistence: datastore.snapshot.persistence,
                datastore: datastore,
                index: self,
                insertAfter: nil
            )
        }
    }
    
    var lastInsertionCursor: DiskPersistence.InsertionCursor {
        get async throws {
            let pages = try await manifest.orderedPages
            var pageIndex = pages.count - 1
            
            for pageInfo in pages.reversed() {
                defer { pageIndex -= 1 }
                
                let page: DiskPersistence.Datastore.Page
                switch pageInfo {
                case .removed: continue
                case .existing(let pageID), .added(let pageID):
                    page = await datastore.page(for: .init(index: self.id, page: pageID))
                }
                
                let blocks = try await Array(page.blocks)
                guard !blocks.isEmpty else { throw DiskPersistenceError.invalidPageFormat }
                
                return DiskPersistence.InsertionCursor(
                    persistence: datastore.snapshot.persistence,
                    datastore: datastore,
                    index: self,
                    insertAfter: DiskPersistence.CursorBlock(
                        pageIndex: pageIndex,
                        page: page,
                        blockIndex: blocks.count - 1
                    )
                )
            }
            
            /// Couldn't find a last page, so the cursor is the same as the first insertion cursor.
            return firstInsertionCursor
        }
    }
    
    func insertionCursor<T>(
        for proposedEntry: T,
        requiresCompleteEntries: Bool,
        comparator: @Sendable (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
    ) async throws -> DiskPersistence.InsertionCursor {
        try await insertionCursor(
            for: proposedEntry,
            in: try await manifest.orderedPages,
            requiresCompleteEntries: requiresCompleteEntries,
            pageBuilder: { await datastore.page(for: .init(index: self.id, page: $0)) },
            comparator: comparator
        )
    }
    
    func insertionCursor<T>(
        for proposedEntry: T,
        in pages: [DatastoreIndexManifest.PageInfo],
        requiresCompleteEntries: Bool,
        pageBuilder: @Sendable (_ pageID: DatastorePageIdentifier) async -> DiskPersistence.Datastore.Page,
        comparator: @Sendable (_ lhs: T, _ rhs: DatastorePageEntry) throws -> SortOrder
    ) async throws -> DiskPersistence.InsertionCursor {
        /// Get the page the entry should reside on
        guard
            let startingPageIndex = try await pageIndex(
                for: proposedEntry,
                in: pages,
                requiresCompleteEntries: requiresCompleteEntries,
                pageBuilder: pageBuilder,
                comparator: comparator
            )
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
        var previousCompleteBlock: DiskPersistence.CursorBlock? = nil
        var currentBlock: DiskPersistence.CursorBlock? = nil
        var pageIndex = startingPageIndex
        
        pageIterator: for pageInfo in pages[startingPageIndex...] {
            defer { pageIndex += 1 }
            
            let page: DiskPersistence.Datastore.Page
            switch pageInfo {
            case .removed: continue
            case .existing(let pageID), .added(let pageID):
                page = await pageBuilder(pageID)
            }
            
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
                    /// In the first position, lets skip it, but add it as the last complete block in case the next one is a step too far.
                    guard bytesForEntry != nil else {
                        previousCompleteBlock = DiskPersistence.CursorBlock(
                            pageIndex: pageIndex,
                            page: page,
                            blockIndex: blockIndex
                        )
                        continue
                    }
                    /// In the final position, lets save and stop.
                    bytesForEntry?.append(contentsOf: bytes)
                    isEntryComplete = true
                }
                
                currentBlock = DiskPersistence.CursorBlock(
                    pageIndex: pageIndex,
                    page: page,
                    blockIndex: blockIndex
                )
                
                /// Make sure to only keep a reference to the end of the last complete block, so if we roll back, we'll have a valid cursor
                defer {
                    switch block {
                    case .complete, .tail: previousCompleteBlock = currentBlock
                    case .head, .slice: break
                    }
                }
                
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
                            insertAfter: previousCompleteBlock
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
            insertAfter: previousCompleteBlock
        )
    }
}

// MARK: - Entry Scans

extension DiskPersistence.Datastore.Index {
    func forwardScanEntries(
        after startCursor: DiskPersistence.InsertionCursor,
        entryHandler: @Sendable (_ entry: DatastorePageEntry) async throws -> Bool
    ) async throws {
        try await forwardScanEntries(
            after: startCursor,
            in: try await manifest.orderedPages,
            pageBuilder: { await datastore.page(for: .init(index: self.id, page: $0)) },
            entryHandler: entryHandler
        )
    }
    
    func forwardScanEntries(
        after startCursor: DiskPersistence.InsertionCursor,
        in pages: [DatastoreIndexManifest.PageInfo],
        pageBuilder: @Sendable (_ pageID: DatastorePageIdentifier) async -> DiskPersistence.Datastore.Page,
        entryHandler: @Sendable (_ entry: DatastorePageEntry) async throws -> Bool
    ) async throws {
        guard
            startCursor.datastore === datastore,
            startCursor.index === self
        else { throw DatastoreInterfaceError.staleCursor }
        
        guard !pages.isEmpty else { return }
        
        let pageStartIndex = startCursor.insertAfter?.pageIndex ?? 0
        
        var bytesForEntry: Bytes = []
        var isEntryComplete = false
        
        for (pageOffsetIndex, pageInfo) in pages[pageStartIndex...].enumerated() {
            switch pageInfo {
            case .removed: continue
            case .added(let pageID), .existing(let pageID):
                let page = await pageBuilder(pageID)
                let blocks = try await page.blocks
                
                var blockCountToDrop = 0
                /// If we are on the first page, use the real block index
                if pageOffsetIndex == 0, let blockIndex = startCursor.insertAfter?.blockIndex {
                    blockCountToDrop = blockIndex + 1
                }
                
                for try await block in blocks.dropFirst(blockCountToDrop) {
                    switch block {
                    case .complete(let bytes):
                        /// We have a complete entry, lets use it and stop scanning
                        bytesForEntry = bytes
                        isEntryComplete = true
                    case .head(let bytes):
                        /// We are starting an entry, but will need to go to the next page.
                        bytesForEntry = bytes
                    case .slice(let bytes):
                        /// In the final position, lets save and continue.
                        bytesForEntry.append(contentsOf: bytes)
                    case .tail(let bytes):
                        /// In the final position, lets save and stop.
                        bytesForEntry.append(contentsOf: bytes)
                        isEntryComplete = true
                    }
                    
                    if isEntryComplete {
                        let entry = try DatastorePageEntry(bytes: bytesForEntry, isPartial: false)
                        
                        let shouldContinue = try await entryHandler(entry)
                        guard shouldContinue else { return }
                        
                        bytesForEntry = []
                        isEntryComplete = false
                    }
                }
            }
        }
    }
    
    func backwardScanEntries(
        before startCursor: DiskPersistence.InsertionCursor,
        entryHandler: @Sendable (_ entry: DatastorePageEntry) async throws -> Bool
    ) async throws {
        try await backwardScanEntries(
            before: startCursor,
            in: try await manifest.orderedPages,
            pageBuilder: { await datastore.page(for: .init(index: self.id, page: $0)) },
            entryHandler: entryHandler
        )
    }
    
    func backwardScanEntries(
        before startCursor: DiskPersistence.InsertionCursor,
        in pages: [DatastoreIndexManifest.PageInfo],
        pageBuilder: @Sendable (_ pageID: DatastorePageIdentifier) async -> DiskPersistence.Datastore.Page,
        entryHandler: @Sendable (_ entry: DatastorePageEntry) async throws -> Bool
    ) async throws {
        guard
            startCursor.datastore === datastore,
            startCursor.index === self
        else { throw DatastoreInterfaceError.staleCursor }
        
        guard !pages.isEmpty else { return }
        
        let pageStartIndex = startCursor.insertAfter?.pageIndex ?? 0
        
        var bytesForEntry: Bytes = []
        var isEntryComplete = false
        
        for (pageOffsetIndex, pageInfo) in pages[...pageStartIndex].reversed().enumerated() {
            switch pageInfo {
            case .removed: continue
            case .added(let pageID), .existing(let pageID):
                let page = await pageBuilder(pageID)
                
                var blockCountToInclude = Int.max
                /// If we are on the first page, use the real block index
                if pageOffsetIndex == 0, let blockIndex = startCursor.insertAfter?.blockIndex {
                    blockCountToInclude = blockIndex + 1
                }
                
                let blocks = try await Array(page.blocks.prefix(blockCountToInclude))
                
                for block in blocks.reversed() {
                    switch block {
                    case .complete(let bytes):
                        /// We have a complete entry, lets use it and stop scanning
                        bytesForEntry = bytes
                        isEntryComplete = true
                    case .tail(let bytes):
                        /// We are starting an entry, but will need to go to the next page.
                        bytesForEntry = bytes
                    case .slice(let bytes):
                        /// In the final position, lets save and continue.
                        bytesForEntry.insert(contentsOf: bytes, at: 0)
                    case .head(let bytes):
                        /// In the final position, lets save and stop.
                        bytesForEntry.insert(contentsOf: bytes, at: 0)
                        isEntryComplete = true
                    }
                    
                    if isEntryComplete {
                        let entry = try DatastorePageEntry(bytes: bytesForEntry, isPartial: false)
                        
                        let shouldContinue = try await entryHandler(entry)
                        guard shouldContinue else { return }
                        
                        bytesForEntry = []
                        isEntryComplete = false
                    }
                }
            }
        }
    }
}

// MARK: - Mutations

extension DiskPersistence.Datastore.Index {
    func manifest(
        inserting entry: DatastorePageEntry,
        at insertionCursor: DiskPersistence.InsertionCursor,
        targetPageSize: Int = 4*1024
    ) async throws -> (
        manifest: DatastoreIndexManifest,
        createdPages: Set<DiskPersistence.Datastore.Page>,
        removedPages: Set<DiskPersistence.Datastore.Page>
    ) {
        let actualPageSize = max(targetPageSize, 4*1024) - DiskPersistence.Datastore.Page.headerSize
        
        guard
            insertionCursor.datastore === datastore,
            insertionCursor.index === self
        else { throw DatastoreInterfaceError.staleCursor }
        
        var manifest = try await manifest
        
        let newIndexID = id.with(manifestID: DatastoreIndexManifestIdentifier())
        var createdPages: Set<DiskPersistence.Datastore.Page> = []
        var removedPages: Set<DiskPersistence.Datastore.Page> = []
        
        var newOrderedPages: [DatastoreIndexManifest.PageInfo] = []
        let insertionPage = insertionCursor.insertAfter?.pageIndex ?? 0
        var newPageBlocks: [[DatastorePageEntryBlock]] = []
        var attemptPageCollation = false
        var finishedInserting = false
        
        let originalOrderedPages = manifest.orderedPages
        for (index, pageInfo) in originalOrderedPages.enumerated() {
            switch pageInfo {
            case .removed:
                /// Skip previously removed entries, unless this index is based on a transient index, and the removed entry was from before the transaction began.
                if !isPersisted {
                    newOrderedPages.append(pageInfo)
                }
                continue
            case .existing(let pageID), .added(let pageID):
                /// If we are processing an earlier page or are finished inserting, just include it as an existing page.
                guard index >= insertionPage, !finishedInserting else {
                    if isPersisted {
                        /// Add the specified page as an existing page, since this is an index based on persisted data.
                        newOrderedPages.append(.existing(pageID))
                    } else {
                        /// Add the specified page as is since we are working off a transient index, and would lose the fact that it may have been recently added otherwise.
                        newOrderedPages.append(pageInfo)
                    }
                    continue
                }
                
                /// If this is our first time reaching this point, we have some new blocks to insert.
                if newPageBlocks.isEmpty {
                    if let insertAfter = insertionCursor.insertAfter {
                        /// Make sure nothing changed with our cursor, since we'll be using the cached data inside of it.
                        guard insertAfter.page.id.page == pageID else {
                            throw DatastoreInterfaceError.staleCursor
                        }
                        
                        let existingPage = insertAfter.page
                        
                        /// Split the existing page into two halves.
                        let existingPageBlocks = try await Array(existingPage.blocks)
                        let firstHalf = existingPageBlocks[...insertAfter.blockIndex]
                        let remainingBlocks = existingPageBlocks[(insertAfter.blockIndex+1)...]
                        
                        /// Calculate the remaining space, and generate blocks for the new entry.
                        var remainingSpace = actualPageSize - firstHalf.encodedSize
                        let entryBlocks = entry.blocks(remainingPageSpace: remainingSpace, maxPageSpace: actualPageSize)
                        
                        /// Check if the first page has enough room for the block being added.
                        if entryBlocks[0].encodedSize <= remainingSpace {
                            /// Add the first half of the existing entries to the pages we'll create
                            newPageBlocks.append(Array(firstHalf))
                            
                            /// Import the new blocks, appending the fist page in the process
                            var insertionIndex = 0
                            for block in entryBlocks {
                                defer { insertionIndex += 1 }
                                if insertionIndex == newPageBlocks.count {
                                    newPageBlocks.append([])
                                }
                                newPageBlocks[insertionIndex].append(block)
                            }
                            
                            /// If the index had data on disk, or it existed prior to the transaction, mark it as removed.
                            /// Otherwise, simply skip the page, since we added it in a transient index.
                            removedPages.insert(existingPage)
                            if isPersisted || pageInfo.isExisting {
                                newOrderedPages.append(.removed(pageID))
                            }
                            
                            /// Calculate how much room is left on the last page we are making, so we can finish importing the remaining blocks form the original page, creating new pages if necessary.
                            remainingSpace = actualPageSize - newPageBlocks[newPageBlocks.count-1].encodedSize
                            for block in remainingBlocks {
                                if block.encodedSize > remainingSpace {
                                    newPageBlocks.append([])
                                    remainingSpace = actualPageSize
                                }
                                
                                // TODO: Also chop/re-combine blocks to efficiently pack them here
                                newPageBlocks[newPageBlocks.count-1].append(block)
                                remainingSpace -= block.encodedSize
                            }
                        } else {
                            /// If if doesn't, leave the page as is, and insert the rest of the new blocks as pages following this one.
                            if isPersisted {
                                /// Add the specified page as an existing page, since this is an index based on persisted data.
                                newOrderedPages.append(.existing(pageID))
                            } else {
                                /// Add the specified page as is since we are working off a transient index, and would lose the fact that it may have been recently added otherwise.
                                newOrderedPages.append(pageInfo)
                            }
                            newPageBlocks = entryBlocks.map { [$0] }
                        }
                    } else {
                        /// First, separate the new entry into the necessary amount of pages it'll need.
                        newPageBlocks = entry.blocks(
                            remainingPageSpace: actualPageSize,
                            maxPageSpace: actualPageSize
                        ).map { [$0] }
                        
                        /// Attempt to collate this page since we are inserting before it.
                        attemptPageCollation = true
                    }
                } else {
                    /// We finished processing the insert by now, so attempt to collate the next page if we can.
                    attemptPageCollation = true
                }
                
                var importCurrentPage = false
                /// We finished inserting, so attempt to collate either the current page or the next one if there is space for it, then stop.
                if attemptPageCollation {
                    /// Load the first page to see how large it is compared to the amount of space we have left on our final new page
                    let existingPage = await datastore.page(for: .init(index: id, page: pageID))
                    let existingPageBlocks = try await Array(existingPage.blocks)
                    
                    /// Calculate how much space remains on the final new page, and insert the existing blocks if they all fit.
                    /// Note that we are guaranteed to have at least one new page by this point, since we are inserting and not replacing.
                    let remainingSpace = actualPageSize - newPageBlocks[newPageBlocks.count - 1].encodedSize
                    if existingPageBlocks.encodedSize <= remainingSpace {
                        /// If the index had data on disk, or it existed prior to the transaction, mark it as removed.
                        /// Otherwise, simply skip the page, since we added it in a transient index.
                        removedPages.insert(existingPage)
                        if isPersisted || pageInfo.isExisting {
                            newOrderedPages.append(.removed(pageID))
                        }
                        
                        /// Import the blocks into the last page.
                        newPageBlocks[newPageBlocks.count - 1].append(contentsOf: existingPageBlocks)
                    } else {
                        /// We didn't have enough room for the whole thing, so just use the page as is.
                        importCurrentPage = true
                    }
                    
                    /// Now that we've checked to see if this page fits, stop generating new pages
                    finishedInserting = true
                }
                
                /// Finally, insert the pages we've created if we are done, or if we don't have a next iteration.
                if finishedInserting || index == originalOrderedPages.count - 1 {
                    /// Get a pre-sorted list of IDs for our sanity when looking up pages
                    let newPageIDs = newPageBlocks.map { _ in
                        DatastorePageIdentifier()
                    }.sorted()
                    /// Create the page objects for the newly-segmented blocks.
                    for (newPageID, pageBlocks) in zip(newPageIDs, newPageBlocks) {
                        assert(pageBlocks.encodedSize <= actualPageSize)
                        let page = DiskPersistence.Datastore.Page(
                            datastore: datastore,
                            id: .init(index: newIndexID, page: newPageID),
                            blocks: pageBlocks
                        )
                        createdPages.insert(page)
                        newOrderedPages.append(.added(newPageID))
                    }
                    
                    /// Conditionally import the current page after we inserted everything.
                    if importCurrentPage {
                        if isPersisted {
                            /// Add the specified page as an existing page, since this is an index based on persisted data.
                            newOrderedPages.append(.existing(pageID))
                        } else {
                            /// Add the specified page as is since we are working off a transient index, and would lose the fact that it may have been recently added otherwise.
                            newOrderedPages.append(pageInfo)
                        }
                    }
                }
            }
        }
        
        if newPageBlocks.isEmpty {
            /// If we got here, we never had a chance to insert anything above, so just do it here.
            /// (Probably should find a way to combine this with the above, but works for now 😅)
            guard insertionCursor.insertAfter == nil else { throw DatastoreInterfaceError.staleCursor }
            
            newPageBlocks = entry.blocks(
                remainingPageSpace: actualPageSize,
                maxPageSpace: actualPageSize
            ).map { [$0] }
            
            /// Get a pre-sorted list of IDs for our sanity when looking up pages
            let newPageIDs = newPageBlocks.map { _ in
                DatastorePageIdentifier()
            }.sorted()
            /// Create the page objects for the newly-segmented blocks.
            for (newPageID, pageBlocks) in zip(newPageIDs, newPageBlocks) {
                assert(pageBlocks.encodedSize <= actualPageSize)
                let page = DiskPersistence.Datastore.Page(
                    datastore: datastore,
                    id: .init(index: newIndexID, page: newPageID),
                    blocks: pageBlocks
                )
                createdPages.insert(page)
                newOrderedPages.append(.added(newPageID))
            }
        }
        
        if !createdPages.isEmpty {
            manifest.id = newIndexID.manifestID
            manifest.orderedPages = newOrderedPages
        }
        
        return (manifest: manifest, createdPages: createdPages, removedPages: removedPages)
    }
    
    func manifest(
        replacing entry: DatastorePageEntry,
        at instanceCursor: DiskPersistence.InstanceCursor,
        targetPageSize: Int = 4*1024
    ) async throws -> (
        manifest: DatastoreIndexManifest,
        createdPages: Set<DiskPersistence.Datastore.Page>,
        removedPages: Set<DiskPersistence.Datastore.Page>
    ) {
        guard
            instanceCursor.datastore === datastore,
            instanceCursor.index === self,
            let firstInstanceBlock = instanceCursor.blocks.first,
            let lastInstanceBlock = instanceCursor.blocks.last,
            firstInstanceBlock.pageIndex <= lastInstanceBlock.pageIndex,
            firstInstanceBlock.pageIndex != lastInstanceBlock.pageIndex || firstInstanceBlock.blockIndex <= lastInstanceBlock.blockIndex
        else { throw DatastoreInterfaceError.staleCursor }
        
        let actualPageSize = max(targetPageSize, 4*1024) - DiskPersistence.Datastore.Page.headerSize
        
        var manifest = try await manifest
        
        let newIndexID = id.with(manifestID: DatastoreIndexManifestIdentifier())
        var createdPages: Set<DiskPersistence.Datastore.Page> = []
        var removedPages: Set<DiskPersistence.Datastore.Page> = []
        
        var newOrderedPages: [DatastoreIndexManifest.PageInfo] = []
        var newPageBlocks: [[DatastorePageEntryBlock]] = []
        var finishedInserting = false
        var existingEntryBlocks: [DatastorePageEntryBlock] = []
        
        let originalOrderedPages = manifest.orderedPages
        for (index, pageInfo) in originalOrderedPages.enumerated() {
            switch pageInfo {
            case .removed:
                /// Skip previously removed entries, unless this index is based on a transient index, and the removed entry was from before the transaction began.
                if !isPersisted {
                    newOrderedPages.append(pageInfo)
                }
                continue
            case .existing(let pageID), .added(let pageID):
                /// If we are processing an earlier page or are finished inserting, just include it as an existing page.
                guard index >= firstInstanceBlock.pageIndex, !finishedInserting else {
                    if isPersisted {
                        /// Add the specified page as an existing page, since this is an index based on persisted data.
                        newOrderedPages.append(.existing(pageID))
                    } else {
                        /// Add the specified page as is since we are working off a transient index, and would lose the fact that it may have been recently added otherwise.
                        newOrderedPages.append(pageInfo)
                    }
                    continue
                }
                
                var importCurrentPage = false
                
                /// If this is our first time reaching this point, we have some new blocks to insert.
                if index <= lastInstanceBlock.pageIndex {
                    let existingPage = await datastore.page(for: .init(index: self.id, page: pageID))
                    let existingPageBlocks = try await Array(existingPage.blocks)
                    
                    /// Grab the index range that we are replacing on this page
                    let startingIndex = index == firstInstanceBlock.pageIndex ? firstInstanceBlock.blockIndex : 0
                    let endingIndex = index == lastInstanceBlock.pageIndex ? lastInstanceBlock.blockIndex : (existingPageBlocks.count-1)
                    
                    /// Split the existing page into three parts: what comes before what we are replacing, what comes after, and the middle.
                    let firstHalf = existingPageBlocks[..<startingIndex]
                    let middleHalf = existingPageBlocks[startingIndex...endingIndex]
                    let lastHalf = existingPageBlocks[(endingIndex+1)...]
                    
                    /// Make sure that the data is not going to be corrupted by our replacement.
                    guard
                        index == firstInstanceBlock.pageIndex || firstHalf.isEmpty,
                        index == lastInstanceBlock.pageIndex || lastHalf.isEmpty
                    else { throw DatastoreInterfaceError.staleCursor }
                    
                    /// Save the data we are replacing so we can see if it changed before ultimately writing to disk.
                    // TODO: Could we move this to be the Datastore's responsibility?
                    existingEntryBlocks.append(contentsOf: middleHalf)
                    
                    /// If the index had data on disk, or it existed prior to the transaction, mark it as removed.
                    /// Otherwise, simply skip the page, since we added it in a transient index.
                    removedPages.insert(existingPage)
                    if isPersisted || pageInfo.isExisting {
                        newOrderedPages.append(.removed(pageID))
                    }
                    
                    /// If we are at the start of the range, go ahead and prep the entry for insertion.
                    if index == firstInstanceBlock.pageIndex {
                        /// Calculate the remaining space, and generate blocks for the new entry.
                        let remainingSpace = actualPageSize - firstHalf.encodedSize
                        let newEntryBlocks = entry.blocks(remainingPageSpace: remainingSpace, maxPageSpace: actualPageSize)
                        
                        /// Add the first half of the existing entries to the pages we'll create, since it will always change in some way.
                        newPageBlocks.append(Array(firstHalf))
                        
                        /// Import the new blocks, appending the fist page only if there is room there.
                        var insertionIndex = 0
                        for block in newEntryBlocks {
                            defer { insertionIndex += 1 }
                            if insertionIndex == 0 && block.encodedSize > remainingSpace {
                                insertionIndex += 1
                            }
                            if insertionIndex == newPageBlocks.count {
                                newPageBlocks.append([])
                            }
                            newPageBlocks[insertionIndex].append(block)
                        }
                    }
                    
                    /// If we are at the end of the range, collate the end of the page to what we are adding.
                    if index == lastInstanceBlock.pageIndex  {
                        /// First, make sure the new entry is made of different data than the existing one, otherwise we are wasting our time writing it to disk.
                        let existingEntryBytes = existingEntryBlocks.reduce(into: Bytes()) { $0.append(contentsOf: $1.contents) }
                        let existingEntry = try DatastorePageEntry(bytes: existingEntryBytes, isPartial: false)
                        guard existingEntry != entry else { return (manifest, [], []) }
                        
                        /// Calculate how much room is left on the last page we are making, so we can finish importing the remaining blocks form the original page, creating new pages if necessary.
                        var remainingSpace = actualPageSize - newPageBlocks[newPageBlocks.count-1].encodedSize
                        for block in lastHalf {
                            if block.encodedSize > remainingSpace {
                                newPageBlocks.append([])
                                remainingSpace = actualPageSize
                            }
                            
                            // TODO: Also chop/re-combine blocks to efficiently pack them here
                            newPageBlocks[newPageBlocks.count-1].append(block)
                            remainingSpace -= block.encodedSize
                        }
                    }
                } else {
                    /// We finished processing the insert by now, so attempt to collate the next page if we can.
                    
                    /// Load the first page to see how large it is compared to the amount of space we have left on our final new page
                    let existingPage = await datastore.page(for: .init(index: id, page: pageID))
                    let existingPageBlocks = try await Array(existingPage.blocks)
                    
                    /// Calculate how much space remains on the final new page, and insert the existing blocks if they all fit.
                    /// Note that we are guaranteed to have at least one new page by this point, since we are inserting and not replacing.
                    let remainingSpace = actualPageSize - newPageBlocks[newPageBlocks.count - 1].encodedSize
                    if existingPageBlocks.encodedSize <= remainingSpace {
                        /// If the index had data on disk, or it existed prior to the transaction, mark it as removed.
                        /// Otherwise, simply skip the page, since we added it in a transient index.
                        removedPages.insert(existingPage)
                        if isPersisted || pageInfo.isExisting {
                            newOrderedPages.append(.removed(pageID))
                        }
                        
                        /// Import the blocks into the last page.
                        newPageBlocks[newPageBlocks.count - 1].append(contentsOf: existingPageBlocks)
                    } else {
                        /// We didn't have enough room for the whole thing, so just use the page as is.
                        importCurrentPage = true
                    }
                    
                    /// Now that we've checked to see if this page fits, stop generating new pages
                    finishedInserting = true
                }
                
                /// Finally, insert the pages we've created if we are done, or if we don't have a next iteration.
                if finishedInserting || index == originalOrderedPages.count - 1 {
                    /// Get a pre-sorted list of IDs for our sanity when looking up pages
                    let newPageIDs = newPageBlocks.map { _ in
                        DatastorePageIdentifier()
                    }.sorted()
                    /// Create the page objects for the newly-segmented blocks.
                    for (newPageID, pageBlocks) in zip(newPageIDs, newPageBlocks) {
                        assert(pageBlocks.encodedSize <= actualPageSize)
                        let page = DiskPersistence.Datastore.Page(
                            datastore: datastore,
                            id: .init(index: newIndexID, page: newPageID),
                            blocks: pageBlocks
                        )
                        createdPages.insert(page)
                        newOrderedPages.append(.added(newPageID))
                    }
                    
                    /// Conditionally import the current page after we inserted everything.
                    if importCurrentPage {
                        if isPersisted {
                            /// Add the specified page as an existing page, since this is an index based on persisted data.
                            newOrderedPages.append(.existing(pageID))
                        } else {
                            /// Add the specified page as is since we are working off a transient index, and would lose the fact that it may have been recently added otherwise.
                            newOrderedPages.append(pageInfo)
                        }
                    }
                }
            }
        }
        
        if !createdPages.isEmpty {
            manifest.id = newIndexID.manifestID
            manifest.orderedPages = newOrderedPages
        }
        
        return (manifest: manifest, createdPages: createdPages, removedPages: removedPages)
    }
    
    func manifest(
        deletingEntryAt instanceCursor: DiskPersistence.InstanceCursor,
        targetPageSize: Int = 4*1024
    ) async throws -> (
        manifest: DatastoreIndexManifest,
        createdPages: Set<DiskPersistence.Datastore.Page>,
        removedPages: Set<DiskPersistence.Datastore.Page>
    ) {
        guard
            instanceCursor.datastore === datastore,
            instanceCursor.index === self,
            let firstInstanceBlock = instanceCursor.blocks.first,
            let lastInstanceBlock = instanceCursor.blocks.last,
            firstInstanceBlock.pageIndex <= lastInstanceBlock.pageIndex,
            firstInstanceBlock.pageIndex != lastInstanceBlock.pageIndex || firstInstanceBlock.blockIndex <= lastInstanceBlock.blockIndex
        else { throw DatastoreInterfaceError.staleCursor }
        
        let actualPageSize = max(targetPageSize, 4*1024) - DiskPersistence.Datastore.Page.headerSize
        
        var manifest = try await manifest
        
        let newIndexID = id.with(manifestID: DatastoreIndexManifestIdentifier())
        var createdPages: Set<DiskPersistence.Datastore.Page> = []
        var removedPages: Set<DiskPersistence.Datastore.Page> = []
        
        var newOrderedPages: [DatastoreIndexManifest.PageInfo] = []
        var newPageBlocks: [[DatastorePageEntryBlock]] = []
        var finishedInserting = false
        
        let originalOrderedPages = manifest.orderedPages
        for (index, pageInfo) in originalOrderedPages.enumerated() {
            switch pageInfo {
            case .removed:
                /// Skip previously removed entries, unless this index is based on a transient index, and the removed entry was from before the transaction began.
                if !isPersisted {
                    newOrderedPages.append(pageInfo)
                }
                continue
            case .existing(let pageID), .added(let pageID):
                /// If we are processing an earlier page or are finished inserting, just include it as an existing page.
                guard index >= firstInstanceBlock.pageIndex, !finishedInserting else {
                    if isPersisted {
                        /// Add the specified page as an existing page, since this is an index based on persisted data.
                        newOrderedPages.append(.existing(pageID))
                    } else {
                        /// Add the specified page as is since we are working off a transient index, and would lose the fact that it may have been recently added otherwise.
                        newOrderedPages.append(pageInfo)
                    }
                    continue
                }
                
                var importCurrentPage = false
                
                /// If this is our first time reaching this point, we have some new blocks to insert.
                if index <= lastInstanceBlock.pageIndex {
                    let existingPage = await datastore.page(for: .init(index: self.id, page: pageID))
                    let existingPageBlocks = try await Array(existingPage.blocks)
                    
                    /// Grab the index range that we are replacing on this page
                    let startingIndex = index == firstInstanceBlock.pageIndex ? firstInstanceBlock.blockIndex : 0
                    let endingIndex = index == lastInstanceBlock.pageIndex ? lastInstanceBlock.blockIndex : (existingPageBlocks.count-1)
                    
                    /// Split the existing page into three parts: what comes before what we are replacing, what comes after, and the middle.
                    let firstHalf = existingPageBlocks[..<startingIndex]
                    let lastHalf = existingPageBlocks[(endingIndex+1)...]
                    
                    /// Make sure that the data is not going to be corrupted by our replacement.
                    guard
                        index == firstInstanceBlock.pageIndex || firstHalf.isEmpty,
                        index == lastInstanceBlock.pageIndex || lastHalf.isEmpty
                    else { throw DatastoreInterfaceError.staleCursor }
                    
                    /// If the index had data on disk, or it existed prior to the transaction, mark it as removed.
                    /// Otherwise, simply skip the page, since we added it in a transient index.
                    removedPages.insert(existingPage)
                    if isPersisted || pageInfo.isExisting {
                        newOrderedPages.append(.removed(pageID))
                    }
                    
                    /// If we are at the start of the range, go ahead and prep the entry for insertion.
                    if index == firstInstanceBlock.pageIndex {
                        /// Add the first half of the existing entries to the pages we'll create, since it will always change in some way.
                        newPageBlocks.append(Array(firstHalf))
                    }
                    
                    /// If we are at the end of the range, collate the end of the page to what we are adding.
                    if index == lastInstanceBlock.pageIndex  {
                        /// Calculate how much room is left on the last page we are making, so we can finish importing the remaining blocks form the original page, creating new pages if necessary.
                        var remainingSpace = actualPageSize - newPageBlocks[newPageBlocks.count-1].encodedSize
                        for block in lastHalf {
                            if block.encodedSize > remainingSpace {
                                newPageBlocks.append([])
                                remainingSpace = actualPageSize
                            }
                            
                            // TODO: Also chop/re-combine blocks to efficiently pack them here
                            newPageBlocks[newPageBlocks.count-1].append(block)
                            remainingSpace -= block.encodedSize
                        }
                    }
                } else {
                    /// We finished processing the insert by now, so attempt to collate the next page if we can.
                    
                    /// Load the first page to see how large it is compared to the amount of space we have left on our final new page
                    let existingPage = await datastore.page(for: .init(index: id, page: pageID))
                    let existingPageBlocks = try await Array(existingPage.blocks)
                    
                    /// Calculate how much space remains on the final new page, and insert the existing blocks if they all fit.
                    /// Note that we are guaranteed to have at least one new page by this point, since we are inserting and not replacing.
                    let remainingSpace = actualPageSize - newPageBlocks[newPageBlocks.count - 1].encodedSize
                    if existingPageBlocks.encodedSize <= remainingSpace {
                        /// If the index had data on disk, or it existed prior to the transaction, mark it as removed.
                        /// Otherwise, simply skip the page, since we added it in a transient index.
                        removedPages.insert(existingPage)
                        if isPersisted || pageInfo.isExisting {
                            newOrderedPages.append(.removed(pageID))
                        }
                        
                        /// Import the blocks into the last page.
                        newPageBlocks[newPageBlocks.count - 1].append(contentsOf: existingPageBlocks)
                    } else {
                        /// We didn't have enough room for the whole thing, so just use the page as is.
                        importCurrentPage = true
                    }
                    
                    /// Now that we've checked to see if this page fits, stop generating new pages
                    finishedInserting = true
                }
                
                /// Finally, insert the pages we've created if we are done, or if we don't have a next iteration.
                if finishedInserting || index == originalOrderedPages.count - 1 {
                    /// Get a pre-sorted list of IDs for our sanity when looking up pages
                    let newPageIDs = newPageBlocks.map { _ in
                        DatastorePageIdentifier()
                    }.sorted()
                    /// Create the page objects for the newly-segmented blocks.
                    for (newPageID, pageBlocks) in zip(newPageIDs, newPageBlocks) {
                        assert(pageBlocks.encodedSize <= actualPageSize)
                        let page = DiskPersistence.Datastore.Page(
                            datastore: datastore,
                            id: .init(index: newIndexID, page: newPageID),
                            blocks: pageBlocks
                        )
                        createdPages.insert(page)
                        newOrderedPages.append(.added(newPageID))
                    }
                    
                    /// Conditionally import the current page after we inserted everything.
                    if importCurrentPage {
                        if isPersisted {
                            /// Add the specified page as an existing page, since this is an index based on persisted data.
                            newOrderedPages.append(.existing(pageID))
                        } else {
                            /// Add the specified page as is since we are working off a transient index, and would lose the fact that it may have been recently added otherwise.
                            newOrderedPages.append(pageInfo)
                        }
                    }
                }
            }
        }
        
        if !createdPages.isEmpty || !removedPages.isEmpty {
            manifest.id = newIndexID.manifestID
            manifest.orderedPages = newOrderedPages
        }
        
        return (manifest: manifest, createdPages: createdPages, removedPages: removedPages)
    }
    
    func manifestDeletingAllEntries() async throws -> (
        manifest: DatastoreIndexManifest,
        removedPages: Set<DiskPersistence.Datastore.Page>
    ) {
        var manifest = try await manifest
        
        let newIndexID = id.with(manifestID: DatastoreIndexManifestIdentifier())
        var removedPages: Set<DiskPersistence.Datastore.Page> = []
        
        let originalOrderedPages = manifest.orderedPages
        var newOrderedPages: [DatastoreIndexManifest.PageInfo] = []
        newOrderedPages.reserveCapacity(originalOrderedPages.count)
        
        for pageInfo in originalOrderedPages {
            switch pageInfo {
            case .removed:
                /// Skip previously removed entries, unless this index is based on a transient index, and the removed entry was from before the transaction began.
                if !isPersisted {
                    newOrderedPages.append(pageInfo)
                }
                continue
            case .existing(let pageID), .added(let pageID):
                let existingPage = await datastore.page(for: .init(index: self.id, page: pageID))
                /// If the index had data on disk, or it existed prior to the transaction, mark it as removed.
                /// Otherwise, simply skip the page, since we added it in a transient index.
                removedPages.insert(existingPage)
                if isPersisted || pageInfo.isExisting {
                    newOrderedPages.append(.removed(pageID))
                }
            }
        }
        
        if !removedPages.isEmpty {
            manifest.id = newIndexID.manifestID
            manifest.orderedPages = newOrderedPages
        }
        
        return (manifest: manifest, removedPages: removedPages)
    }
}
