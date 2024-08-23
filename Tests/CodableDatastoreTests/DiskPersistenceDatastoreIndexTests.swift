//
//  DiskPersistenceDatastoreIndexTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-04.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

#if !canImport(Darwin)
@preconcurrency import Foundation
#endif
import XCTest
@testable import CodableDatastore

fileprivate struct SortError: Error, Equatable {}

final class DiskPersistenceDatastoreIndexTests: XCTestCase {
    var temporaryStoreURL: URL = FileManager.default.temporaryDirectory
    
    override func setUp() async throws {
        temporaryStoreURL = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString, isDirectory: true);
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: temporaryStoreURL)
    }
    
    func assertPageSearch(
        proposedEntry: UInt8,
        pages: [[DatastorePageEntryBlock]],
        requiredContentLength: Int? = nil,
        requiresCompleteEntries: Bool = false,
        expectedIndex: Int?,
        expectedSearchFailure: Bool = false,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let persistence = DiskPersistence(readOnlyURL: temporaryStoreURL)
        let snapshot = Snapshot(
            id: .init(rawValue: "Snapshot"),
            persistence: persistence
        )
        let datastore = DiskPersistence.Datastore(
            id: .init(rawValue: "Datastore"),
            snapshot: snapshot
        )
        let index = DiskPersistence.Datastore.Index(
            datastore: datastore,
            id: .primary(manifest: .init(rawValue: "Index")),
            manifest: DatastoreIndexManifest(
                id: .init(rawValue: "Index"),
                orderedPages: pages.enumerated().map { (index, _) in
                        .existing(.init(rawValue: "Page \(index)"))
                }
            )
        )
        
        var pageLookup: [DatastorePageIdentifier : DiskPersistence<ReadOnly>.Datastore.Page] = [:]
        var pageInfos: [DatastoreIndexManifest.PageInfo] = []
        
        for (index, blocks) in pages.enumerated() {
            let pageID = DatastorePageIdentifier(rawValue: "Page \(index)")
            let page = DiskPersistence<ReadOnly>.Datastore.Page(
                datastore: datastore,
                id: .init(
                    index: .primary(manifest: .init(rawValue: "Index")),
                    page: pageID
                ),
                blocks: blocks
            )
            pageLookup[pageID] = page
            pageInfos.append(.existing(pageID))
        }
        
        do {
            let result = try await index.pageIndex(
                for: proposedEntry,
                in: pageInfos,
                requiresCompleteEntries: requiresCompleteEntries
            ) { [pageLookup] pageID in
                pageLookup[pageID]!
            } comparator: { lhs, rhs in
                if let requiredContentLength, rhs.content.count != requiredContentLength {
                    throw SortError()
                }
                return lhs.sortOrder(comparedTo: rhs.headers[0][0])
            }
            
            XCTAssertEqual(result, expectedIndex, file: file, line: line)
            XCTAssertFalse(expectedSearchFailure, file: file, line: line)
        } catch is SortError {
            XCTAssertTrue(expectedSearchFailure, "Encountered unexpected error", file: file, line: line)
        }
    }
    
    func assertInsertionCursor(
        proposedEntry: UInt8,
        pages: [[DatastorePageEntryBlock]],
        pageIndex: Int?,
        blockIndex: Int?,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let persistence = DiskPersistence(readOnlyURL: temporaryStoreURL)
        let snapshot = Snapshot(
            id: .init(rawValue: "Snapshot"),
            persistence: persistence
        )
        let datastore = DiskPersistence.Datastore(
            id: .init(rawValue: "Datastore"),
            snapshot: snapshot
        )
        let index = DiskPersistence.Datastore.Index(
            datastore: datastore,
            id: .primary(manifest: .init(rawValue: "Index")),
            manifest: DatastoreIndexManifest(
                id: .init(rawValue: "Index"),
                orderedPages: pages.enumerated().map { (index, _) in
                        .existing(.init(rawValue: "Page \(index)"))
                }
            )
        )
        
        var pageLookup: [DatastorePageIdentifier : DiskPersistence<ReadOnly>.Datastore.Page] = [:]
        var pageInfos: [DatastoreIndexManifest.PageInfo] = []
        
        for (index, blocks) in pages.enumerated() {
            let pageID = DatastorePageIdentifier(rawValue: "Page \(index)")
            let page = DiskPersistence<ReadOnly>.Datastore.Page(
                datastore: datastore,
                id: .init(
                    index: .primary(manifest: .init(rawValue: "Index")),
                    page: pageID
                ),
                blocks: blocks
            )
            pageLookup[pageID] = page
            pageInfos.append(.existing(pageID))
        }
        
        let result = try await index.insertionCursor(for: RangeBoundExpression.including(proposedEntry), in: pageInfos, requiresCompleteEntries: false) { [pageLookup] pageID in
            pageLookup[pageID]!
        } comparator: { lhs, rhs in
            lhs.sortOrder(comparedTo: rhs.headers[0][0], order: .ascending)
        }
        
        XCTAssertTrue(result.persistence === persistence, file: file, line: line)
        XCTAssertTrue(result.datastore === datastore, file: file, line: line)
        XCTAssertEqual(result.index, index, file: file, line: line)
        XCTAssertEqual(result.insertAfter?.pageIndex, pageIndex, file: file, line: line)
        let pageID = await result.insertAfter?.page.id.page
        XCTAssertEqual(pageID, pageIndex.map({ DatastorePageIdentifier(rawValue: "Page \($0)") }), file: file, line: line)
        XCTAssertEqual(result.insertAfter?.blockIndex, blockIndex, file: file, line: line)
    }
    
    func testEmptyPagesSearch() async throws {
        try await assertPageSearch(proposedEntry: 0, pages: [], expectedIndex: nil)
        try await assertPageSearch(proposedEntry: 0, pages: [], expectedIndex: nil)
    }
    
    func testSinglePageNoBlocksSearch() async throws {
        try await assertPageSearch(proposedEntry: 0, pages: [[]], expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: [[]], expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: [[]], expectedIndex: 0)
    }
    
    func testSinglePageSingleBlockSearch() async throws {
        let pages = [DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
    }
    
    func testSinglePageTwoBlockSearch() async throws {
        let pages = [DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 6, maxPageSpace: 1024)]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
    }
    
    func testSinglePageMultipleBlockSearch() async throws {
        let pages = [DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 6, maxPageSpace: 6)]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
    }
    
    func testTwoPageSingleBlockEachSearch() async throws {
        let pages = [
            DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024),
            DatastorePageEntry(headers: [[3]], content: [3]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024),
        ]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 3, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 4, pages: pages, expectedIndex: 1)
    }
    
    func testTwoPageSharedSingleBlockSearch() async throws {
        let pages = DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 7, maxPageSpace: 1024).map { [$0] }
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
    }
    
    func testTwoPageForwardsBleedingBlockSearch() async throws {
        let entry1 = DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 7, maxPageSpace: 1024)
        let entry3 = DatastorePageEntry(headers: [[3]], content: [3]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
        
        let pages = [
            [entry1[0]],
            [entry1[1], entry3[0]]
        ]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 3, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 4, pages: pages, expectedIndex: 1)
    }
    
    func testSplitContentBlockSearch() async throws {
        let entry1 = DatastorePageEntry(headers: [[1]], content: Array(repeating: 1, count: 100)).blocks(remainingPageSpace: 20, maxPageSpace: 1024)
        let entry3 = DatastorePageEntry(headers: [[3]], content: Array(repeating: 3, count: 100)).blocks(remainingPageSpace: 20, maxPageSpace: 1024)
        
        let pages = [
            [entry1[0]],
            [entry1[1], entry3[0]],
            [entry3[1]]
        ]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, requiredContentLength: 100, requiresCompleteEntries: false, expectedIndex: 0, expectedSearchFailure: true)
        try await assertPageSearch(proposedEntry: 1, pages: pages, requiredContentLength: 100, requiresCompleteEntries: false, expectedIndex: 0, expectedSearchFailure: true)
        try await assertPageSearch(proposedEntry: 2, pages: pages, requiredContentLength: 100, requiresCompleteEntries: false, expectedIndex: 0, expectedSearchFailure: true)
        try await assertPageSearch(proposedEntry: 3, pages: pages, requiredContentLength: 100, requiresCompleteEntries: false, expectedIndex: 1, expectedSearchFailure: true)
        try await assertPageSearch(proposedEntry: 4, pages: pages, requiredContentLength: 100, requiresCompleteEntries: false, expectedIndex: 1, expectedSearchFailure: true)
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, requiredContentLength: 100, requiresCompleteEntries: true, expectedIndex: 0, expectedSearchFailure: false)
        try await assertPageSearch(proposedEntry: 1, pages: pages, requiredContentLength: 100, requiresCompleteEntries: true, expectedIndex: 0, expectedSearchFailure: false)
        try await assertPageSearch(proposedEntry: 2, pages: pages, requiredContentLength: 100, requiresCompleteEntries: true, expectedIndex: 0, expectedSearchFailure: false)
        try await assertPageSearch(proposedEntry: 3, pages: pages, requiredContentLength: 100, requiresCompleteEntries: true, expectedIndex: 1, expectedSearchFailure: false)
        try await assertPageSearch(proposedEntry: 4, pages: pages, requiredContentLength: 100, requiresCompleteEntries: true, expectedIndex: 1, expectedSearchFailure: false)
    }
    
    func testTwoPageBackwardsBleedingBlockSearch() async throws {
        let entry1 = DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
        let entry3 = DatastorePageEntry(headers: [[3]], content: [3]).blocks(remainingPageSpace: 7, maxPageSpace: 1024)
        
        let pages = [
            [entry1[0], entry3[0]],
            [entry3[1]]
        ]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 3, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 4, pages: pages, expectedIndex: 0)
    }
    
    func testTwoPageThreeEntryBlockSearch() async throws {
        let entry1 = DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
        let entry3 = DatastorePageEntry(headers: [[3]], content: [3]).blocks(remainingPageSpace: 7, maxPageSpace: 1024)
        let entry5 = DatastorePageEntry(headers: [[5]], content: [5]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
        
        let pages = [
            [entry1[0], entry3[0]],
            [entry3[1], entry5[0]]
        ]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 3, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 4, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 5, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 6, pages: pages, expectedIndex: 1)
    }
    
    func testTwoPageMultipleBlockSearch() async throws {
        let entry1 = DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 6, maxPageSpace: 1024)
        let entry3 = DatastorePageEntry(headers: [[3]], content: Array(repeating: 3, count: 100)).blocks(remainingPageSpace: 20, maxPageSpace: 1024)
        let entry5 = DatastorePageEntry(headers: [[5]], content: [5]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
        let entry7 = DatastorePageEntry(headers: [[7]], content: [7]).blocks(remainingPageSpace: 6, maxPageSpace: 1024)
        
        let pages = [
            [entry1[0], entry1[1], entry3[0]],
            [entry3[1], entry5[0], entry7[0], entry7[1]]
        ]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 3, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 4, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 5, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 6, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 7, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 8, pages: pages, expectedIndex: 1)
    }
    
    func testThreePageSingleBlockEachSearch() async throws {
        let pages = [
            DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024),
            DatastorePageEntry(headers: [[3]], content: [3]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024),
            DatastorePageEntry(headers: [[5]], content: [5]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024),
        ]
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 3, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 4, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 5, pages: pages, expectedIndex: 2)
        try await assertPageSearch(proposedEntry: 6, pages: pages, expectedIndex: 2)
    }
    
    func testFourteenPageSingleBlockEachSearch() async throws {
        let pages = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27].map {
            DatastorePageEntry(headers: [[$0]], content: [$0]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
        }
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 3, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 4, pages: pages, expectedIndex: 1)
        try await assertPageSearch(proposedEntry: 5, pages: pages, expectedIndex: 2)
        try await assertPageSearch(proposedEntry: 6, pages: pages, expectedIndex: 2)
        try await assertPageSearch(proposedEntry: 7, pages: pages, expectedIndex: 3)
        try await assertPageSearch(proposedEntry: 8, pages: pages, expectedIndex: 3)
        try await assertPageSearch(proposedEntry: 9, pages: pages, expectedIndex: 4)
        try await assertPageSearch(proposedEntry: 10, pages: pages, expectedIndex: 4)
        try await assertPageSearch(proposedEntry: 11, pages: pages, expectedIndex: 5)
        try await assertPageSearch(proposedEntry: 12, pages: pages, expectedIndex: 5)
        try await assertPageSearch(proposedEntry: 13, pages: pages, expectedIndex: 6)
        try await assertPageSearch(proposedEntry: 14, pages: pages, expectedIndex: 6)
        try await assertPageSearch(proposedEntry: 15, pages: pages, expectedIndex: 7)
        try await assertPageSearch(proposedEntry: 16, pages: pages, expectedIndex: 7)
        try await assertPageSearch(proposedEntry: 17, pages: pages, expectedIndex: 8)
        try await assertPageSearch(proposedEntry: 18, pages: pages, expectedIndex: 8)
        try await assertPageSearch(proposedEntry: 19, pages: pages, expectedIndex: 9)
        try await assertPageSearch(proposedEntry: 20, pages: pages, expectedIndex: 9)
        try await assertPageSearch(proposedEntry: 21, pages: pages, expectedIndex: 10)
        try await assertPageSearch(proposedEntry: 22, pages: pages, expectedIndex: 10)
        try await assertPageSearch(proposedEntry: 23, pages: pages, expectedIndex: 11)
        try await assertPageSearch(proposedEntry: 24, pages: pages, expectedIndex: 11)
        try await assertPageSearch(proposedEntry: 25, pages: pages, expectedIndex: 12)
        try await assertPageSearch(proposedEntry: 26, pages: pages, expectedIndex: 12)
        try await assertPageSearch(proposedEntry: 27, pages: pages, expectedIndex: 13)
        try await assertPageSearch(proposedEntry: 28, pages: pages, expectedIndex: 13)
    }
    
    func testLongSpanSearch() async throws {
        var pages: [[DatastorePageEntryBlock]] = []
        pages.append(contentsOf: DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 6, maxPageSpace: 1024).map { [$0] })
        pages.append(contentsOf: DatastorePageEntry(headers: [[3]], content: Array(repeating: 3, count: 100)).blocks(remainingPageSpace: 6, maxPageSpace: 20).map { [$0] })
        pages.append(contentsOf: DatastorePageEntry(headers: [[5]], content: [5]).blocks(remainingPageSpace: 6, maxPageSpace: 1024).map { [$0] })
        
        try await assertPageSearch(proposedEntry: 0, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 1, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 2, pages: pages, expectedIndex: 0)
        try await assertPageSearch(proposedEntry: 3, pages: pages, expectedIndex: 2)
        try await assertPageSearch(proposedEntry: 4, pages: pages, expectedIndex: 2)
        try await assertPageSearch(proposedEntry: 5, pages: pages, expectedIndex: 10)
        try await assertPageSearch(proposedEntry: 6, pages: pages, expectedIndex: 10)
    }
    
    func testInMemorySearchSpeed() async throws {
        /// 100,000 pages with 10 entries per page
        let pageBlocks = (UInt64.zero..<100000).map { n in
            (n*10..<(n+1)*10).flatMap { m in
                DatastorePageEntry(headers: [m.bigEndianBytes], content: m.bigEndianBytes).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
            }
        }
        
        let persistence = DiskPersistence(readOnlyURL: temporaryStoreURL)
        let snapshot = Snapshot(
            id: .init(rawValue: "Snapshot"),
            persistence: persistence
        )
        let datastore = DiskPersistence.Datastore(
            id: .init(rawValue: "Datastore"),
            snapshot: snapshot
        )
        let index = DiskPersistence.Datastore.Index(
            datastore: datastore,
            id: .primary(manifest: .init(rawValue: "Index")),
            manifest: DatastoreIndexManifest(
                id: .init(rawValue: "Index"),
                orderedPages: pageBlocks.enumerated().map { (index, _) in
                        .existing(.init(rawValue: "Page \(index)"))
                }
            )
        )
        
        var pageLookup: [DatastorePageIdentifier : DiskPersistence<ReadOnly>.Datastore.Page] = [:]
        var pageInfos: [DatastoreIndexManifest.PageInfo] = []
        
        for (index, blocks) in pageBlocks.enumerated() {
            let pageID = DatastorePageIdentifier(rawValue: "Page \(index)")
            let page = DiskPersistence<ReadOnly>.Datastore.Page(
                datastore: datastore,
                id: .init(
                    index: .primary(manifest: .init(rawValue: "Index")),
                    page: pageID
                ),
                blocks: blocks
            )
            pageLookup[pageID] = page
            pageInfos.append(.existing(pageID))
        }
        
        measure {
            let exp = expectation(description: "Finished")
            Task { [pageInfos, pageLookup] in
                for _ in 0..<1000 {
                    _ = try await index.pageIndex(for: UInt64.random(in: 0..<1000000), in: pageInfos, requiresCompleteEntries: false) { pageID in
                        pageLookup[pageID]!
                    } comparator: { lhs, rhs in
                        lhs.sortOrder(comparedTo: try UInt64(bigEndianBytes: rhs.headers[0]))
                    }
                }
                
                exp.fulfill()
            }
            wait(for: [exp], timeout: 200.0)
        }
    }
    
    func testNonContiguousInsertionCursor() async throws {
        let entry1 = DatastorePageEntry(headers: [[1]], content: [1]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
        let entry2 = DatastorePageEntry(headers: [[2]], content: [2]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
        let entry3 = DatastorePageEntry(headers: [[3]], content: Array(repeating: 3, count: 100)).blocks(remainingPageSpace: 20, maxPageSpace: 60)
        let entry4 = DatastorePageEntry(headers: [[4]], content: [4]).blocks(remainingPageSpace: 1024, maxPageSpace: 1024)
        
        let pages = [
            [entry1[0], entry2[0], entry3[0]],
            [entry3[1]],
            [entry3[2], entry4[0]]
        ]
        
        try await assertInsertionCursor(proposedEntry: 0, pages: pages, pageIndex: nil, blockIndex: nil)
        try await assertInsertionCursor(proposedEntry: 1, pages: pages, pageIndex: nil, blockIndex: nil)
        try await assertInsertionCursor(proposedEntry: 2, pages: pages, pageIndex: 0, blockIndex: 0)
        try await assertInsertionCursor(proposedEntry: 3, pages: pages, pageIndex: 0, blockIndex: 1)
        try await assertInsertionCursor(proposedEntry: 4, pages: pages, pageIndex: 2, blockIndex: 0)
        try await assertInsertionCursor(proposedEntry: 5, pages: pages, pageIndex: 2, blockIndex: 1)
    }
}
