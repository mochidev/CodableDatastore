//
//  MultiplePersistenceTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2025-12-26.
//  Copyright © 2023-25 Mochi Development, Inc. All rights reserved.
//

#if !canImport(Darwin)
@preconcurrency import Foundation
#endif
import XCTest
@testable import CodableDatastore

final class MultiplePersistenceTests: XCTestCase, @unchecked Sendable {
    var temporaryStoreURLOuter: URL = FileManager.default.temporaryDirectory
    var temporaryStoreURLInner: URL = FileManager.default.temporaryDirectory
    
    override func setUp() async throws {
        temporaryStoreURLOuter = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString + "-Outer", isDirectory: true);
        temporaryStoreURLInner = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString + "-Inner", isDirectory: true);
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: temporaryStoreURLOuter)
        try? FileManager.default.removeItem(at: temporaryStoreURLInner)
    }
    
    func testCanWorkWithMultiplePersistences() async throws {
        let outerPersistence = try DiskPersistence(readWriteURL: temporaryStoreURLOuter)
        let innerPersistence = try DiskPersistence(readWriteURL: temporaryStoreURLInner)
        
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: String
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        let outerDatastore = Datastore.JSONStore(
            persistence: outerPersistence,
            format: TestFormat.self,
            migrations: [
                .zero: { try $0.decode(TestFormat.Instance.self, from: $1) }
            ]
        )
        
        let innerDatastore = Datastore.JSONStore(
            persistence: innerPersistence,
            format: TestFormat.self,
            migrations: [
                .zero: { try $0.decode(TestFormat.Instance.self, from: $1) }
            ]
        )
        
        /// Set some default starting values
        try await outerDatastore.persist(.init(id: "A", value: "OuterA"))
        try await outerDatastore.persist(.init(id: "B", value: "OuterB"))
        try await innerDatastore.persist(.init(id: "A", value: "InnerA"))
        try await innerDatastore.persist(.init(id: "B", value: "InnerB"))
        
        /// Start a transaction in the outer persistence, and modify a record.
        try await outerPersistence.perform {
            try await outerDatastore.persist(.init(id: "A", value: "OuterA-New"))
            try await innerPersistence.perform(options: .readOnly) {
                /// Access the inner persistence within a read-only transaction.
                let innerValueA = try await innerDatastore.load(id: "A")
                XCTAssertEqual(innerValueA?.value, "InnerA")
                
                try await Task.detached {
                    /// Attempt to modify the inner persistence in a detached task, which should succeed without issue.
                    try await innerDatastore.persist(.init(id: "B", value: "InnerB-New"))
                    let innerValueB = try await innerDatastore.load(id: "B")
                    XCTAssertEqual(innerValueB?.value, "InnerB-New")
                }.value
                
                /// Check for the old value since we are locked to a transaction where reads already started.
                let innerValueB = try await innerDatastore.load(id: "B")
                XCTAssertEqual(innerValueB?.value, "InnerB")
            }
            
            /// Check to see that the inner persistence value is unchanged.
            let innerValue = try await innerDatastore.load(id: "A")
            XCTAssertEqual(innerValue?.value, "InnerA")
        }
        
        /// Make sure final values all agree.
        let outerValueA = try await outerDatastore.load(id: "A")
        let outerValueB = try await outerDatastore.load(id: "B")
        let innerValueA = try await innerDatastore.load(id: "A")
        let innerValueB = try await innerDatastore.load(id: "B")
        XCTAssertEqual(outerValueA?.value, "OuterA-New")
        XCTAssertEqual(outerValueB?.value, "OuterB")
        XCTAssertEqual(innerValueA?.value, "InnerA")
        XCTAssertEqual(innerValueB?.value, "InnerB-New")
    }
}
