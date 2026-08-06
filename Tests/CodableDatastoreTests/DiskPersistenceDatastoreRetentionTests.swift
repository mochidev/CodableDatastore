//
//  DiskPersistenceDatastoreRetentionTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-09-09.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

#if !canImport(Darwin)
@preconcurrency import Foundation
#endif
import XCTest
@testable import CodableDatastore

final class DiskPersistenceDatastoreRetentionTests: XCTestCase, @unchecked Sendable {
    var temporaryStoreURL: URL = FileManager.default.temporaryDirectory
    
    override func setUp() async throws {
        temporaryStoreURL = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString, isDirectory: true);
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: temporaryStoreURL)
    }
    
    func testTransactionCountPrunedDatastoreStillReadable() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: String
                var value: String
                var index: Int
                var bucket: Int
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
            
            let index = OneToOneIndex(\.index)
            @Direct var bucket = Index(\.bucket)
        }
        
        let max = 1000
        
        do {
            let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
            
            let datastore = Datastore.JSONStore(
                persistence: persistence,
                format: TestFormat.self,
                migrations: [
                    .zero: { data, decoder in
                        try decoder.decode(TestFormat.Instance.self, from: data)
                    }
                ]
            )
            
            await persistence.setTransactionRetentionPolicy(.transactionCount(0))
            try await persistence.createPersistenceIfNecessary()
            
            for index in 0..<max {
                try await datastore.persist(.init(
                    id: "\((index * 7) % max)",
                    value: "Twenty Three is Number One",
                    index: index,
                    bucket: index % 7
                ))
            }
            
            try await datastore.persist(.init(
                id: "0",
                value: "Twenty Three is Number One",
                index: 0,
                bucket: 1
            ))
            
            let count = try await datastore.count
            XCTAssertEqual(count, max)
            
            await persistence.enforceRetentionPolicy()
        }
        
        do {
            let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
            
            let datastore = Datastore.JSONStore(
                persistence: persistence,
                format: TestFormat.self,
                migrations: [
                    .zero: { data, decoder in
                        try decoder.decode(TestFormat.Instance.self, from: data)
                    }
                ]
            )
            
            let count = try await datastore.count
            XCTAssertEqual(count, max)
            
            let all = datastore.load(...)
            var entries = 0
            
            for try await entry in all {
                XCTAssertEqual(entry.value, "Twenty Three is Number One")
                entries += 1
            }
            
            XCTAssertEqual(entries, max)
            
            let allByIndex = datastore.load(..., from: \.index)
            entries = 0
            
            for try await entry in allByIndex {
                XCTAssertEqual(entry.value, "Twenty Three is Number One")
                entries += 1
            }
            
            XCTAssertEqual(entries, max)
            
            let allByBucket = datastore.load(..., from: \.bucket)
            entries = 0
            
            for try await entry in allByBucket {
                XCTAssertEqual(entry.value, "Twenty Three is Number One")
                entries += 1
            }
            
            XCTAssertEqual(entries, max)
        }
    }
    
    func testDurationPrunedDatastoreStillReadable() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: String
                var value: String
                var index: Int
                var bucket: Int
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
            
            let index = OneToOneIndex(\.index)
            @Direct var bucket = Index(\.bucket)
        }
        
        let max = 1000
        
        do {
            let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
            
            let datastore = Datastore.JSONStore(
                persistence: persistence,
                format: TestFormat.self,
                migrations: [
                    .zero: { data, decoder in
                        try decoder.decode(TestFormat.Instance.self, from: data)
                    }
                ]
            )
            
            await persistence.setTransactionRetentionPolicy(.duration(3))
            try await persistence.createPersistenceIfNecessary()
            
            for index in 0..<max {
                try await datastore.persist(.init(
                    id: "\((index * 7) % max)",
                    value: "Twenty Three is Number One",
                    index: index,
                    bucket: index % 7
                ))
            }
            
            try await Task.sleep(for: .seconds(5))
            
            try await datastore.persist(.init(
                id: "0",
                value: "Twenty Three is Number One",
                index: 0,
                bucket: 1
            ))
            
            let count = try await datastore.count
            XCTAssertEqual(count, max)
            
            await persistence.enforceRetentionPolicy()
        }
        
        do {
            let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
            
            let datastore = Datastore.JSONStore(
                persistence: persistence,
                format: TestFormat.self,
                migrations: [
                    .zero: { data, decoder in
                        try decoder.decode(TestFormat.Instance.self, from: data)
                    }
                ]
            )
            
            let count = try await datastore.count
            XCTAssertEqual(count, max)
            
            let all = datastore.load(...)
            var entries = 0
            
            for try await entry in all {
                XCTAssertEqual(entry.value, "Twenty Three is Number One")
                entries += 1
            }
            
            XCTAssertEqual(entries, max)
            
            let allByIndex = datastore.load(..., from: \.index)
            entries = 0
            
            for try await entry in allByIndex {
                XCTAssertEqual(entry.value, "Twenty Three is Number One")
                entries += 1
            }
            
            XCTAssertEqual(entries, max)
            
            let allByBucket = datastore.load(..., from: \.bucket)
            entries = 0
            
            for try await entry in allByBucket {
                XCTAssertEqual(entry.value, "Twenty Three is Number One")
                entries += 1
            }
            
            XCTAssertEqual(entries, max)
        }
    }
    
    func testDelayedEnforcement() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: String
                var value: String
                var index: Int
                var bucket: Int
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
            
            let index = OneToOneIndex(\.index)
            @Direct var bucket = Index(\.bucket)
        }
        
        let max = 1000
        
        do {
            let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
            
            let datastore = Datastore.JSONStore(
                persistence: persistence,
                format: TestFormat.self,
                migrations: [
                    .zero: { data, decoder in
                        try decoder.decode(TestFormat.Instance.self, from: data)
                    }
                ]
            )
            
            try await persistence.createPersistenceIfNecessary()
            
            for index in 0..<max {
                try await datastore.persist(.init(
                    id: "\((index * 7) % max)",
                    value: "Twenty Three is Number One",
                    index: index,
                    bucket: index % 7
                ))
            }
            
            try await datastore.persist(.init(
                id: "0",
                value: "Twenty Three is Number One",
                index: 0,
                bucket: 1
            ))
            
            let count = try await datastore.count
            XCTAssertEqual(count, max)
            
            await persistence.setTransactionRetentionPolicy(.transactionCount(0))
            await persistence.enforceRetentionPolicy()
            
            let count2 = try await datastore.count
            XCTAssertEqual(count2, max)
        }
    }
}
