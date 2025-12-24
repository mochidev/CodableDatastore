//
//  DiskTransactionTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-02.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

#if !canImport(Darwin)
@preconcurrency import Foundation
#endif
import XCTest
@testable import CodableDatastore

final class DiskPersistenceDatastoreTests: XCTestCase, @unchecked Sendable {
    var temporaryStoreURL: URL = FileManager.default.temporaryDirectory
    
    override func setUp() async throws {
        temporaryStoreURL = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString, isDirectory: true);
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: temporaryStoreURL)
    }
    
    func testCreatingEmptyPersistence() async throws {
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
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        _ = Datastore.JSONStore(
            persistence: persistence,
            format: TestFormat.self,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestFormat.Instance.self, from: data)
                }
            ]
        )
        
        try await persistence.createPersistenceIfNecessary()
        
        let snapshotContents = try FileManager().contentsOfDirectory(at: temporaryStoreURL.appendingPathComponent("Snapshots", isDirectory: true), includingPropertiesForKeys: nil)
        XCTAssertEqual(snapshotContents.count, 0)
    }
    
    func testCreatingEmptyDatastoreIndexesAfterRead() async throws {
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
        XCTAssertEqual(count, 0)
        
        // TODO: Add code to verify that the Datastores directory is empty. This is true as of 2024-10-10, but has only been validated manually.
    }
    
    func testCreatingEmptyDatastoreIndexesAfterSingleWrite() async throws {
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
        
        try await datastore.persist(.init(id: "0", value: "0", index: 0, bucket: 0))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 1)
        
        // TODO: Add code to verify that the Index directories have a single manifest each. This is true as of 2024-10-10, but has only been validated manually.
    }
    
    func testCreatingUnreferencedDatastoreIndexesAfterUpdate() async throws {
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
        
        try await datastore.persist(.init(id: "0", value: "0", index: 0, bucket: 0))
        try await datastore.persist(.init(id: "0", value: "0", index: 0, bucket: 0))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 1)
        
        // TODO: Add code to verify that the Index directories have exactly two index manifests each. This is true as of 2024-10-10, but has only been validated manually.
    }
    
    func testWritingEntry() async throws {
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
        
        try await datastore.persist(.init(id: "3", value: "My name is Dimitri"))
        try await datastore.persist(.init(id: "1", value: "Hello, World!"))
        try await datastore.persist(.init(id: "2", value: "Twenty Three is Number One"))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 3)
    }
    
    func testLoadingEntriesFromDisk() async throws {
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
            XCTAssertEqual(count, 0)
            
            let entry0 = try await datastore.load("0")
            XCTAssertNil(entry0)
            
            try await datastore.persist(.init(id: "3", value: "My name is Dimitri"))
            try await datastore.persist(.init(id: "1", value: "Hello, World!"))
            try await datastore.persist(.init(id: "2", value: "Twenty Three is Number One"))
        } catch { throw error }
        
        /// Create a brand new persistence and load the entries we saved
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        let datastore = Datastore<TestFormat, _>.JSONStore(
            persistence: persistence,
            key: "test",
            version: .zero,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestFormat.Instance.self, from: data)
                }
            ]
        )
        let count = try await datastore.count
        XCTAssertEqual(count, 3)
        
        let entry0 = try await datastore.load("0")
        XCTAssertNil(entry0)
        let entry1 = try await datastore.load("1")
        XCTAssertEqual(entry1?.value, "Hello, World!")
        let entry2 = try await datastore.load("2")
        XCTAssertEqual(entry2?.value, "Twenty Three is Number One")
        let entry3 = try await datastore.load("3")
        XCTAssertEqual(entry3?.value, "My name is Dimitri")
    }
    
    func testWritingEntryWithIndex() async throws {
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
            
            let value = Index(\.value)
        }
        
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
        
        try await datastore.persist(.init(id: "3", value: "My name is Dimitri"))
        try await datastore.persist(.init(id: "1", value: "Hello, World!"))
        try await datastore.persist(.init(id: "2", value: "Twenty Three is Number One"))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 3)
        
        let values = try await datastore.load("A"..."Z", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["2", "3", "1"])
    }
    
    func testWritingEntryWithOneToOneIndex() async throws {
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
            
            let value = OneToOneIndex(\.value)
        }
        
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
        
        try await datastore.persist(.init(id: "3", value: "My name is Dimitri"))
        try await datastore.persist(.init(id: "1", value: "Hello, World!"))
        try await datastore.persist(.init(id: "2", value: "Twenty Three is Number One"))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 3)
        
        let values = try await datastore.load("A"..."Z", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["2", "3", "1"])
        let value3 = try await datastore.load("My name is Dimitri", from: \.value).map { $0.id }
        XCTAssertEqual(value3, "3")
        let value1 = try await datastore.load("Hello, World!", from: \.value).map { $0.id }
        XCTAssertEqual(value1, "1")
        let value2 = try await datastore.load("Twenty Three is Number One", from: \.value).map { $0.id }
        XCTAssertEqual(value2, "2")
        let valueNil = try await datastore.load("D", from: \.value).map { $0.id }
        XCTAssertNil(valueNil)
    }
    
    func testWritingEntryWithManyToManyIndex() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: String
                var value: [String]
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
            
            let value = ManyToManyIndex(\.value)
        }
        
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
        
        try await datastore.persist(.init(id: "3", value: ["My name is Dimitri", "A", "B"]))
        try await datastore.persist(.init(id: "1", value: ["Hello, World!", "B", "B", "C"]))
        try await datastore.persist(.init(id: "2", value: ["Twenty Three is Number One", "C"]))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 3)
        
        let values = try await datastore.load("A"..."Z", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["2", "3", "1", "2", "1", "3", "1", "3"])
        let valuesA = try await datastore.load("A", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(valuesA, ["3"])
        let valuesB = try await datastore.load("B", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(valuesB, ["3", "1"])
        let valuesC = try await datastore.load("C", order: .ascending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(valuesC, ["1", "2"])
        let valuesD = try await datastore.load("D", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(valuesD, [])
    }
    
    func testWritingEntryWithManyToOneIndex() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: String
                var value: [String]
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
            
            let value = ManyToOneIndex(\.value)
        }
        
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
        
        try await datastore.persist(.init(id: "3", value: ["My name is Dimitri", "A", "B"]))
        try await datastore.persist(.init(id: "1", value: ["Hello, World!", "B", "B", "C"]))
        try await datastore.persist(.init(id: "2", value: ["Twenty Three is Number One", "C"]))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 3)
        
        let values = try await datastore.load("A"..."Z", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["2", "3", "1", "2", "1", "3", "1", "3"])
        let valuesA = try await datastore.load("A", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(valuesA, ["3"])
        let valueA = try await datastore.load("A", from: \.value).map { $0.id }
        XCTAssertEqual(valueA, "3")
        let valuesB = try await datastore.load("B", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(valuesB, ["3", "1"])
        let valueB = try await datastore.load("B", from: \.value).map { $0.id }
        XCTAssertEqual(valueB, "1")
        let valuesC = try await datastore.load("C", order: .ascending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(valuesC, ["1", "2"])
        let valueC = try await datastore.load("C", from: \.value).map { $0.id }
        XCTAssertEqual(valueC, "1")
        let valuesD = try await datastore.load("D", order: .descending, from: \.value).map { $0.id }.collectInstances(upTo: .infinity)
        XCTAssertEqual(valuesD, [])
        let valueNil = try await datastore.load("D", from: \.value).map { $0.id }
        XCTAssertNil(valueNil)
    }
    
    func testObservingEntries() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: String
                var value: Int
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
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
        
        let events = try await datastore.observe()
        
        let observations = Task {
            var total = 0
            loop: for try await event in events {
                switch event {
                case .created(_, let entry):
                    total += entry.value
                case .updated(_, _, let entry):
                    total += entry.value
                case .deleted(_, let entry):
                    total += entry.value
                    break loop
                }
            }
            return total
        }
        
        try await datastore.persist(.init(id: "3", value: 3))
        try await datastore.persist(.init(id: "1", value: 1))
        try await datastore.persist(.init(id: "2", value: 2))
        try await datastore.persist(.init(id: "1", value: 5))
        try await datastore.delete("2")
        try await datastore.persist(.init(id: "1", value: 3))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 2)
        let total = try await observations.value
        XCTAssertEqual(total, 13)
    }
    
    func testRangeReads() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: Int
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
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
        
        /// Read before persisting anything
        var values = try await datastore.load(...).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, [])
        
        for n in 0..<200 {
            try await datastore.persist(.init(id: n*2, value: "\(n*2)"))
        }
        
        let count = try await datastore.count
        XCTAssertEqual(count, 200)
        
        let iteratedCount = try await datastore.load(...).reduce(into: 0) { partialResult, _ in partialResult += 1 }
        XCTAssertEqual(iteratedCount, 200)
        
        /// Simple ranges
        values = try await datastore.load(5..<9).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["6", "8"])
        
        values = try await datastore.load((5..<9).reversed).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["8", "6"])
        
        /// Larger ranges
        values = try await datastore.load(221..<241).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["222", "224", "226", "228", "230", "232", "234", "236", "238", "240"])

        values = try await datastore.load((221..<241).reversed).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["240", "238", "236", "234", "232", "230", "228", "226", "224", "222"])
        
        /// Across page boudries
        values = try await datastore.load(209...217).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["210", "212", "214", "216"])
        
        values = try await datastore.load((209...217).reversed).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["216", "214", "212", "210"])
        
        /// Unbounded ranges
        values = Array(try await datastore.load(.unbounded).map { $0.value }.collectInstances(upTo: .infinity).prefix(5))
        XCTAssertEqual(values, ["0", "2", "4", "6", "8"])
        
        values = Array(try await datastore.load(...).map { $0.value }.collectInstances(upTo: .infinity).prefix(5))
        XCTAssertEqual(values, ["0", "2", "4", "6", "8"])
        
        values = Array(try await datastore.load(.unbounded.reversed).map { $0.value }.collectInstances(upTo: .infinity).prefix(5))
        XCTAssertEqual(values, ["398", "396", "394", "392", "390"])
        
        values = Array(try await datastore.load(..., order: .descending).map { $0.value }.collectInstances(upTo: .infinity).prefix(5))
        XCTAssertEqual(values, ["398", "396", "394", "392", "390"])
        
        /// Inclusive ranges
        values = try await datastore.load(6...10).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["6", "8", "10"])
        
        values = try await datastore.load(6...10, order: .descending).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["10", "8", "6"])
        
        /// Exclusive ranges
        values = try await datastore.load(6..<10).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["6", "8"])
        
        values = try await datastore.load(6..<10, order: .descending).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["8", "6"])
        
        values = try await datastore.load(6..>10).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["8", "10"])
        
        values = try await datastore.load(6..>10, order: .descending).map { $0.value }.collectInstances(upTo: .infinity)
        XCTAssertEqual(values, ["10", "8"])
    }
    
    func testWritingManyEntries() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: UUID = UUID()
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            format: TestFormat.self,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestFormat.Instance.self, from: data)
                }
            ]
        )
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        var start = ProcessInfo.processInfo.systemUptime
        for n in 1...100 {
            let time = ProcessInfo.processInfo.systemUptime
            for _ in 0..<100 {
                try await datastore.persist(.init(value: valueBank.randomElement()!))
            }
            let now = ProcessInfo.processInfo.systemUptime
            print("\(n*100): \((100*(now - time)).rounded()/100)s -   total: \((10*(now - start)).rounded()/10)s")
        }
        
        let count = try await datastore.count
        start = ProcessInfo.processInfo.systemUptime
        let iteratedCount = try await datastore.load(...).reduce(into: 0) { partialResult, _ in partialResult += 1 }
        let now = ProcessInfo.processInfo.systemUptime
        print("Scanning \(iteratedCount) instances: \((1000*(now - start)).rounded()/1000)s")
        XCTAssertEqual(count, iteratedCount)
    }
    
    func testWritingManyEntriesInTransactions() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: UUID = UUID()
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            format: TestFormat.self,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestFormat.Instance.self, from: data)
                }
            ]
        )
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        var start = ProcessInfo.processInfo.systemUptime
        for n in 1...5 {
            let time = ProcessInfo.processInfo.systemUptime
            try await persistence.perform {
                for _ in 0..<5000 {
                    try await datastore.persist(.init(value: valueBank.randomElement()!))
                }
            }
            let now = ProcessInfo.processInfo.systemUptime
            print("\(n*5000): \((100*(now - time)).rounded()/100)s -   total: \((10*(now - start)).rounded()/10)s")
        }
        
        let count = try await datastore.count
        start = ProcessInfo.processInfo.systemUptime
        let iteratedCount = try await datastore.load(...).reduce(into: 0) { partialResult, _ in partialResult += 1 }
        let now = ProcessInfo.processInfo.systemUptime
        print("Scanning \(iteratedCount) instances: \((1000*(now - start)).rounded()/1000)s")
        XCTAssertEqual(count, iteratedCount)
    }
    
    func testWritingManyConsecutiveEntriesInTransactions() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: Int
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            format: TestFormat.self,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestFormat.Instance.self, from: data)
                }
            ]
        )
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        var start = ProcessInfo.processInfo.systemUptime
        for n in 1...5 {
            let time = ProcessInfo.processInfo.systemUptime
            try await persistence.perform {
                for m in 0..<5000 {
                    let id = (n-1)*5000 + m
                    try await datastore.persist(.init(id: id, value: valueBank.randomElement()!))
                }
            }
            let now = ProcessInfo.processInfo.systemUptime
            print("\(n*5000): \((100*(now - time)).rounded()/100)s -   total: \((10*(now - start)).rounded()/10)s")
        }
        
        let count = try await datastore.count
        start = ProcessInfo.processInfo.systemUptime
        let iteratedCount = try await datastore.load(...).reduce(into: 0) { partialResult, _ in partialResult += 1 }
        let now = ProcessInfo.processInfo.systemUptime
        print("Scanning \(iteratedCount) instances: \((1000*(now - start)).rounded()/1000)s")
        XCTAssertEqual(count, iteratedCount)
    }
    
    func testReplacingEntriesInTransactions() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: Int
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            format: TestFormat.self,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestFormat.Instance.self, from: data)
                }
            ]
        )
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        try await persistence.perform {
            for id in 0..<5000 {
                try await datastore.persist(.init(id: id, value: valueBank.randomElement()!))
            }
        }
        
        var start = ProcessInfo.processInfo.systemUptime
        for n in 1...100 {
            let time = ProcessInfo.processInfo.systemUptime
            try await persistence.perform {
                for _ in 0..<100 {
                    try await datastore.persist(.init(id: Int.random(in: 0..<5000), value: valueBank.randomElement()!))
                }
            }
            let now = ProcessInfo.processInfo.systemUptime
            print("\(n*100): \((100*(now - time)).rounded()/100)s -   total: \((10*(now - start)).rounded()/10)s")
        }
        
        let count = try await datastore.count
        start = ProcessInfo.processInfo.systemUptime
        let iteratedCount = try await datastore.load(...).reduce(into: 0) { partialResult, _ in partialResult += 1 }
        let now = ProcessInfo.processInfo.systemUptime
        print("Scanning \(iteratedCount) instances: \((1000*(now - start)).rounded()/1000)s")
        XCTAssertEqual(count, iteratedCount)
    }
    
    func testNestedTransactions() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: Int
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            format: TestFormat.self,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestFormat.Instance.self, from: data)
                }
            ]
        )
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        try await persistence.perform {
            for id in 0..<10 {
                try await datastore.persist(.init(id: id, value: valueBank.randomElement()!))
            }
        }
        
        try await persistence.perform {
            let allInstances = datastore.load(...)
            try await datastore.persist(.init(id: 10, value: valueBank.randomElement()!))
            try await persistence.perform {
                /// Resolve and close out the previous child transaction, which should not corrupt the parent.
                let resolvedInstances = try await allInstances.collectInstances(upTo: .infinity)
                XCTAssertEqual(resolvedInstances.count, 10)
                
                /// Allow corruption to occur if they will.
                try await Task.sleep(for: .seconds(1))
                
                /// Check to make sure that we are reading the last written to root object, not the one that just got applied.
                let lastAddedInstance = try await datastore.load(10)
                XCTAssertNotNil(lastAddedInstance)
            }
        }
    }
    
    func testReadOnlyTransactionDoesNotOverrideWrittenTransactions() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: Int
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            format: TestFormat.self,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestFormat.Instance.self, from: data)
                }
            ]
        )
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        try await persistence.perform {
            for id in 0..<10 {
                try await datastore.persist(.init(id: id, value: valueBank.randomElement()!))
            }
        }
        
        var totalCount = try await datastore.count
        XCTAssertEqual(totalCount, 10)
        
        let (startReadingTask, startReadingContinuation) = await Task.makeUnresolved()
        let (didStartReadingTask, didStartReadingContinuation) = await Task.makeUnresolved()
        
        let readTask = Task {
            let allEntries = datastore.load(...)
            
            didStartReadingContinuation.resume()
            await startReadingTask.value
            
            var count = 0
            for try await _ in allEntries {
                count += 1
            }
        }
        
        await didStartReadingTask.value
        
        try await persistence.perform {
            for id in 10..<20 {
                try await datastore.persist(.init(id: id, value: valueBank.randomElement()!))
            }
        }
        totalCount = try await datastore.count
        XCTAssertEqual(totalCount, 20)
        
        startReadingContinuation.resume()
        try await readTask.value
        
        totalCount = try await datastore.count
        XCTAssertEqual(totalCount, 20)
    }
    
    func testNestedReadOnlyTransactionDoesNotOverrideWrittenTransactions() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: Int
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            format: TestFormat.self,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestFormat.Instance.self, from: data)
                }
            ]
        )
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        try await persistence.perform {
            for id in 0..<10 {
                try await datastore.persist(.init(id: id, value: valueBank.randomElement()!))
            }
        }
        
        var totalCount = try await datastore.count
        XCTAssertEqual(totalCount, 10)
        
        let (startReadingTask, startReadingContinuation) = await Task.makeUnresolved()
        let (didStartReadingTask, didStartReadingContinuation) = await Task.makeUnresolved()
        
        let readTask = Task {
            try await persistence.perform(options: .readOnly) {
                let allEntries = datastore.load(...)
                
                didStartReadingContinuation.resume()
                await startReadingTask.value
                
                var count = 0
                for try await _ in allEntries {
                    count += 1
                }
            }
        }
        
        await didStartReadingTask.value
        
        try await persistence.perform {
            for id in 10..<20 {
                try await datastore.persist(.init(id: id, value: valueBank.randomElement()!))
            }
        }
        totalCount = try await datastore.count
        XCTAssertEqual(totalCount, 20)
        
        startReadingContinuation.resume()
        try await readTask.value
        
        totalCount = try await datastore.count
        XCTAssertEqual(totalCount, 20)
    }
    
    func testTakingSnapshots() async throws {
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable, Identifiable {
                var id: UUID = UUID()
                var value: String
            }
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        do {
            let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
            try await persistence.createPersistenceIfNecessary()
            
            let datastore = Datastore.JSONStore(
                persistence: persistence,
                format: TestFormat.self,
                migrations: [
                    .zero: { data, decoder in
                        try decoder.decode(TestFormat.Instance.self, from: data)
                    }
                ]
            )
            
            let valueBank = [
                "Hello, World!",
                "My name is Dimitri",
                "Writen using CodableDatastore",
                "Swift is better than Objective-C, there, I said it",
                "Twenty Three is Number One"
            ]
            
            for _ in 1...100 {
                try await datastore.persist(.init(value: valueBank.randomElement()!))
            }
            
            let count = try await datastore.count
            let iteratedCount = try await datastore.load(...).reduce(into: 0) { partialResult, _ in partialResult += 1 }
            XCTAssertEqual(count, iteratedCount)
        }
        
        do {
            let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
            try await persistence._takeSnapshot()
            
            let datastore = Datastore.JSONStore(
                persistence: persistence,
                format: TestFormat.self,
                migrations: [
                    .zero: { data, decoder in
                        try decoder.decode(TestFormat.Instance.self, from: data)
                    }
                ]
            )
            
            try await datastore.persist(.init(value: "hello"))
            
            let count = try await datastore.count
            let iteratedCount = try await datastore.load(...).reduce(into: 0) { partialResult, _ in partialResult += 1 }
            XCTAssertEqual(count, iteratedCount)
            XCTAssertEqual(count, 101)
        }
    }
}
