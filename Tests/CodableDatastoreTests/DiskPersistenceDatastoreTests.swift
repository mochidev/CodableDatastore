//
//  DiskTransactionTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-02.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class DiskPersistenceDatastoreTests: XCTestCase {
    var temporaryStoreURL: URL = FileManager.default.temporaryDirectory
    
    override func setUp() async throws {
        temporaryStoreURL = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString, isDirectory: true);
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: temporaryStoreURL)
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
        }
        
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
        }
        
        do {
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
                @Indexed var value: String
            }
        }
        
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
        
        try await datastore.persist(.init(id: "3", value: "My name is Dimitri"))
        try await datastore.persist(.init(id: "1", value: "Hello, World!"))
        try await datastore.persist(.init(id: "2", value: "Twenty Three is Number One"))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 3)
        
        let values = try await datastore.load("A"..."Z", order: .descending, from: IndexPath(uncheckedKeyPath: \.$value, path: "$value")).map { $0.id }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["2", "3", "1"])
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
        }
        
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
        }
        
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
        
        /// Read before persisting anything
        var values = try await datastore.load(...).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, [])
        
        for n in 0..<200 {
            try await datastore.persist(.init(id: n*2, value: "\(n*2)"))
        }
        
        let count = try await datastore.count
        XCTAssertEqual(count, 200)
        
        /// Simple ranges
        values = try await datastore.load(5..<9).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["6", "8"])
        
        values = try await datastore.load((5..<9).reversed).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["8", "6"])
        
        /// Larger ranges
        values = try await datastore.load(221..<241).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["222", "224", "226", "228", "230", "232", "234", "236", "238", "240"])

        values = try await datastore.load((221..<241).reversed).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["240", "238", "236", "234", "232", "230", "228", "226", "224", "222"])
        
        /// Across page boudries
        values = try await datastore.load(209...217).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["210", "212", "214", "216"])
        
        values = try await datastore.load((209...217).reversed).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["216", "214", "212", "210"])
        
        /// Unbounded ranges
        values = Array(try await datastore.load(.unbounded).map { $0.value }.reduce(into: []) { $0.append($1) }.prefix(5))
        XCTAssertEqual(values, ["0", "2", "4", "6", "8"])
        
        values = Array(try await datastore.load(...).map { $0.value }.reduce(into: []) { $0.append($1) }.prefix(5))
        XCTAssertEqual(values, ["0", "2", "4", "6", "8"])
        
        values = Array(try await datastore.load(.unbounded.reversed).map { $0.value }.reduce(into: []) { $0.append($1) }.prefix(5))
        XCTAssertEqual(values, ["398", "396", "394", "392", "390"])
        
        values = Array(try await datastore.load(..., order: .descending).map { $0.value }.reduce(into: []) { $0.append($1) }.prefix(5))
        XCTAssertEqual(values, ["398", "396", "394", "392", "390"])
        
        /// Inclusive ranges
        values = try await datastore.load(6...10).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["6", "8", "10"])
        
        values = try await datastore.load(6...10, order: .descending).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["10", "8", "6"])
        
        /// Exclusive ranges
        values = try await datastore.load(6..<10).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["6", "8"])
        
        values = try await datastore.load(6..<10, order: .descending).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["8", "6"])
        
        values = try await datastore.load(6..>10).map { $0.value }.reduce(into: []) { $0.append($1) }
        XCTAssertEqual(values, ["8", "10"])
        
        values = try await datastore.load(6..>10, order: .descending).map { $0.value }.reduce(into: []) { $0.append($1) }
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
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
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
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        let start = ProcessInfo.processInfo.systemUptime
        for n in 1...100 {
            let time = ProcessInfo.processInfo.systemUptime
            for _ in 0..<100 {
                try await datastore.persist(.init(value: valueBank.randomElement()!))
            }
            let now = ProcessInfo.processInfo.systemUptime
            print("\(n*100): \((100*(now - time)).rounded()/100)s -   total: \((10*(now - start)).rounded()/10)s")
        }
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
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
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
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        let start = ProcessInfo.processInfo.systemUptime
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
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
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
        
        let valueBank = [
            "Hello, World!",
            "My name is Dimitri",
            "Writen using CodableDatastore",
            "Swift is better than Objective-C, there, I said it",
            "Twenty Three is Number One"
        ]
        
        let start = ProcessInfo.processInfo.systemUptime
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
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
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
        
        let start = ProcessInfo.processInfo.systemUptime
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
    }
}
