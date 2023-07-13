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
        enum Version: Int, CaseIterable {
            case zero
        }
        
        struct TestStruct: Codable, Identifiable {
            var id: String
            var value: String
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            key: "test",
            version: Version.zero,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestStruct.self, from: data)
                }
            ]
        )
        
        try await datastore.persist(TestStruct(id: "3", value: "My name is Dimitri"))
        try await datastore.persist(TestStruct(id: "1", value: "Hello, World!"))
        try await datastore.persist(TestStruct(id: "2", value: "Twenty Three is Number One"))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 3)
    }
    
    func testWritingEntryWithIndex() async throws {
        enum Version: Int, CaseIterable {
            case zero
        }
        
        struct TestStruct: Codable, Identifiable {
            var id: String
            @Indexed var value: String
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            key: "test",
            version: Version.zero,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestStruct.self, from: data)
                }
            ]
        )
        
        try await datastore.persist(TestStruct(id: "3", value: "My name is Dimitri"))
        try await datastore.persist(TestStruct(id: "1", value: "Hello, World!"))
        try await datastore.persist(TestStruct(id: "2", value: "Twenty Three is Number One"))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 3)
    }
    
    func testObservingEntries() async throws {
        enum Version: Int, CaseIterable {
            case zero
        }
        
        struct TestStruct: Codable, Identifiable {
            var id: String
            var value: Int
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            key: "test",
            version: Version.zero,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestStruct.self, from: data)
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
        
        try await datastore.persist(TestStruct(id: "3", value: 3))
        try await datastore.persist(TestStruct(id: "1", value: 1))
        try await datastore.persist(TestStruct(id: "2", value: 2))
        try await datastore.persist(TestStruct(id: "1", value: 5))
        try await datastore.delete("2")
        try await datastore.persist(TestStruct(id: "1", value: 3))
        
        let count = try await datastore.count
        XCTAssertEqual(count, 2)
        let total = try await observations.value
        XCTAssertEqual(total, 13)
    }
    
    func testWritingManyEntries() async throws {
        enum Version: Int, CaseIterable {
            case zero
        }
        
        struct TestStruct: Codable, Identifiable {
            var id: UUID = UUID()
            var value: String
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            key: "test",
            version: Version.zero,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestStruct.self, from: data)
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
                try await datastore.persist(TestStruct(value: valueBank.randomElement()!))
            }
            let now = ProcessInfo.processInfo.systemUptime
            print("\(n*100): \((100*(now - time)).rounded()/100)s -   total: \((10*(now - start)).rounded()/10)s")
        }
    }
    
    func testWritingManyEntriesInTransactions() async throws {
        enum Version: Int, CaseIterable {
            case zero
        }
        
        struct TestStruct: Codable, Identifiable {
            var id: UUID = UUID()
            var value: String
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            key: "test",
            version: Version.zero,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestStruct.self, from: data)
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
            try await persistence.perform { persistence in
                for _ in 0..<5000 {
                    try await datastore.persist(TestStruct(value: valueBank.randomElement()!))
                }
            }
            let now = ProcessInfo.processInfo.systemUptime
            print("\(n*5000): \((100*(now - time)).rounded()/100)s -   total: \((10*(now - start)).rounded()/10)s")
        }
    }
    
    func testWritingManyConsecutiveEntriesInTransactions() async throws {
        enum Version: Int, CaseIterable {
            case zero
        }
        
        struct TestStruct: Codable, Identifiable {
            var id: Int
            var value: String
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            key: "test",
            version: Version.zero,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestStruct.self, from: data)
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
            try await persistence.perform { persistence in
                for m in 0..<5000 {
                    let id = (n-1)*5000 + m
                    try await datastore.persist(TestStruct(id: id, value: valueBank.randomElement()!))
                }
            }
            let now = ProcessInfo.processInfo.systemUptime
            print("\(n*5000): \((100*(now - time)).rounded()/100)s -   total: \((10*(now - start)).rounded()/10)s")
        }
    }
    
    func testReplacingEntriesInTransactions() async throws {
        enum Version: Int, CaseIterable {
            case zero
        }
        
        struct TestStruct: Codable, Identifiable {
            var id: Int
            var value: String
        }
        
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let datastore = Datastore.JSONStore(
            persistence: persistence,
            key: "test",
            version: Version.zero,
            migrations: [
                .zero: { data, decoder in
                    try decoder.decode(TestStruct.self, from: data)
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
        
        try await persistence.perform { persistence in
            for id in 0..<5000 {
                try await datastore.persist(TestStruct(id: id, value: valueBank.randomElement()!))
            }
        }
        
        let start = ProcessInfo.processInfo.systemUptime
        for n in 1...100 {
            let time = ProcessInfo.processInfo.systemUptime
            try await persistence.perform { persistence in
                for _ in 0..<100 {
                    try await datastore.persist(TestStruct(id: Int.random(in: 0..<5000), value: valueBank.randomElement()!))
                }
            }
            let now = ProcessInfo.processInfo.systemUptime
            print("\(n*100): \((100*(now - time)).rounded()/100)s -   total: \((10*(now - start)).rounded()/10)s")
        }
    }
}
