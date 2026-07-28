//
//  DiskTransactionTests.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-02.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

#if !canImport(Darwin)
@preconcurrency import Foundation
#endif
import XCTest
@testable import CodableDatastore

final class DiskTransactionTests: XCTestCase, @unchecked Sendable {
    var temporaryStoreURL: URL = FileManager.default.temporaryDirectory
    
    override func setUp() async throws {
        temporaryStoreURL = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString, isDirectory: true);
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: temporaryStoreURL)
    }
    
    func testApplyDescriptor() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable {}
            typealias Identifier = UUID
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        
        let datastore = Datastore(
            persistence: persistence,
            format: TestFormat.self,
            decoders: [.zero: { _ in (id: UUID(), instance: TestFormat.Instance()) }],
            configuration: .init()
        )
        
        let descriptor = DatastoreDescriptor(
            version: Data([0x00]),
            instanceType: "TestStruct",
            identifierType: "UUID",
            directIndexes: [:],
            referenceIndexes: [:],
            size: 0
        )
        
        try await persistence._withTransaction(actionName: nil, options: []) { transaction, _ in
            let existingDescriptor = try await transaction.register(datastore: datastore)
            XCTAssertNil(existingDescriptor)
        }
        
        try await persistence._withTransaction(actionName: nil, options: []) { transaction, _ in
            try await transaction.apply(descriptor: descriptor, for: "test")
        }
        
        try await persistence._withTransaction(actionName: nil, options: []) { transaction, _ in
            let existingDescriptor = try await transaction.datastoreDescriptor(for: "test")
            XCTAssertEqual(existingDescriptor, descriptor)
        }
    }
    
    func testSerialMutableRootTransactions() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable {}
            typealias Identifier = UUID
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        var tasks: [Task<Void, Never>] = []
        
        let iterations = 5000
        nonisolated(unsafe) var count = 0
        nonisolated(unsafe) var lastStartedIndex = 0
        for index in 0..<iterations {
            tasks.append(Task {
                let result = try? await persistence._withTransaction(actionName: nil, options: []) { _, _ in
                    lastStartedIndex = index
                    try? await Task.sleep(for: .seconds(Double.random(in: 0.0001...0.001)))
                    count += 1
                    XCTAssertEqual(lastStartedIndex, index)
                    return index
                }
                
                XCTAssertEqual(result, index)
            })
        }
        
        for task in tasks {
            await task.value
        }
        XCTAssertEqual(count, iterations)
    }
    
    func testConcurrentReadOnlyRootTransactions() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable {}
            typealias Identifier = UUID
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        var tasks: [Task<Void, Never>] = []
        
        let iterations = 5000
        nonisolated(unsafe) var count = 0
        for index in 0..<iterations {
            tasks.append(Task {
                let result = try? await persistence._withTransaction(actionName: nil, options: [.readOnly]) { _, _ in
                    try? await Task.sleep(for: .seconds(Double.random(in: 0.0001...0.001)))
                    count += 1
                    return index
                }
                
                XCTAssertEqual(result, index)
            })
        }
        
        for task in tasks {
            await task.value
        }
        XCTAssertLessThanOrEqual(count, iterations)
    }
    
    func testSerialMutableChildTransactions() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable {}
            typealias Identifier = UUID
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        nonisolated(unsafe) var tasks: [Task<Void, Never>] = []
        
        let iterations = 5000
        nonisolated(unsafe) var count = 0
        nonisolated(unsafe) var lastStartedIndex = 0
        try await persistence._withTransaction(actionName: nil, options: []) { _, _ in
            for index in 0..<iterations {
                tasks.append(Task {
                    let result = try? await persistence._withTransaction(actionName: nil, options: []) { _, _ in
                        lastStartedIndex = index
                        try? await Task.sleep(for: .seconds(Double.random(in: 0.0001...0.001)))
                        count += 1
                        XCTAssertEqual(lastStartedIndex, index)
                        return index
                    }
                    
                    XCTAssertEqual(result, index)
                })
            }
            
            for task in tasks {
                await task.value
            }
        }
        XCTAssertEqual(count, iterations)
    }
    
    func testConcurrentReadOnlyChildTransactions() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        struct TestFormat: DatastoreFormat {
            enum Version: Int, CaseIterable {
                case zero
            }
            
            struct Instance: Codable {}
            typealias Identifier = UUID
            
            static let defaultKey: DatastoreKey = "test"
            static let currentVersion = Version.zero
        }
        
        nonisolated(unsafe) var tasks: [Task<Void, Never>] = []
        
        let iterations = 5000
        nonisolated(unsafe) var count = 0
        try await persistence._withTransaction(actionName: nil, options: []) { _, _ in
            for index in 0..<iterations {
                tasks.append(Task {
                    let result = try? await persistence._withTransaction(actionName: nil, options: [.readOnly]) { _, _ in
                        try? await Task.sleep(for: .seconds(Double.random(in: 0.0001...0.001)))
                        count += 1
                        return index
                    }
                    
                    XCTAssertEqual(result, index)
                })
            }
            
            for task in tasks {
                await task.value
            }
        }
        XCTAssertLessThanOrEqual(count, iterations)
    }
}
