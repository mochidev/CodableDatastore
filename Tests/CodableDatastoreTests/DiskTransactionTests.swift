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
}
