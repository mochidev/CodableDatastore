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
        
        try await datastore.persist(TestStruct(id: "2", value: "My name is Dimitri"))
        try await datastore.persist(TestStruct(id: "1", value: "Hello, World!"))
    }
}
