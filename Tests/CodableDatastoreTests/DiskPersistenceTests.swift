//
//  DiskPersistenceTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-07.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class DiskPersistenceTests: XCTestCase {
    func testTypes() throws {
#if !os(Linux)
        XCTAssertTrue(type(of: try DiskPersistence.defaultStore() as Any) is DiskPersistence<ReadWrite>.Type)
        XCTAssertTrue(type(of: try DiskPersistence.readOnlyDefaultStore() as Any) is DiskPersistence<ReadOnly>.Type)
#endif
        XCTAssertTrue(type(of: try DiskPersistence(readWriteURL: FileManager.default.temporaryDirectory) as Any) is DiskPersistence<ReadWrite>.Type)
        XCTAssertTrue(type(of: DiskPersistence(readOnlyURL: FileManager.default.temporaryDirectory) as Any) is DiskPersistence<ReadOnly>.Type)
    }
    
#if os(Linux)
    func testDefaultURLs() async throws {
        do {
            _ = try await DiskPersistence.defaultStore().storeURL
        } catch {
            XCTAssertEqual(error as? DiskPersistenceError, DiskPersistenceError.missingBundleID)
        }
        do {
            _ = try await DiskPersistence.readOnlyDefaultStore().storeURL
        } catch {
            XCTAssertEqual(error as? DiskPersistenceError, DiskPersistenceError.missingBundleID)
        }
    }
#else
    func testDefaultURLs() async throws {
        let defaultDirectory = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first!.absoluteString.appending("com.apple.dt.xctest.tool/DefaultStore.persistencestore/")
        
        let url = try await DiskPersistence.defaultStore().storeURL
        XCTAssertEqual(url.absoluteString, defaultDirectory)
        
        let url2 = try await DiskPersistence.readOnlyDefaultStore().storeURL
        XCTAssertEqual(url2.absoluteString, defaultDirectory)
    }
#endif
}
