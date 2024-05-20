//
//  DiskPersistenceTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-07.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

#if !canImport(Darwin)
@preconcurrency import Foundation
#endif
import XCTest
@testable import CodableDatastore

final class DiskPersistenceTests: XCTestCase {
    var temporaryStoreURL: URL = FileManager.default.temporaryDirectory
    
    override func setUp() async throws {
        temporaryStoreURL = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString, isDirectory: true);
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: temporaryStoreURL)
    }
    
    func testTypes() throws {
#if canImport(Darwin)
        XCTAssertTrue(type(of: try DiskPersistence.defaultStore() as Any) is DiskPersistence<ReadWrite>.Type)
        XCTAssertTrue(type(of: try DiskPersistence.readOnlyDefaultStore() as Any) is DiskPersistence<ReadOnly>.Type)
#endif
        XCTAssertTrue(type(of: try DiskPersistence(readWriteURL: FileManager.default.temporaryDirectory) as Any) is DiskPersistence<ReadWrite>.Type)
        XCTAssertTrue(type(of: DiskPersistence(readOnlyURL: FileManager.default.temporaryDirectory) as Any) is DiskPersistence<ReadOnly>.Type)
    }
    
#if !canImport(Darwin)
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
    
    func testNoFileCreatedOnInit() async throws {
        _ = try DiskPersistence(readWriteURL: temporaryStoreURL)
        XCTAssertThrowsError(try temporaryStoreURL.checkResourceIsReachable())
        
        _ = DiskPersistence(readOnlyURL: temporaryStoreURL)
        XCTAssertThrowsError(try temporaryStoreURL.checkResourceIsReachable())
    }
    
    func testThrowsWithRemoteURLs() async throws {
        XCTAssertThrowsError(try DiskPersistence(readWriteURL: URL(string: "https://apple.com/")!))
    }
    
    func testStoreCreatedWhenAsked() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        XCTAssertTrue(try temporaryStoreURL.checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Snapshots", isDirectory: true).checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Backups", isDirectory: true).checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Info.json", isDirectory: false).checkResourceIsReachable())
    }
    
    func testStoreInfoOnEmptyStore() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let data = try Data(contentsOf: temporaryStoreURL.appendingPathComponent("Info.json", isDirectory: false))
        
        struct TestStruct: Codable {
            var version: String
            var modificationDate: String
            var currentSnapshot: String?
        }
        
        let testStruct = try JSONDecoder().decode(TestStruct.self, from: data)
        XCTAssertEqual(testStruct.version, "alpha")
        XCTAssertNil(testStruct.currentSnapshot)
        
        let formatter = ISO8601DateFormatter()
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.formatOptions = [
            .withInternetDateTime,
            .withDashSeparatorInDate,
            .withColonSeparatorInTime,
            .withTimeZone,
            .withFractionalSeconds
        ]
        XCTAssertNotNil(formatter.date(from: testStruct.modificationDate))
    }
    
    func testStoreCreatesOnlyIfNecessary() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        let dataBefore = try Data(contentsOf: temporaryStoreURL.appendingPathComponent("Info.json", isDirectory: false))
        // This second time should be a no-op and shouldn't throw
        try await persistence.createPersistenceIfNecessary()
        
        XCTAssertTrue(try temporaryStoreURL.checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Snapshots", isDirectory: true).checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Backups", isDirectory: true).checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Info.json", isDirectory: false).checkResourceIsReachable())
        
        let dataAfter = try Data(contentsOf: temporaryStoreURL.appendingPathComponent("Info.json", isDirectory: false))
        XCTAssertEqual(dataBefore, dataAfter)
    }
    
    func testStoreInfoAccessOrder() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let date1 = Date(timeIntervalSince1970: 0)
        let date2 = Date(timeIntervalSince1970: 10)
        
        let task1 = await persistence.updateStoreInfo { storeInfo in
            sleep(1)
            XCTAssertNotEqual(storeInfo.modificationDate, date2)
            storeInfo.modificationDate = date1
        }
        
        let task2 = await persistence.updateStoreInfo { storeInfo in
            XCTAssertEqual(storeInfo.modificationDate, date1)
            storeInfo.modificationDate = date2
        }
        
        try await task1.value
        try await task2.value
        
        let currentStoreInfo = await persistence.cachedStoreInfo
        XCTAssertEqual(currentStoreInfo?.modificationDate, date2)
    }
    
    func testAccessingStoreInfoAlsoCreatesPersistence() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        try await persistence.withStoreInfo { _ in }
        
        XCTAssertTrue(try temporaryStoreURL.checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Snapshots", isDirectory: true).checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Backups", isDirectory: true).checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Info.json", isDirectory: false).checkResourceIsReachable())
    }
    
    func testCallingStoreInfoResursively() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        try await persistence.withStoreInfo { storeInfoA in
            let originalStoreA = storeInfoA
            storeInfoA.modificationDate = Date(timeIntervalSince1970: 1)
            let modifiedStoreA = storeInfoA
            try await persistence.withStoreInfo { storeInfoB in
                XCTAssertEqual(originalStoreA, storeInfoB)
                XCTAssertNotEqual(modifiedStoreA, storeInfoB)
            }
        }
        
        try await persistence.withStoreInfo { storeInfoA in
            try await persistence.withStoreInfo { storeInfoB in
                // No change:
                storeInfoB.modificationDate = Date(timeIntervalSince1970: 1)
            }
        }
        
        do {
            try await persistence.withStoreInfo { storeInfoA in
                try await persistence.withStoreInfo { storeInfoB in
                    storeInfoB.modificationDate = Date(timeIntervalSince1970: 2)
                }
            }
            XCTFail("Reached code that shouldn't run")
        } catch {
            XCTAssertEqual(error as? DiskPersistenceInternalError, .nestedStoreWrite)
        }
        
    }
}
