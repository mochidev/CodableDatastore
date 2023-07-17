//
//  SnapshotTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class SnapshotTests: XCTestCase {
    var temporaryStoreURL: URL = FileManager.default.temporaryDirectory
    
    override func setUp() async throws {
        temporaryStoreURL = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString, isDirectory: true);
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: temporaryStoreURL)
    }
    
    func testSnapshotIdentifiers() throws {
        XCTAssertEqual(SnapshotIdentifier(date: Date(timeIntervalSince1970: 0), token: 0), SnapshotIdentifier(rawValue: "1970-01-01 00-00-00-000 0000000000000000"))
        XCTAssertEqual(SnapshotIdentifier(rawValue: "1970-01-01 00-00-00-000 0000000000000000").rawValue, "1970-01-01 00-00-00-000 0000000000000000")
        
        XCTAssertEqual(SnapshotIdentifier(date: Date(timeIntervalSince1970: 0), token: 0x0123456789abcdef), SnapshotIdentifier(rawValue: "1970-01-01 00-00-00-000 0123456789ABCDEF"))
        XCTAssertEqual(SnapshotIdentifier(rawValue: "1970-01-01 00-00-00-000 0123456789ABCDEF").rawValue, "1970-01-01 00-00-00-000 0123456789ABCDEF")
        
        let components = try SnapshotIdentifier.mockIdentifier.components
        XCTAssertEqual(components.year, "1970")
        XCTAssertEqual(components.month, "01")
        XCTAssertEqual(components.day, "02")
        XCTAssertEqual(components.hour, "03")
        XCTAssertEqual(components.minute, "04")
        XCTAssertEqual(components.second, "05")
        XCTAssertEqual(components.millisecond, "678")
        XCTAssertEqual(components.token, "0123456789ABCDEF")
        XCTAssertEqual(components.monthDay, "01-02")
        XCTAssertEqual(components.hourMinute, "03-04")
        
        XCTAssertEqual(SnapshotIdentifier(rawValue: "semi-valid").rawValue, "semi-valid")
        XCTAssertThrowsError(try SnapshotIdentifier(rawValue: "semi-valid").components) { error in
            XCTAssertEqual(error as? DatedIdentifierError, .invalidLength)
        }
    }
    
    func testNoFileCreatedOnInit() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        try await persistence.createPersistenceIfNecessary()
        
        let isEmpty = await persistence.snapshots.isEmpty
        XCTAssertTrue(isEmpty)
        
        let snapshot = Snapshot(id: SnapshotIdentifier.mockIdentifier, persistence: persistence, isBackup: false)
        let snapshotURL = snapshot.snapshotURL
        XCTAssertEqual(snapshotURL.absoluteString, temporaryStoreURL.absoluteString.appending("Snapshots/1970/01-02/03-04/1970-01-02%2003-04-05-678%200123456789ABCDEF.snapshot/"))
        XCTAssertThrowsError(try snapshotURL.checkResourceIsReachable())
    }
    
    func testManifestCreatedWhenAsked() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        
        let snapshot = Snapshot(id: SnapshotIdentifier.mockIdentifier, persistence: persistence, isBackup: false)
        
        try await snapshot.updatingManifest { _, _ in }
        
        let snapshotURL = snapshot.snapshotURL
        XCTAssertEqual(snapshotURL.absoluteString, temporaryStoreURL.absoluteString.appending("Snapshots/1970/01-02/03-04/1970-01-02%2003-04-05-678%200123456789ABCDEF.snapshot/"))
        
        XCTAssertTrue(try snapshotURL.checkResourceIsReachable())
        XCTAssertTrue(try snapshotURL.appendingPathComponent("Inbox", isDirectory: true).checkResourceIsReachable())
        XCTAssertTrue(try snapshotURL.appendingPathComponent("Datastores", isDirectory: true).checkResourceIsReachable())
        XCTAssertTrue(try snapshotURL.appendingPathComponent("Manifest.json", isDirectory: false).checkResourceIsReachable())
    }
    
    func testStoreNotCreatedWithManifest() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        let snapshot = Snapshot(id: SnapshotIdentifier.mockIdentifier, persistence: persistence, isBackup: false)
        try await snapshot.updatingManifest { _, _ in }
        
        XCTAssertTrue(try temporaryStoreURL.checkResourceIsReachable())
        XCTAssertTrue(try temporaryStoreURL.appendingPathComponent("Snapshots", isDirectory: true).checkResourceIsReachable())
        XCTAssertThrowsError(try temporaryStoreURL.appendingPathComponent("Backups", isDirectory: true).checkResourceIsReachable())
        XCTAssertThrowsError(try temporaryStoreURL.appendingPathComponent("Info.json", isDirectory: false).checkResourceIsReachable())
    }
    
    func testManifestOnEmptySnapshot() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        let snapshot = Snapshot(id: SnapshotIdentifier.mockIdentifier, persistence: persistence, isBackup: false)
        try await snapshot.updatingManifest { _, _ in }
        let snapshotURL = snapshot.snapshotURL
        
        let data = try Data(contentsOf: snapshotURL.appendingPathComponent("Manifest.json", isDirectory: false))
        
        struct TestStruct: Codable {
            var version: String
            var id: String
            var modificationDate: String
            var currentIteration: String?
        }
        
        let testStruct = try JSONDecoder().decode(TestStruct.self, from: data)
        XCTAssertEqual(testStruct.version, "alpha")
        XCTAssertNotNil(testStruct.currentIteration)
        XCTAssertEqual(testStruct.id, "1970-01-02 03-04-05-678 0123456789ABCDEF")
        
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
    
    func testSnapshotCreatesOnlyIfNecessary() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        let snapshot = Snapshot(id: SnapshotIdentifier.mockIdentifier, persistence: persistence, isBackup: false)
        try await snapshot.updatingManifest { _, _ in }
        let snapshotURL = snapshot.snapshotURL
        
        let dataBefore = try Data(contentsOf: snapshotURL.appendingPathComponent("Manifest.json", isDirectory: false))
        // This second time should be a no-op and shouldn't throw
        try await snapshot.updatingManifest { _, _ in }
        
        let dataAfter = try Data(contentsOf: snapshotURL.appendingPathComponent("Manifest.json", isDirectory: false))
        XCTAssertEqual(dataBefore, dataAfter)
    }
    
    func testManifestAccessOrder() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        let snapshot = Snapshot(id: SnapshotIdentifier.mockIdentifier, persistence: persistence, isBackup: false)
        try await snapshot.updatingManifest { _, _  in }
        
        let date1 = Date(timeIntervalSince1970: 0)
        let date2 = Date(timeIntervalSince1970: 10)
        
        let task1 = await snapshot.updateManifest { manifest, _ in
            sleep(1)
            XCTAssertNotEqual(manifest.modificationDate, date2)
            manifest.modificationDate = date1
        }
        
        let task2 = await snapshot.updateManifest { manifest, _ in
            XCTAssertEqual(manifest.modificationDate, date1)
            manifest.modificationDate = date2
        }
        
        try await task1.value
        try await task2.value
        
        let currentManifest = await snapshot.cachedManifest
        XCTAssertEqual(currentManifest?.modificationDate, date2)
    }
    
    func testCallingManifestResursively() async throws {
        let persistence = try DiskPersistence(readWriteURL: temporaryStoreURL)
        let snapshot = Snapshot(id: SnapshotIdentifier.mockIdentifier, persistence: persistence, isBackup: false)
        try await snapshot.updatingManifest { _, _ in }
        
        try await snapshot.updatingManifest { manifestA, _ in
            let originalManifestA = manifestA
            manifestA.modificationDate = Date(timeIntervalSince1970: 1)
            let modifiedManifestA = manifestA
            try await snapshot.updatingManifest { manifestB, _ in
                XCTAssertEqual(originalManifestA, manifestB)
                XCTAssertNotEqual(modifiedManifestA, manifestB)
            }
        }
        
        try await snapshot.updatingManifest { manifestA, _ in
            try await snapshot.updatingManifest { manifestB, _ in
                // No change:
                manifestB.modificationDate = Date(timeIntervalSince1970: 1)
            }
        }
        
        do {
            try await snapshot.updatingManifest { manifestA, _ in
                try await snapshot.updatingManifest { manifestB, _ in
                    manifestB.modificationDate = Date(timeIntervalSince1970: 2)
                }
            }
            XCTFail("Reached code that shouldn't run")
        } catch {
            XCTAssertEqual(error as? DiskPersistenceInternalError, .nestedSnapshotWrite)
        }
        
    }
}
