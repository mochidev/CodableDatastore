//
//  FileManagerTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-10-14.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

#if !canImport(Darwin)
@preconcurrency import Foundation
#endif
import XCTest
@testable import CodableDatastore

final class FileManagerTests: XCTestCase, @unchecked Sendable {
    var temporaryStoreURL: URL = FileManager.default.temporaryDirectory
    
    override func setUp() async throws {
        temporaryStoreURL = FileManager.default.temporaryDirectory.appendingPathComponent(ProcessInfo.processInfo.globallyUniqueString, isDirectory: true);
    }
    
    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: temporaryStoreURL)
    }
    
    func testDirectoryWithFileLeftInPlace() async throws {
        let fileManager = FileManager()
        try fileManager.createDirectory(at: temporaryStoreURL, withIntermediateDirectories: true)
        try "".write(to: temporaryStoreURL.appendingPathComponent("Temp.txt", isDirectory: false), atomically: false, encoding: .utf8)
        
        let directoryURL = temporaryStoreURL.appendingPathComponent("Directory", isDirectory: true)
        try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        try "".write(to: directoryURL.appendingPathComponent("Temp.txt", isDirectory: false), atomically: false, encoding: .utf8)
        
        try fileManager.removeDirectoryIfEmpty(url: directoryURL, recursivelyRemoveParents: false)
        
        var isDirectory: ObjCBool = false
        XCTAssertTrue(fileManager.fileExists(atPath: directoryURL.standardizedFileURL.path, isDirectory: &isDirectory))
        XCTAssertTrue(isDirectory.boolValue)
    }
    
    func testDirectoryWithDirectoryLeftInPlace() async throws {
        let fileManager = FileManager()
        try fileManager.createDirectory(at: temporaryStoreURL, withIntermediateDirectories: true)
        try "".write(to: temporaryStoreURL.appendingPathComponent("Temp.txt", isDirectory: false), atomically: false, encoding: .utf8)
        
        let directoryURL = temporaryStoreURL.appendingPathComponent("Directory", isDirectory: true)
        try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        try fileManager.createDirectory(at: directoryURL.appendingPathComponent("SubDirectory", isDirectory: true), withIntermediateDirectories: true)
        
        try fileManager.removeDirectoryIfEmpty(url: directoryURL, recursivelyRemoveParents: false)
        
        var isDirectory: ObjCBool = false
        XCTAssertTrue(fileManager.fileExists(atPath: directoryURL.standardizedFileURL.path, isDirectory: &isDirectory))
        XCTAssertTrue(isDirectory.boolValue)
    }
    
    func testDirectoryWithFileAndDSStoreLeftInPlace() async throws {
        let fileManager = FileManager()
        try fileManager.createDirectory(at: temporaryStoreURL, withIntermediateDirectories: true)
        try "".write(to: temporaryStoreURL.appendingPathComponent("Temp.txt", isDirectory: false), atomically: false, encoding: .utf8)
        
        let directoryURL = temporaryStoreURL.appendingPathComponent("Directory", isDirectory: true)
        try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        try "".write(to: directoryURL.appendingPathComponent("Temp.txt", isDirectory: false), atomically: false, encoding: .utf8)
        try "".write(to: directoryURL.appendingPathComponent(".DS_Store", isDirectory: false), atomically: false, encoding: .utf8)
        
        try fileManager.removeDirectoryIfEmpty(url: directoryURL, recursivelyRemoveParents: false)
        
        var isDirectory: ObjCBool = false
        XCTAssertTrue(fileManager.fileExists(atPath: directoryURL.standardizedFileURL.path, isDirectory: &isDirectory))
        XCTAssertTrue(isDirectory.boolValue)
    }
    
    func testDirectoryWithDSStoreRemoved() async throws {
        let fileManager = FileManager()
        try fileManager.createDirectory(at: temporaryStoreURL, withIntermediateDirectories: true)
        try "".write(to: temporaryStoreURL.appendingPathComponent("Temp.txt", isDirectory: false), atomically: false, encoding: .utf8)
        
        let directoryURL = temporaryStoreURL.appendingPathComponent("Directory", isDirectory: true)
        try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        try "".write(to: directoryURL.appendingPathComponent(".DS_Store", isDirectory: false), atomically: false, encoding: .utf8)
        
        try fileManager.removeDirectoryIfEmpty(url: directoryURL, recursivelyRemoveParents: false)
        
        var isDirectory: ObjCBool = false
        XCTAssertFalse(fileManager.fileExists(atPath: directoryURL.standardizedFileURL.path, isDirectory: &isDirectory))
        XCTAssertTrue(fileManager.fileExists(atPath: temporaryStoreURL.standardizedFileURL.path, isDirectory: &isDirectory))
        XCTAssertTrue(isDirectory.boolValue)
    }
    
    func testEmptySubDirectoryOnlyRemoved() async throws {
        let fileManager = FileManager()
        try fileManager.createDirectory(at: temporaryStoreURL, withIntermediateDirectories: true)
        try "".write(to: temporaryStoreURL.appendingPathComponent("Temp.txt", isDirectory: false), atomically: false, encoding: .utf8)
        
        let directoryURL = temporaryStoreURL.appendingPathComponent("Directory", isDirectory: true)
        try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        let subDirectoryURL = directoryURL.appendingPathComponent("SubDirectory", isDirectory: true)
        try fileManager.createDirectory(at: subDirectoryURL, withIntermediateDirectories: true)
        
        try fileManager.removeDirectoryIfEmpty(url: subDirectoryURL, recursivelyRemoveParents: false)
        
        var isDirectory: ObjCBool = false
        XCTAssertFalse(fileManager.fileExists(atPath: subDirectoryURL.standardizedFileURL.path, isDirectory: &isDirectory))
        XCTAssertTrue(fileManager.fileExists(atPath: directoryURL.standardizedFileURL.path, isDirectory: &isDirectory))
        XCTAssertTrue(isDirectory.boolValue)
    }
    
    func testEmptySubDirectoryRecursivelyRemoved() async throws {
        let fileManager = FileManager()
        try fileManager.createDirectory(at: temporaryStoreURL, withIntermediateDirectories: true)
        try "".write(to: temporaryStoreURL.appendingPathComponent("Temp.txt", isDirectory: false), atomically: false, encoding: .utf8)
        
        let directoryURL = temporaryStoreURL.appendingPathComponent("Directory", isDirectory: true)
        try fileManager.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        let subDirectoryURL = directoryURL.appendingPathComponent("SubDirectory", isDirectory: true)
        try fileManager.createDirectory(at: subDirectoryURL, withIntermediateDirectories: true)
        
        try fileManager.removeDirectoryIfEmpty(url: subDirectoryURL, recursivelyRemoveParents: true)
        
        var isDirectory: ObjCBool = false
        XCTAssertFalse(fileManager.fileExists(atPath: directoryURL.standardizedFileURL.path, isDirectory: &isDirectory))
        XCTAssertTrue(fileManager.fileExists(atPath: temporaryStoreURL.standardizedFileURL.path, isDirectory: &isDirectory))
        XCTAssertTrue(isDirectory.boolValue)
    }
}
