//
//  DatastoreDescriptorTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-11.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class DatastoreDescriptorTests: XCTestCase {
    func testIndexDescriptorEquatable() throws {
        let desc1 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), name: "abc", type: "String")
        let desc2 = DatastoreDescriptor.IndexDescriptor(version: Data([0]), name: "abc", type: "String")
        let desc3 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), name: "a", type: "String")
        let desc4 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), name: "abc", type: "Int")
        
        XCTAssertEqual(desc1, desc1)
        XCTAssertNotEqual(desc1, desc2)
        XCTAssertNotEqual(desc1, desc3)
        XCTAssertNotEqual(desc1, desc4)
    }
    
    func testIndexDescriptorComparable() throws {
        let desc1 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), name: "abc", type: "String")
        let desc2 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), name: "abc", type: "Int")
        let desc3 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), name: "aac", type: "String")
        let desc4 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), name: "acc", type: "String")
        
        XCTAssertLessThan(desc3, desc1)
        XCTAssertLessThan(desc1, desc4)
        XCTAssertFalse(desc1 < desc2)
        XCTAssertFalse(desc2 < desc1)
    }
    
    func testTypeReflection() throws {
        enum SharedVersion: String, CaseIterable {
            case a, b, c
        }
        
        enum Enum: Codable, Comparable {
            case a, b, c
        }
        
        struct Nested: Codable {
            var a: Enum
        }
        
        struct SampleType: Codable {
            var id: UUID
            var a: String
            var b: Int
            var c: Nested
            var d: String { "\(a).\(b)" }
        }
        
        struct SampleFormatA: DatastoreFormat {
            static let defaultKey = DatastoreKey("sample")
            static let currentVersion = SharedVersion.a
            
            typealias Version = SharedVersion
            typealias Instance = SampleType
            typealias Identifier = UUID
            
            let id = OneToOneIndex(\.id)
            let a = Index(\.a)
            let b = Index(\.b)
        }
        
        let descA = try DatastoreDescriptor(
            format: SampleFormatA(),
            version: .a
        )
        XCTAssertEqual(descA.version, Data([34, 97, 34]))
        XCTAssertEqual(descA.instanceType, "SampleType")
        XCTAssertEqual(descA.identifierType, "UUID")
        XCTAssertEqual(descA.directIndexes, [:])
        XCTAssertEqual(descA.referenceIndexes, [
            "id" : .init(version: Data([34, 97, 34]), name: "id", type: "OneToOneIndex(UUID)"),
            "a" : .init(version: Data([34, 97, 34]), name: "a", type: "OneToManyIndex(String)"),
            "b" : .init(version: Data([34, 97, 34]), name: "b", type: "OneToManyIndex(Int)"),
        ])
        
        struct SampleFormatB: DatastoreFormat {
            static let defaultKey = DatastoreKey("sample")
            static let currentVersion = SharedVersion.a
            
            typealias Version = SharedVersion
            typealias Instance = SampleType
            typealias Identifier = UUID
            
            let id = OneToOneIndex(\.id)
            @Direct var a = Index(\.a)
            let b = Index(\.b)
        }
        
        let descB = try DatastoreDescriptor(
            format: SampleFormatB(),
            version: .a
        )
        XCTAssertEqual(descB.version, Data([34, 97, 34]))
        XCTAssertEqual(descB.instanceType, "SampleType")
        XCTAssertEqual(descB.identifierType, "UUID")
        XCTAssertEqual(descB.directIndexes, [
            "a" : .init(version: Data([34, 97, 34]), name: "a", type: "OneToManyIndex(String)"),
        ])
        XCTAssertEqual(descB.referenceIndexes, [
            "id" : .init(version: Data([34, 97, 34]), name: "id", type: "OneToOneIndex(UUID)"),
            "b" : .init(version: Data([34, 97, 34]), name: "b", type: "OneToManyIndex(Int)"),
        ])
        
        struct SampleFormatC: DatastoreFormat {
            static let defaultKey = DatastoreKey("sample")
            static let currentVersion = SharedVersion.a
            
            typealias Version = SharedVersion
            typealias Instance = SampleType
            typealias Identifier = UUID
            
            let id = OneToOneIndex(\.id)
            let a = Index(\.a)
            let b = Index(\.b)
            @Direct var c = Index(\.c.a)
        }
        
        let descC = try DatastoreDescriptor(
            format: SampleFormatC(),
            version: .a
        )
        XCTAssertEqual(descC.version, Data([34, 97, 34]))
        XCTAssertEqual(descC.instanceType, "SampleType")
        XCTAssertEqual(descC.identifierType, "UUID")
        XCTAssertEqual(descC.directIndexes, [
            "c" : .init(version: Data([34, 97, 34]), name: "c", type: "OneToManyIndex(Enum)"),
        ])
        XCTAssertEqual(descC.referenceIndexes, [
            "id" : .init(version: Data([34, 97, 34]), name: "id", type: "OneToOneIndex(UUID)"),
            "a" : .init(version: Data([34, 97, 34]), name: "a", type: "OneToManyIndex(String)"),
            "b" : .init(version: Data([34, 97, 34]), name: "b", type: "OneToManyIndex(Int)"),
        ])
        
        struct SampleFormatD: DatastoreFormat {
            static let defaultKey = DatastoreKey("sample")
            static let currentVersion = SharedVersion.a
            
            typealias Version = SharedVersion
            typealias Instance = SampleType
            typealias Identifier = UUID
            
            @Direct var id = OneToOneIndex(\.id)
            @Direct var a = Index(\.a)
            @Direct var b = Index(\.b)
            @Direct var c = Index(\.c.a)
        }
        
        let descD = try DatastoreDescriptor(
            format: SampleFormatD(),
            version: .a
        )
        XCTAssertEqual(descD.version, Data([34, 97, 34]))
        XCTAssertEqual(descD.instanceType, "SampleType")
        XCTAssertEqual(descD.identifierType, "UUID")
        XCTAssertEqual(descD.directIndexes, [
            "id" : .init(version: Data([34, 97, 34]), name: "id", type: "OneToOneIndex(UUID)"),
            "a" : .init(version: Data([34, 97, 34]), name: "a", type: "OneToManyIndex(String)"),
            "b" : .init(version: Data([34, 97, 34]), name: "b", type: "OneToManyIndex(Int)"),
            "c" : .init(version: Data([34, 97, 34]), name: "c", type: "OneToManyIndex(Enum)"),
        ])
        XCTAssertEqual(descD.referenceIndexes, [:])
    }
    
    func testTypeIdentifiableReflection() throws {
        enum SharedVersion: String, CaseIterable {
            case a, b, c
        }
        
        enum Enum: Codable, Comparable {
            case a, b, c
        }
        
        struct Nested: Codable {
            var a: Enum
        }
        
        struct SampleType: Codable, Identifiable {
            var id: UUID
            var a: String
            var b: Int
            var c: Nested
            var d: String { "\(a).\(b)" }
        }
        
        struct SampleFormatA: DatastoreFormat {
            static let defaultKey = DatastoreKey("sample")
            static let currentVersion = SharedVersion.a
            
            typealias Version = SharedVersion
            typealias Instance = SampleType
            
            let id = OneToOneIndex(\.id)
            let a = Index(\.a)
            let b = Index(\.b)
        }
        
        let descA = try DatastoreDescriptor(
            format: SampleFormatA(),
            version: .a
        )
        XCTAssertEqual(descA.version, Data([34, 97, 34]))
        XCTAssertEqual(descA.instanceType, "SampleType")
        XCTAssertEqual(descA.identifierType, "UUID")
        XCTAssertEqual(descA.directIndexes, [:])
        XCTAssertEqual(descA.referenceIndexes, [
            "a" : .init(version: Data([34, 97, 34]), name: "a", type: "OneToManyIndex(String)"),
            "b" : .init(version: Data([34, 97, 34]), name: "b", type: "OneToManyIndex(Int)"),
        ])
        
        struct SampleFormatB: DatastoreFormat {
            static let defaultKey = DatastoreKey("sample")
            static let currentVersion = SharedVersion.a
            
            typealias Version = SharedVersion
            typealias Instance = SampleType
            
            let id = OneToOneIndex(\.id)
            @Direct var a = Index(\.a)
            let b = Index(\.b)
        }
        
        let descB = try DatastoreDescriptor(
            format: SampleFormatB(),
            version: .a
        )
        XCTAssertEqual(descB.version, Data([34, 97, 34]))
        XCTAssertEqual(descB.instanceType, "SampleType")
        XCTAssertEqual(descB.identifierType, "UUID")
        XCTAssertEqual(descB.directIndexes, [
            "a" : .init(version: Data([34, 97, 34]), name: "a", type: "OneToManyIndex(String)"),
        ])
        XCTAssertEqual(descB.referenceIndexes, [
            "b" : .init(version: Data([34, 97, 34]), name: "b", type: "OneToManyIndex(Int)"),
        ])
        
        struct SampleFormatC: DatastoreFormat {
            static let defaultKey = DatastoreKey("sample")
            static let currentVersion = SharedVersion.a
            
            typealias Version = SharedVersion
            typealias Instance = SampleType
            
            let id = OneToOneIndex(\.id)
            let a = Index(\.a)
            let b = Index(\.b)
            @Direct var c = Index(\.c.a)
        }
        
        let descC = try DatastoreDescriptor(
            format: SampleFormatC(),
            version: .a
        )
        XCTAssertEqual(descC.version, Data([34, 97, 34]))
        XCTAssertEqual(descC.instanceType, "SampleType")
        XCTAssertEqual(descC.identifierType, "UUID")
        XCTAssertEqual(descC.directIndexes, [
            "c" : .init(version: Data([34, 97, 34]), name: "c", type: "OneToManyIndex(Enum)"),
        ])
        XCTAssertEqual(descC.referenceIndexes, [
            "a" : .init(version: Data([34, 97, 34]), name: "a", type: "OneToManyIndex(String)"),
            "b" : .init(version: Data([34, 97, 34]), name: "b", type: "OneToManyIndex(Int)"),
        ])
        
        struct SampleFormatD: DatastoreFormat {
            static let defaultKey = DatastoreKey("sample")
            static let currentVersion = SharedVersion.a
            
            typealias Version = SharedVersion
            typealias Instance = SampleType
            
            @Direct var id = OneToOneIndex(\.id)
            @Direct var a = Index(\.a)
            @Direct var b = Index(\.b)
            @Direct var c = Index(\.c.a)
        }
        
        let descD = try DatastoreDescriptor(
            format: SampleFormatD(),
            version: .a
        )
        XCTAssertEqual(descD.version, Data([34, 97, 34]))
        XCTAssertEqual(descD.instanceType, "SampleType")
        XCTAssertEqual(descD.identifierType, "UUID")
        XCTAssertEqual(descD.directIndexes, [
            "a" : .init(version: Data([34, 97, 34]), name: "a", type: "OneToManyIndex(String)"),
            "b" : .init(version: Data([34, 97, 34]), name: "b", type: "OneToManyIndex(Int)"),
            "c" : .init(version: Data([34, 97, 34]), name: "c", type: "OneToManyIndex(Enum)"),
        ])
        XCTAssertEqual(descD.referenceIndexes, [:])
    }
    
    func testTypeDuplicatePaths() throws {
        enum SharedVersion: String, CaseIterable {
            case a, b, c
        }
        
        enum Enum: Codable, Comparable {
            case a, b, c
        }
        
        struct Nested: Codable {
            var a: Enum
        }
        
        struct SampleType: Codable {
            var id: UUID
            var a: String
            var b: Int
            var c: Nested
            var d: String { "\(a).\(b)" }
        }
        
        struct SampleFormatA: DatastoreFormat {
            static let defaultKey = DatastoreKey("sample")
            static let currentVersion = SharedVersion.a
            
            typealias Version = SharedVersion
            typealias Instance = SampleType
            typealias Identifier = UUID
            
            let id = OneToOneIndex(\.id)
            let a = Index(\.a)
            let b = Index(\.b)
            @Direct var otherB = Index(\.b)
        }
        
        let descA = try DatastoreDescriptor(
            format: SampleFormatA(),
            version: .a
        )
        XCTAssertEqual(descA.version, Data([34, 97, 34]))
        XCTAssertEqual(descA.instanceType, "SampleType")
        XCTAssertEqual(descA.identifierType, "UUID")
        XCTAssertEqual(descA.directIndexes, [:])
        XCTAssertEqual(descA.referenceIndexes, [
            "id" : .init(version: Data([34, 97, 34]), name: "id", type: "OneToOneIndex(UUID)"),
            "a" : .init(version: Data([34, 97, 34]), name: "a", type: "OneToManyIndex(String)"),
            "b" : .init(version: Data([34, 97, 34]), name: "b", type: "OneToManyIndex(Int)"),
        ])
    }
}
