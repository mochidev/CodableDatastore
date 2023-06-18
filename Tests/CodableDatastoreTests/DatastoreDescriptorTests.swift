//
//  DatastoreDescriptorTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-11.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class DatastoreDescriptorTests: XCTestCase {
    func testIndexDescriptorEquatable() throws {
        let desc1 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), key: "abc", indexType: "String")
        let desc2 = DatastoreDescriptor.IndexDescriptor(version: Data([0]), key: "abc", indexType: "String")
        let desc3 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), key: "a", indexType: "String")
        let desc4 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), key: "abc", indexType: "Int")
        
        XCTAssertEqual(desc1, desc1)
        XCTAssertNotEqual(desc1, desc2)
        XCTAssertNotEqual(desc1, desc3)
        XCTAssertNotEqual(desc1, desc4)
    }
    
    func testIndexDescriptorComparable() throws {
        let desc1 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), key: "abc", indexType: "String")
        let desc2 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), key: "abc", indexType: "Int")
        let desc3 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), key: "aac", indexType: "String")
        let desc4 = DatastoreDescriptor.IndexDescriptor(version: Data([0, 1, 2]), key: "acc", indexType: "String")
        
        XCTAssertLessThan(desc3, desc1)
        XCTAssertLessThan(desc1, desc4)
        XCTAssertFalse(desc1 < desc2)
        XCTAssertFalse(desc2 < desc1)
    }
    
    func testIndexDescriptorReflection() throws {
        enum Enum: Codable, Comparable {
            case a, b, c
        }
        
        struct Nested: Codable {
            @Indexed
            var a: Enum
        }
        
        struct SampleType: Codable {
            @Indexed
            var a: String
            
            @Indexed
            var b: Int
            
            var c: Nested
            
            var d: _AnyIndexed { Indexed(wrappedValue: "\(a).\(b)").projectedValue }
        }
        
        let sample = SampleType(a: "A", b: 1, c: Nested(a: .b))
        
        let descA = DatastoreDescriptor.IndexDescriptor(version: Data([0]), sampleInstance: sample, indexPath: IndexPath(uncheckedKeyPath: \.$a, path: "$a"))
        XCTAssertEqual(descA.version, Data([0]))
        XCTAssertEqual(descA.key, "$a")
        XCTAssertEqual(descA.indexType, "String")
        
        let descB = DatastoreDescriptor.IndexDescriptor(version: Data([0]), sampleInstance: sample, indexPath: IndexPath(uncheckedKeyPath: \.$b, path: "$b"))
        XCTAssertEqual(descB.version, Data([0]))
        XCTAssertEqual(descB.key, "$b")
        XCTAssertEqual(descB.indexType, "Int")
        
        let descC = DatastoreDescriptor.IndexDescriptor(version: Data([0]), sampleInstance: sample, indexPath: IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a"))
        XCTAssertEqual(descC.version, Data([0]))
        XCTAssertEqual(descC.key, "c.$a")
        XCTAssertEqual(descC.indexType, "Enum")
        
        let descD = DatastoreDescriptor.IndexDescriptor(version: Data([0]), sampleInstance: sample, indexPath: IndexPath(uncheckedKeyPath: \.d, path: "d"))
        XCTAssertEqual(descD.version, Data([0]))
        XCTAssertEqual(descD.key, "d")
        XCTAssertEqual(descD.indexType, "String")
    }
    
    func testTypeReflection() throws {
        enum Version: String, CaseIterable {
            case a, b, c
        }
        
        enum Enum: Codable, Comparable {
            case a, b, c
        }
        
        struct Nested: Codable {
            @Indexed
            var a: Enum
        }
        
        struct SampleType: Codable {
            @Indexed
            var id: UUID
            
            @Indexed
            var a: String
            
            @Indexed
            var b: Int
            
            var c: Nested
            
            var d: _AnyIndexed { Indexed(wrappedValue: "\(a).\(b)").projectedValue }
        }
        
        let sample = SampleType(id: UUID(), a: "A", b: 1, c: Nested(a: .b))
        
        let descA = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [],
            computedIndexes: []
        )
        XCTAssertEqual(descA.version, Data([34, 97, 34]))
        XCTAssertEqual(descA.codedType, "SampleType")
        XCTAssertEqual(descA.identifierType, "UUID")
        XCTAssertEqual(descA.directIndexes, [:])
        XCTAssertEqual(descA.secondaryIndexes, [
            "$id" : .init(version: Data([34, 97, 34]), key: "$id", indexType: "UUID"),
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
        ])
        
        let descB = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [IndexPath(uncheckedKeyPath: \.$a, path: "$a")],
            computedIndexes: []
        )
        XCTAssertEqual(descB.version, Data([34, 97, 34]))
        XCTAssertEqual(descB.codedType, "SampleType")
        XCTAssertEqual(descB.identifierType, "UUID")
        XCTAssertEqual(descB.directIndexes, [
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
        ])
        XCTAssertEqual(descB.secondaryIndexes, [
            "$id" : .init(version: Data([34, 97, 34]), key: "$id", indexType: "UUID"),
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
        ])
        
        let descC = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a")],
            computedIndexes: []
        )
        XCTAssertEqual(descC.version, Data([34, 97, 34]))
        XCTAssertEqual(descC.codedType, "SampleType")
        XCTAssertEqual(descC.identifierType, "UUID")
        XCTAssertEqual(descC.directIndexes, [
            "c.$a" : .init(version: Data([34, 97, 34]), key: "c.$a", indexType: "Enum"),
        ])
        XCTAssertEqual(descC.secondaryIndexes, [
            "$id" : .init(version: Data([34, 97, 34]), key: "$id", indexType: "UUID"),
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
        ])
        
        let descD = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a")],
            computedIndexes: [
                IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a"),
                IndexPath(uncheckedKeyPath: \.$b, path: "$b")
            ]
        )
        XCTAssertEqual(descD.version, Data([34, 97, 34]))
        XCTAssertEqual(descD.codedType, "SampleType")
        XCTAssertEqual(descD.identifierType, "UUID")
        XCTAssertEqual(descD.directIndexes, [
            "c.$a" : .init(version: Data([34, 97, 34]), key: "c.$a", indexType: "Enum"),
        ])
        XCTAssertEqual(descD.secondaryIndexes, [
            "$id" : .init(version: Data([34, 97, 34]), key: "$id", indexType: "UUID"),
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
        ])
        
        let descE = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [
                IndexPath(uncheckedKeyPath: \.$id, path: "$id"),
                IndexPath(uncheckedKeyPath: \.$a, path: "$a"),
                IndexPath(uncheckedKeyPath: \.$b, path: "$b"),
                IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a")
            ],
            computedIndexes: [
                IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a"),
                IndexPath(uncheckedKeyPath: \.$b, path: "$b")
            ]
        )
        XCTAssertEqual(descE.version, Data([34, 97, 34]))
        XCTAssertEqual(descE.codedType, "SampleType")
        XCTAssertEqual(descE.identifierType, "UUID")
        XCTAssertEqual(descE.directIndexes, [
            "$id" : .init(version: Data([34, 97, 34]), key: "$id", indexType: "UUID"),
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
            "c.$a" : .init(version: Data([34, 97, 34]), key: "c.$a", indexType: "Enum"),
        ])
        XCTAssertEqual(descE.secondaryIndexes, [:])
    }
    
    func testTypeIdentifiableReflection() throws {
        enum Version: String, CaseIterable {
            case a, b, c
        }
        
        enum Enum: Codable, Comparable {
            case a, b, c
        }
        
        struct Nested: Codable {
            @Indexed
            var a: Enum
        }
        
        struct SampleType: Codable, Identifiable {
            @Indexed
            var id: UUID
            
            @Indexed
            var a: String
            
            @Indexed
            var b: Int
            
            var c: Nested
            
            var d: _AnyIndexed { Indexed(wrappedValue: "\(a).\(b)").projectedValue }
        }
        
        let sample = SampleType(id: UUID(), a: "A", b: 1, c: Nested(a: .b))
        
        let descA = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [],
            computedIndexes: []
        )
        XCTAssertEqual(descA.version, Data([34, 97, 34]))
        XCTAssertEqual(descA.codedType, "SampleType")
        XCTAssertEqual(descA.identifierType, "UUID")
        XCTAssertEqual(descA.directIndexes, [:])
        XCTAssertEqual(descA.secondaryIndexes, [
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
        ])
        
        let descB = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [
                IndexPath(uncheckedKeyPath: \.$a, path: "$a")
            ],
            computedIndexes: []
        )
        XCTAssertEqual(descB.version, Data([34, 97, 34]))
        XCTAssertEqual(descB.codedType, "SampleType")
        XCTAssertEqual(descB.identifierType, "UUID")
        XCTAssertEqual(descB.directIndexes, [
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
        ])
        XCTAssertEqual(descB.secondaryIndexes, [
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
        ])
        
        let descC = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [
                IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a")
            ],
            computedIndexes: []
        )
        XCTAssertEqual(descC.version, Data([34, 97, 34]))
        XCTAssertEqual(descC.codedType, "SampleType")
        XCTAssertEqual(descC.identifierType, "UUID")
        XCTAssertEqual(descC.directIndexes, [
            "c.$a" : .init(version: Data([34, 97, 34]), key: "c.$a", indexType: "Enum"),
        ])
        XCTAssertEqual(descC.secondaryIndexes, [
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
        ])
        
        let descD = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [
                IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a")
            ],
            computedIndexes: [
                IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a"),
                IndexPath(uncheckedKeyPath: \.$b, path: "$b")
            ]
        )
        XCTAssertEqual(descD.version, Data([34, 97, 34]))
        XCTAssertEqual(descD.codedType, "SampleType")
        XCTAssertEqual(descD.identifierType, "UUID")
        XCTAssertEqual(descD.directIndexes, [
            "c.$a" : .init(version: Data([34, 97, 34]), key: "c.$a", indexType: "Enum"),
        ])
        XCTAssertEqual(descD.secondaryIndexes, [
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
        ])
        
        let descE = try DatastoreDescriptor(
            version: Version.a,
            sampleInstance: sample,
            identifierType: UUID.self,
            directIndexes: [
                IndexPath(uncheckedKeyPath: \.$id, path: "$id"),
                IndexPath(uncheckedKeyPath: \.$a, path: "$a"),
                IndexPath(uncheckedKeyPath: \.$b, path: "$b"),
                IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a")
            ],
            computedIndexes: [
                IndexPath(uncheckedKeyPath: \.c.$a, path: "c.$a"),
                IndexPath(uncheckedKeyPath: \.$b, path: "$b")
            ]
        )
        XCTAssertEqual(descE.version, Data([34, 97, 34]))
        XCTAssertEqual(descE.codedType, "SampleType")
        XCTAssertEqual(descE.identifierType, "UUID")
        XCTAssertEqual(descE.directIndexes, [
            "$a" : .init(version: Data([34, 97, 34]), key: "$a", indexType: "String"),
            "$b" : .init(version: Data([34, 97, 34]), key: "$b", indexType: "Int"),
            "c.$a" : .init(version: Data([34, 97, 34]), key: "c.$a", indexType: "Enum"),
        ])
        XCTAssertEqual(descE.secondaryIndexes, [:])
    }
}
