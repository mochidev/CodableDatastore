//
//  DatastoreFormatTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-04-12.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class DatastoreFormatTests: XCTestCase {
    func testDatastoreFormatAccessors() throws {
        struct NonCodable {}
        
        struct TestFormat: DatastoreFormat {
            static var defaultKey = DatastoreKey("sample")
            static var currentVersion = Version.a
            
            enum Version: String, CaseIterable {
                case a, b, c
            }
            
            struct Instance: Codable, Identifiable {
                let id: UUID
                var name: String
                var age: Int
                var other: [Int]
//                var nonCodable: NonCodable // Not allowed: Type 'TestFormat.Instance' does not conform to protocol 'Codable'
                var composed: String { "\(name) \(age)"}
            }
            
            let name = Index(\.name)
            let age = Index(\.age)
//            let other = Index(\.other) // Not allowed: Generic struct 'OneToManyIndexRepresentation' requires that '[Int]' conform to 'Comparable'
            let other = ManyToManyIndex(\.other)
            let composed = Index(\.composed)
        }
        
        let myValue = TestFormat.Instance(id: UUID(), name: "Hello!", age: 1, other: [2, 6])
        
        XCTAssertEqual(myValue[index: TestFormat().age], [1])
        XCTAssertEqual(myValue[index: TestFormat().name], ["Hello!"])
        XCTAssertEqual(myValue[index: TestFormat().other], [2, 6])
        XCTAssertEqual(myValue[index: TestFormat().composed], ["Hello! 1"])
    }
}
