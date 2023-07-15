//
//  IndexedTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-31.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class IndexedTests: XCTestCase {
    func testIndexed() throws {
        struct NonCodable {}
        
        struct TestStruct: Identifiable, Codable {
            var id: UUID
            
            @Indexed
            var name: String = ""
            
            @Indexed
            var age: Int = 1
            
            var other: [Int] = []
            
//            @Indexed
//            var nonCodable = NonCodable() // Not allowed!
            
            // Technically possible, but heavily discouraged:
            var composed: _AnyIndexed { Indexed(wrappedValue: "\(name) \(age)").projectedValue }
        }
        
        let myValue = TestStruct(id: UUID(), name: "Hello!")
        
        XCTAssertEqual("\(myValue[keyPath: \.age])", "1")
//        XCTAssertEqual("\(myValue[keyPath: \.$age])", "Indexed<Int>(wrappedValue: 1)")
        XCTAssertEqual("\(myValue[keyPath: \.composed])", #"CodableDatastore._SomeIndexed<Swift.String>"#)
        
        // This did not work unfortunately:
//        withUnsafeTemporaryAllocation(of: TestStruct.self, capacity: 1) { pointer in
////            print(Mirror(reflecting: pointer).children)
//            let value = pointer.first!
//
//            let mirror = Mirror(reflecting: value)
//            var indexedProperties: [String] = []
//            for child in mirror.children {
//                guard let label = child.label else { continue }
//                let childType = type(of: child.value)
//                guard childType is _IndexedProtocol.Type else { continue }
//                print("Child: \(label), type: \(childType)")
//                indexedProperties.append(label)
//            }
//            print("Indexable Children from type: \(indexedProperties)")
//        }
        
        
//        let mirror = Mirror(reflecting: TestStruct.self) // Doesn't work :(
        let mirror = Mirror(reflecting: myValue)
        var indexedProperties: [String] = []
        for child in mirror.children {
            guard let label = child.label else { continue }
            let childType = type(of: child.value)
            guard childType is any _IndexedProtocol.Type else { continue }
            indexedProperties.append(label)
        }
        XCTAssertEqual(indexedProperties, ["_name", "_age"])
        
        struct TestAccessor<T> {
            func load<V: _AnyIndexed>(from keypath: KeyPath<T, V>) -> [T] {
                XCTAssertEqual(keypath, \TestStruct.$age)
                return []
            }
        }
        
        let accessor: TestAccessor<TestStruct> = TestAccessor()
//        let values = accessor.load(from: \.other) // not allowed!
//        let values = accessor.load(from: \.age) // not allowed!
        let values = accessor.load(from: \.$age)
        XCTAssertEqual("\(values)", "[]")
        XCTAssertEqual("\(type(of: values))", "Array<TestStruct>")
    }
    
    func testCodable() throws {
        struct TestStruct: Identifiable, Codable, Equatable {
            var id: UUID
            
            @Indexed
            var name: String
            
            @Indexed
            var age: Int = 1
            
            var other: [Int] = []
            
            // Technically possible, but heavily discouraged:
            var composed: Indexed<String> { Indexed(wrappedValue: "\(name) \(age)") }
        }
        
        let originalValue = TestStruct(id: UUID(uuidString: "58167FAA-18C2-43E7-8E31-66E28141C9FE")!, name: "Hello!")
        
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(originalValue)
        let newValue = try JSONDecoder().decode(TestStruct.self, from: data)
        
        XCTAssertEqual(originalValue, newValue)
        
        let jsonString = String(data: data, encoding: .utf8)!
        print(jsonString)
        XCTAssertEqual(jsonString, #"{"age":1,"id":"58167FAA-18C2-43E7-8E31-66E28141C9FE","name":"Hello!","other":[]}"#)
    }
    
    func testCodableIndexedString() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(Indexed(wrappedValue: "A string"))
        let jsonString = String(data: data, encoding: .utf8)!
        let decodedValue = try JSONDecoder().decode(String.self, from: data)
        
        XCTAssertEqual(jsonString, #""A string""#)
        XCTAssertEqual(decodedValue, "A string")
    }
    
    func testCodableIndexedInt() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(Indexed(wrappedValue: 1234))
        let jsonString = String(data: data, encoding: .utf8)!
        let decodedValue = try JSONDecoder().decode(Int.self, from: data)
        
        XCTAssertEqual(jsonString, #"1234"#)
        XCTAssertEqual(decodedValue, 1234)
    }
    
    func testCodableIndexedUUID() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(Indexed(wrappedValue: UUID(uuidString: "00112233-4455-6677-8899-AABBCCDDEEFF")!))
        let jsonString = String(data: data, encoding: .utf8)!
        let decodedValue = try JSONDecoder().decode(UUID.self, from: data)
        
        XCTAssertEqual(jsonString, #""00112233-4455-6677-8899-AABBCCDDEEFF""#)
        XCTAssertEqual(decodedValue, UUID(uuidString: "00112233-4455-6677-8899-AABBCCDDEEFF"))
    }
}
