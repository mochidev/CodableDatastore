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
        
        struct TestStruct {
            var id: UUID
            
            @Indexed
            var name: String
            
            @Indexed
            var age: Int = 1
            
            var other: [Int] = []
            
//            @Indexed
//            var nonCodable = NonCodable() // Not allowed!
            
            // Technically possible, but heavily discouraged:
            var composed: Indexed<String> { Indexed(wrappedValue: "\(name) \(age)") }
        }
        
        let myValue = TestStruct(id: UUID(), name: "Hello!")
        
        XCTAssertEqual("\(myValue[keyPath: \.age])", "1")
        XCTAssertEqual("\(myValue[keyPath: \.$age])", "Indexed<Int>(wrappedValue: 1)")
        XCTAssertEqual("\(myValue[keyPath: \.composed])", "Indexed<String>(wrappedValue: \"Hello! 1\")")
        
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
//                guard childType is _Indexed.Type else { continue }
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
            guard childType is any _Indexed.Type else { continue }
            indexedProperties.append(label)
        }
        XCTAssertEqual(indexedProperties, ["_name", "_age"])
        
        struct TestAccessor<T> {
            func load<V>(from keypath: KeyPath<T, Indexed<V>>) -> [T] {
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
}
