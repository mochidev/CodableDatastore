//
//  TypedIdentifierTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-10.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

extension TypedIdentifier {
    static var mockIdentifier: Self {
        Self(name: "Test", token: 0x0123456789abcdef)
    }
}

final class TypedIdentifierTests: XCTestCase {
    func testRawValue() {
        XCTAssertEqual(TypedIdentifier<Self>(rawValue: "").rawValue, "")
        XCTAssertEqual(TypedIdentifier<Self>(rawValue: "Test-0000000000000000").rawValue, "Test-0000000000000000")
        XCTAssertEqual(TypedIdentifier<Self>(rawValue: "semi-valid").rawValue, "semi-valid")
    }
    
    func testNameToken() {
        XCTAssertEqual(TypedIdentifier<Self>(name: "Test", token: 0), TypedIdentifier<Self>(rawValue: "Test-0000000000000000"))
        
        XCTAssertEqual(TypedIdentifier<Self>(name: "Test", token: 0x0123456789abcdef), TypedIdentifier<Self>(rawValue: "Test-0123456789ABCDEF"))
        
        XCTAssertEqual(TypedIdentifier<Self>(name: "With Spaces", token: 0x0123456789abcdef), TypedIdentifier<Self>(rawValue: "With Spaces-0123456789ABCDEF"))
        
        XCTAssertEqual(TypedIdentifier<Self>(name: "With-Dashes", token: 0x0123456789abcdef), TypedIdentifier<Self>(rawValue: "WithDashes-0123456789ABCDEF"))
        
        XCTAssertEqual(TypedIdentifier<Self>(name: "With_Underscores", token: 0x0123456789abcdef), TypedIdentifier<Self>(rawValue: "With_Underscores-0123456789ABCDEF"))
        
        XCTAssertEqual(TypedIdentifier<Self>(name: "With Long-names_And-Characters", token: 0x0123456789abcdef), TypedIdentifier<Self>(rawValue: "With Longnames_A-0123456789ABCDEF"))
        
        XCTAssertEqual(TypedIdentifier<Self>(name: "Numbers? 0", token: 0x0123456789abcdef), TypedIdentifier<Self>(rawValue: "Numbers 0-0123456789ABCDEF"))
        
        XCTAssertEqual(TypedIdentifier<Self>(name: "", token: 0x0123456789abcdef), TypedIdentifier<Self>(rawValue: "-0123456789ABCDEF"))
    }
}
