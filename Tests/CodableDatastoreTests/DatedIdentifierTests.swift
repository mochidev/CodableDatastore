//
//  DatedIdentifierTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

extension DatedIdentifier {
    static var mockIdentifier: Self {
        Self(date: Date(timeIntervalSince1970: 97445), token: 0x0123456789abcdef)
    }
}

final class DatedIdentifierTests: XCTestCase {
    func testRawValue() {
        XCTAssertEqual(Identifier<Self>(date: Date(timeIntervalSince1970: 0), token: 0), Identifier<Self>(rawValue: "1970-01-01 00-00-00 0000000000000000"))
        XCTAssertEqual(Identifier<Self>(rawValue: "1970-01-01 00-00-00 0000000000000000").rawValue, "1970-01-01 00-00-00 0000000000000000")
        
        XCTAssertEqual(Identifier<Self>(date: Date(timeIntervalSince1970: 0), token: 0x0123456789abcdef), Identifier<Self>(rawValue: "1970-01-01 00-00-00 0123456789ABCDEF"))
        XCTAssertEqual(Identifier<Self>(rawValue: "1970-01-01 00-00-00 0123456789ABCDEF").rawValue, "1970-01-01 00-00-00 0123456789ABCDEF")
        
        XCTAssertEqual(Identifier<Self>(rawValue: "semi-valid").rawValue, "semi-valid")
    }
    
    func testValidComponents() throws {
        let components = try Identifier<Self>.mockIdentifier.components
        XCTAssertEqual(components.year, "1970")
        XCTAssertEqual(components.month, "01")
        XCTAssertEqual(components.day, "02")
        XCTAssertEqual(components.hour, "03")
        XCTAssertEqual(components.minute, "04")
        XCTAssertEqual(components.second, "05")
        XCTAssertEqual(components.token, "0123456789ABCDEF")
        XCTAssertEqual(components.monthDay, "01-02")
        XCTAssertEqual(components.hourMinute, "03-04")
        
        XCTAssertEqual(Identifier<Self>(rawValue: "semi-valid").rawValue, "semi-valid")
        XCTAssertThrowsError(try Identifier<Self>(rawValue: "semi-valid").components) { error in
            XCTAssertEqual(error as? DatedIdentifierError, .invalidLength)
        }
    }
    
    func testInvalidComponents() throws {
        XCTAssertThrowsError(try Identifier<Self>(rawValue: "semi-valid").components) { error in
            XCTAssertEqual(error as? DatedIdentifierError, .invalidLength)
        }
    }
}
