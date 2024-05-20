//
//  DatedIdentifierTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-10.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

extension DatedIdentifier {
    static var mockIdentifier: Self {
        Self(date: Date(timeIntervalSince1970: 97445.678), token: 0x0123456789abcdef)
    }
}

final class DatedIdentifierTests: XCTestCase {
    func testRawValue() {
        XCTAssertEqual(DatedIdentifier<Self>(date: Date(timeIntervalSince1970: 0), token: 0), DatedIdentifier<Self>(rawValue: "1970-01-01 00-00-00-000 0000000000000000"))
        XCTAssertEqual(DatedIdentifier<Self>(rawValue: "1970-01-01 00-00-00-000 0000000000000000").rawValue, "1970-01-01 00-00-00-000 0000000000000000")
        
        XCTAssertEqual(DatedIdentifier<Self>(date: Date(timeIntervalSince1970: 0), token: 0x0123456789abcdef), DatedIdentifier<Self>(rawValue: "1970-01-01 00-00-00-000 0123456789ABCDEF"))
        XCTAssertEqual(DatedIdentifier<Self>(rawValue: "1970-01-01 00-00-00-000 0123456789ABCDEF").rawValue, "1970-01-01 00-00-00-000 0123456789ABCDEF")
        
        XCTAssertEqual(DatedIdentifier<Self>(rawValue: "semi-valid").rawValue, "semi-valid")
    }
    
    func testValidComponents() throws {
        let components = try DatedIdentifier<Self>.mockIdentifier.components
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
        
        XCTAssertEqual(DatedIdentifier<Self>(rawValue: "semi-valid").rawValue, "semi-valid")
        XCTAssertThrowsError(try DatedIdentifier<Self>(rawValue: "semi-valid").components) { error in
            XCTAssertEqual(error as? DatedIdentifierError, .invalidLength)
        }
    }
    
    func testInvalidComponents() throws {
        XCTAssertThrowsError(try DatedIdentifier<Self>(rawValue: "semi-valid").components) { error in
            XCTAssertEqual(error as? DatedIdentifierError, .invalidLength)
        }
    }
}
