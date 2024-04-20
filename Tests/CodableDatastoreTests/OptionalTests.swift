//
//  OptionalTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-04-20.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class OptionalTests: XCTestCase {
    func testComparable() throws {
        XCTAssertTrue(Int?.some(5) < Int?.some(10))
        XCTAssertTrue(Int?.none < Int?.some(10))
        XCTAssertFalse(Int?.some(5) < Int?.none)
        XCTAssertFalse(Int?.none < Int?.none)
    }
}
