//
//  OptionalTests.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-04-20.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
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
