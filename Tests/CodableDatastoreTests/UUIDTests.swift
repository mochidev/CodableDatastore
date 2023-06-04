//
//  UUIDTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-04.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class UUIDTests: XCTestCase {
    func testComparable() throws {
        XCTAssertTrue(UUID(uuidString: "00112233-4455-6677-8899-AABBCCDDEEF0")! < UUID(uuidString: "00112233-4455-6677-8899-AABBCCDDEEF1")!)
        XCTAssertTrue(UUID(uuidString: "00112233-4455-6677-8899-AABBCCDDEEF1")! > UUID(uuidString: "00112233-4455-6677-8899-AABBCCDDEEF0")!)
        XCTAssertTrue(UUID(uuidString: "00112233-4455-6677-8899-AABBCCDDEEF1")! < UUID(uuidString: "10112233-4455-6677-8899-AABBCCDDEEF0")!)
        XCTAssertTrue(UUID(uuidString: "10112233-4455-6677-8899-AABBCCDDEEF0")! > UUID(uuidString: "00112233-4455-6677-8899-AABBCCDDEEF1")!)
    }
}
