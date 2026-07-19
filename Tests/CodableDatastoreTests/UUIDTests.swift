//
//  UUIDTests.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-04.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
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
