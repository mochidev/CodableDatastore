//
//  TransactionOptionsTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-12.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import XCTest
import CodableDatastore

final class TransactionOptionsTests: XCTestCase {
    func assertTransactionOptions(
        options: TransactionOptions,
        expectedRawValue: UInt64,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertEqual(options.rawValue, expectedRawValue, file: file, line: line)
    }
    
    func assertTransactionOptions(
        _ options: TransactionOptions,
        haveDebugString expectedString: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertEqual(options.debugDescription, expectedString, file: file, line: line)
    }
    
    func testTransactionOptions() {
        assertTransactionOptions(options: [], expectedRawValue: 0)
        
        assertTransactionOptions(options: .readOnly, expectedRawValue: 1)
        assertTransactionOptions(options: .collateWrites, expectedRawValue: 2)
        assertTransactionOptions(options: .idempotent, expectedRawValue: 4)
        
        assertTransactionOptions(options: [.readOnly, .collateWrites], expectedRawValue: 3)
        assertTransactionOptions(options: [.readOnly, .idempotent], expectedRawValue: 5)
        assertTransactionOptions(options: [.collateWrites, .idempotent], expectedRawValue: 6)
        
        assertTransactionOptions(options: [.readOnly, .collateWrites, .idempotent], expectedRawValue: 7)
    }
    
    func testInvalidTransactionOptions() {
        assertTransactionOptions(options: TransactionOptions(rawValue: 8), expectedRawValue: 0)
        assertTransactionOptions(options: TransactionOptions(rawValue: 9), expectedRawValue: 1)
        assertTransactionOptions(options: TransactionOptions(rawValue: 10), expectedRawValue: 2)
        assertTransactionOptions(options: TransactionOptions(rawValue: 11), expectedRawValue: 3)
    }
    
    func testDebugStrings() {
        assertTransactionOptions([], haveDebugString: "TransactionOptions([])")
        
        assertTransactionOptions(.readOnly, haveDebugString: "TransactionOptions([.readOnly])")
        assertTransactionOptions(.idempotent, haveDebugString: "TransactionOptions([.idempotent])")
        assertTransactionOptions(.collateWrites, haveDebugString: "TransactionOptions([.collateWrites])")
        
        assertTransactionOptions([.idempotent, .readOnly], haveDebugString: "TransactionOptions([.readOnly, .idempotent])")
        assertTransactionOptions([.readOnly, .idempotent], haveDebugString: "TransactionOptions([.readOnly, .idempotent])")
        assertTransactionOptions([.readOnly, .collateWrites], haveDebugString: "TransactionOptions([.readOnly, .collateWrites])")
        assertTransactionOptions([.idempotent, .collateWrites], haveDebugString: "TransactionOptions([.collateWrites, .idempotent])")
        
        assertTransactionOptions([.readOnly, .idempotent, .collateWrites], haveDebugString: "TransactionOptions([.readOnly, .collateWrites, .idempotent])")
    }
}

final class UnsafeTransactionOptionsTests: XCTestCase {
    func assertUnsafeTransactionOptions(
        options: UnsafeTransactionOptions,
        expectedRawValue: UInt64,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertEqual(options.rawValue, expectedRawValue, file: file, line: line)
    }
    
    func assertUnsafeTransactionOptions(
        _ options: UnsafeTransactionOptions,
        haveDebugString expectedString: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertEqual(options.debugDescription, expectedString, file: file, line: line)
    }
    
    func testUnsafeTransactionOptions() {
        assertUnsafeTransactionOptions(options: [], expectedRawValue: 0)
        
        assertUnsafeTransactionOptions(options: .readOnly, expectedRawValue: 1)
        assertUnsafeTransactionOptions(options: .collateWrites, expectedRawValue: 2)
        assertUnsafeTransactionOptions(options: .idempotent, expectedRawValue: 4)
        assertUnsafeTransactionOptions(options: .skipObservations, expectedRawValue: 65536)
        
        assertUnsafeTransactionOptions(options: [.readOnly, .collateWrites], expectedRawValue: 3)
        assertUnsafeTransactionOptions(options: [.readOnly, .idempotent], expectedRawValue: 5)
        assertUnsafeTransactionOptions(options: [.readOnly, .skipObservations], expectedRawValue: 65537)
        assertUnsafeTransactionOptions(options: [.collateWrites, .idempotent], expectedRawValue: 6)
        assertUnsafeTransactionOptions(options: [.collateWrites, .skipObservations], expectedRawValue: 65538)
        assertUnsafeTransactionOptions(options: [.idempotent, .skipObservations], expectedRawValue: 65540)
        
        assertUnsafeTransactionOptions(options: [.readOnly, .collateWrites, .idempotent], expectedRawValue: 7)
        assertUnsafeTransactionOptions(options: [.readOnly, .collateWrites, .skipObservations], expectedRawValue: 65539)
        assertUnsafeTransactionOptions(options: [.readOnly, .idempotent, .skipObservations], expectedRawValue: 65541)
        assertUnsafeTransactionOptions(options: [.collateWrites, .idempotent, .skipObservations], expectedRawValue: 65542)
        
        assertUnsafeTransactionOptions(options: [.readOnly, .collateWrites, .idempotent, .skipObservations], expectedRawValue: 65543)
    }
    
    func testConvertedTransactionOptions() {
        assertUnsafeTransactionOptions(options: UnsafeTransactionOptions(TransactionOptions([])), expectedRawValue: 0)
        
        assertUnsafeTransactionOptions(options: UnsafeTransactionOptions(TransactionOptions.readOnly), expectedRawValue: 1)
        assertUnsafeTransactionOptions(options: UnsafeTransactionOptions(TransactionOptions.collateWrites), expectedRawValue: 2)
        assertUnsafeTransactionOptions(options: UnsafeTransactionOptions(TransactionOptions.idempotent), expectedRawValue: 4)
        
        assertUnsafeTransactionOptions(options: UnsafeTransactionOptions(TransactionOptions([.readOnly, .collateWrites])), expectedRawValue: 3)
        assertUnsafeTransactionOptions(options: UnsafeTransactionOptions(TransactionOptions([.readOnly, .idempotent])), expectedRawValue: 5)
        assertUnsafeTransactionOptions(options: UnsafeTransactionOptions(TransactionOptions([.collateWrites, .idempotent])), expectedRawValue: 6)
        
        assertUnsafeTransactionOptions(options: UnsafeTransactionOptions(TransactionOptions([.readOnly, .collateWrites, .idempotent])), expectedRawValue: 7)
    }
    
    func testDebugStrings() {
        assertUnsafeTransactionOptions([], haveDebugString: "UnsafeTransactionOptions([])")
        
        assertUnsafeTransactionOptions(.readOnly, haveDebugString: "UnsafeTransactionOptions([.readOnly])")
        assertUnsafeTransactionOptions(.idempotent, haveDebugString: "UnsafeTransactionOptions([.idempotent])")
        assertUnsafeTransactionOptions(.collateWrites, haveDebugString: "UnsafeTransactionOptions([.collateWrites])")
        assertUnsafeTransactionOptions(.skipObservations, haveDebugString: "UnsafeTransactionOptions([.skipObservations])")
        assertUnsafeTransactionOptions(.enforceDurability, haveDebugString: "UnsafeTransactionOptions([.enforceDurability])")
        
        assertUnsafeTransactionOptions([.idempotent, .readOnly], haveDebugString: "UnsafeTransactionOptions([.readOnly, .idempotent])")
        assertUnsafeTransactionOptions([.readOnly, .idempotent], haveDebugString: "UnsafeTransactionOptions([.readOnly, .idempotent])")
        
        assertUnsafeTransactionOptions([.readOnly, .collateWrites], haveDebugString: "UnsafeTransactionOptions([.readOnly, .collateWrites])")
        assertUnsafeTransactionOptions([.readOnly, .skipObservations], haveDebugString: "UnsafeTransactionOptions([.readOnly, .skipObservations])")
        assertUnsafeTransactionOptions([.readOnly, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.readOnly, .enforceDurability])")
        assertUnsafeTransactionOptions([.collateWrites, .idempotent], haveDebugString: "UnsafeTransactionOptions([.collateWrites, .idempotent])")
        assertUnsafeTransactionOptions([.collateWrites, .skipObservations], haveDebugString: "UnsafeTransactionOptions([.collateWrites, .skipObservations])")
        assertUnsafeTransactionOptions([.collateWrites, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.collateWrites, .enforceDurability])")
        assertUnsafeTransactionOptions([.idempotent, .skipObservations], haveDebugString: "UnsafeTransactionOptions([.idempotent, .skipObservations])")
        assertUnsafeTransactionOptions([.idempotent, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.idempotent, .enforceDurability])")
        assertUnsafeTransactionOptions([.skipObservations, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.skipObservations, .enforceDurability])")
        
        assertUnsafeTransactionOptions([.readOnly, .collateWrites, .idempotent], haveDebugString: "UnsafeTransactionOptions([.readOnly, .collateWrites, .idempotent])")
        assertUnsafeTransactionOptions([.readOnly, .collateWrites, .skipObservations], haveDebugString: "UnsafeTransactionOptions([.readOnly, .collateWrites, .skipObservations])")
        assertUnsafeTransactionOptions([.readOnly, .collateWrites, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.readOnly, .collateWrites, .enforceDurability])")
        assertUnsafeTransactionOptions([.readOnly, .idempotent, .skipObservations], haveDebugString: "UnsafeTransactionOptions([.readOnly, .idempotent, .skipObservations])")
        assertUnsafeTransactionOptions([.readOnly, .idempotent, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.readOnly, .idempotent, .enforceDurability])")
        assertUnsafeTransactionOptions([.readOnly, .skipObservations, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.readOnly, .skipObservations, .enforceDurability])")
        assertUnsafeTransactionOptions([.collateWrites, .idempotent, .skipObservations], haveDebugString: "UnsafeTransactionOptions([.collateWrites, .idempotent, .skipObservations])")
        assertUnsafeTransactionOptions([.collateWrites, .idempotent, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.collateWrites, .idempotent, .enforceDurability])")
        assertUnsafeTransactionOptions([.collateWrites, .skipObservations, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.collateWrites, .skipObservations, .enforceDurability])")
        assertUnsafeTransactionOptions([.idempotent, .skipObservations, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.idempotent, .skipObservations, .enforceDurability])")
        
        assertUnsafeTransactionOptions([.readOnly, .collateWrites, .idempotent, .skipObservations], haveDebugString: "UnsafeTransactionOptions([.readOnly, .collateWrites, .idempotent, .skipObservations])")
        assertUnsafeTransactionOptions([.readOnly, .collateWrites, .idempotent, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.readOnly, .collateWrites, .idempotent, .enforceDurability])")
        assertUnsafeTransactionOptions([.readOnly, .collateWrites, .skipObservations, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.readOnly, .collateWrites, .skipObservations, .enforceDurability])")
        assertUnsafeTransactionOptions([.readOnly, .idempotent, .skipObservations, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.readOnly, .idempotent, .skipObservations, .enforceDurability])")
        assertUnsafeTransactionOptions([.collateWrites, .idempotent, .skipObservations, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.collateWrites, .idempotent, .skipObservations, .enforceDurability])")
        
        assertUnsafeTransactionOptions([.readOnly, .collateWrites, .idempotent, .skipObservations, .enforceDurability], haveDebugString: "UnsafeTransactionOptions([.readOnly, .collateWrites, .idempotent, .skipObservations, .enforceDurability])")
    }
}
