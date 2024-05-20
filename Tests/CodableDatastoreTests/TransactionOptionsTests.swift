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
}
