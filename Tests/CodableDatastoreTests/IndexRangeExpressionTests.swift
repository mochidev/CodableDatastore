//
//  IndexRangeExpressionTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-05.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class IndexRangeExpressionTests: XCTestCase {
    func testLowerBound() throws {
        func lowerBoundExpression<B>(_ range: some IndexRangeExpression<B>) -> RangeBoundExpression<B> {
            range.lowerBoundExpression
        }
        
        func lowerBoundExpression(_ range: Swift.UnboundedRange) -> RangeBoundExpression<Int> {
            lowerBoundExpression(IndexRange())
        }
        
        XCTAssertEqual(lowerBoundExpression(1..<2), .including(1))
        XCTAssertEqual(lowerBoundExpression(1...2), .including(1))
        XCTAssertEqual(lowerBoundExpression(1..>2), .excluding(1))
        XCTAssertEqual(lowerBoundExpression(..<2), .extent)
        XCTAssertEqual(lowerBoundExpression(...2), .extent)
        XCTAssertEqual(lowerBoundExpression(1...), .including(1))
        XCTAssertEqual(lowerBoundExpression(1..>), .excluding(1))
        XCTAssertEqual(lowerBoundExpression(...), .extent)
    }
    
    func testUpperBound() throws {
        func upperBoundExpression<B>(_ range: some IndexRangeExpression<B>) -> RangeBoundExpression<B> {
            range.upperBoundExpression
        }
        
        func upperBoundExpression(_ range: Swift.UnboundedRange) -> RangeBoundExpression<Int> {
            upperBoundExpression(IndexRange())
        }
        
        XCTAssertEqual(upperBoundExpression(1..<2), .excluding(2))
        XCTAssertEqual(upperBoundExpression(1...2), .including(2))
        XCTAssertEqual(upperBoundExpression(1..>2), .including(2))
        XCTAssertEqual(upperBoundExpression(..<2), .excluding(2))
        XCTAssertEqual(upperBoundExpression(...2), .including(2))
        XCTAssertEqual(upperBoundExpression(1...), .extent)
        XCTAssertEqual(upperBoundExpression(1..>), .extent)
        XCTAssertEqual(upperBoundExpression(...), .extent)
    }
    
    func testPosition() throws {
        func position<B>(of value: B, in range: some IndexRangeExpression<B>) -> RangePosition {
            range.position(of: value)
        }
        
        func position(of value: Int, in range: Swift.UnboundedRange) -> RangePosition {
            position(of: value, in: IndexRange())
        }
        
        XCTAssertEqual(position(of: 0, in: 1..<3), .before)
        XCTAssertEqual(position(of: 0, in: 1...3), .before)
        XCTAssertEqual(position(of: 0, in: 1..>3), .before)
        XCTAssertEqual(position(of: 0, in: ..<3), .within)
        XCTAssertEqual(position(of: 0, in: ...3), .within)
        XCTAssertEqual(position(of: 0, in: 1...), .before)
        XCTAssertEqual(position(of: 0, in: 1..>), .before)
        XCTAssertEqual(position(of: 0, in: ...), .within)
        
        XCTAssertEqual(position(of: 1, in: 1..<3), .within)
        XCTAssertEqual(position(of: 1, in: 1...3), .within)
        XCTAssertEqual(position(of: 1, in: 1..>3), .before)
        XCTAssertEqual(position(of: 1, in: ..<3), .within)
        XCTAssertEqual(position(of: 1, in: ...3), .within)
        XCTAssertEqual(position(of: 1, in: 1...), .within)
        XCTAssertEqual(position(of: 1, in: 1..>), .before)
        XCTAssertEqual(position(of: 1, in: ...), .within)
        
        XCTAssertEqual(position(of: 2, in: 1..<3), .within)
        XCTAssertEqual(position(of: 2, in: 1...3), .within)
        XCTAssertEqual(position(of: 2, in: 1..>3), .within)
        XCTAssertEqual(position(of: 2, in: ..<3), .within)
        XCTAssertEqual(position(of: 2, in: ...3), .within)
        XCTAssertEqual(position(of: 2, in: 1...), .within)
        XCTAssertEqual(position(of: 2, in: 1..>), .within)
        XCTAssertEqual(position(of: 2, in: ...), .within)
        
        XCTAssertEqual(position(of: 3, in: 1..<3), .after)
        XCTAssertEqual(position(of: 3, in: 1...3), .within)
        XCTAssertEqual(position(of: 3, in: 1..>3), .within)
        XCTAssertEqual(position(of: 3, in: ..<3), .after)
        XCTAssertEqual(position(of: 3, in: ...3), .within)
        XCTAssertEqual(position(of: 3, in: 1...), .within)
        XCTAssertEqual(position(of: 3, in: 1..>), .within)
        XCTAssertEqual(position(of: 3, in: ...), .within)
        
        XCTAssertEqual(position(of: 4, in: 1..<3), .after)
        XCTAssertEqual(position(of: 4, in: 1...3), .after)
        XCTAssertEqual(position(of: 4, in: 1..>3), .after)
        XCTAssertEqual(position(of: 4, in: ..<3), .after)
        XCTAssertEqual(position(of: 4, in: ...3), .after)
        XCTAssertEqual(position(of: 4, in: 1...), .within)
        XCTAssertEqual(position(of: 4, in: 1..>), .within)
        XCTAssertEqual(position(of: 4, in: ...), .within)
    }
}
