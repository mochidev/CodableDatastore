//
//  IndexRangeExpression.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-05.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

/// A type of bound found on either end of a range.
public enum RangeBoundExpression<Bound: Comparable>: Equatable {
    /// The bound reaches to the extent of the set the range is within.
    case extent
    
    /// The bound reaches up to, but not including the given value.
    case excluding(Bound)
    
    /// The bound reaches through the given value.
    case including(Bound)
}

extension RangeBoundExpression: Sendable where Bound: Sendable { }

/// A type that can represent a range within an index.
public protocol IndexRangeExpression<Bound> {
    associatedtype Bound: Comparable
    
    /// The definition of the lower bound of the range.
    var lowerBoundExpression: RangeBoundExpression<Bound> { get }
    
    /// The definition of the upper bound of the range.
    var upperBoundExpression: RangeBoundExpression<Bound> { get }
}

/// The position relative to a range.
public enum RangePosition: Equatable {
    /// A value appears before the range.
    case before
    
    /// A value appears within the range.
    case within
    
    /// A value appears after the range.
    case after
}

extension IndexRangeExpression {
    /// The position of a value relative to the range.
    func position(of value: Bound) -> RangePosition {
        switch lowerBoundExpression {
        case .extent: break
        case .excluding(let lowerBound):
            if value <= lowerBound { return .before }
        case .including(let lowerBound):
            if value < lowerBound { return .before }
        }
        
        switch upperBoundExpression {
        case .extent: break
        case .excluding(let upperBound):
            if value >= upperBound { return .after }
        case .including(let upperBound):
            if value > upperBound { return .after }
        }
        
        return .within
    }
}

extension Range: IndexRangeExpression {
    @inlinable
    public var lowerBoundExpression: RangeBoundExpression<Bound> { .including(lowerBound) }
    @inlinable
    public var upperBoundExpression: RangeBoundExpression<Bound> { .excluding(upperBound) }
}

extension ClosedRange: IndexRangeExpression {
    @inlinable
    public var lowerBoundExpression: RangeBoundExpression<Bound> { .including(lowerBound) }
    @inlinable
    public var upperBoundExpression: RangeBoundExpression<Bound> { .including(upperBound) }
}

extension PartialRangeUpTo: IndexRangeExpression {
    @inlinable
    public var lowerBoundExpression: RangeBoundExpression<Bound> { .extent }
    @inlinable
    public var upperBoundExpression: RangeBoundExpression<Bound> { .excluding(upperBound) }
}

extension PartialRangeThrough: IndexRangeExpression {
    @inlinable
    public var lowerBoundExpression: RangeBoundExpression<Bound> { .extent }
    @inlinable
    public var upperBoundExpression: RangeBoundExpression<Bound> { .including(upperBound) }
}

extension PartialRangeFrom: IndexRangeExpression {
    @inlinable
    public var lowerBoundExpression: RangeBoundExpression<Bound> { .including(lowerBound) }
    @inlinable
    public var upperBoundExpression: RangeBoundExpression<Bound> { .extent }
}
