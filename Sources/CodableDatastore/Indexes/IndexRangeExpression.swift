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

/// The order a range is declared in.
public enum RangeOrder: Equatable {
    /// The range is in ascending order.
    case ascending
    
    /// The range is in descending order.
    case descending
    
    var reversed: Self {
        switch self {
        case .ascending: return .descending
        case .descending: return .ascending
        }
    }
}

/// A type that can represent a range within an index.
public protocol IndexRangeExpression<Bound> {
    associatedtype Bound: Comparable
    
    /// The definition of the lower bound of the range.
    var lowerBoundExpression: RangeBoundExpression<Bound> { get }
    
    /// The definition of the upper bound of the range.
    var upperBoundExpression: RangeBoundExpression<Bound> { get }
    
    ///The order the elements in the range appear.
    var order: RangeOrder { get }
}

extension IndexRangeExpression {
    /// Reverse a range so it is iterated on in the opposite direction.
    var reversed: IndexRange<Bound> {
        IndexRange(
            lower: lowerBoundExpression,
            upper: upperBoundExpression,
            order: order.reversed
        )
    }
    
    func applying(_ newOrder: RangeOrder) -> some IndexRangeExpression<Bound> {
        IndexRange(
            lower: lowerBoundExpression,
            upper: upperBoundExpression,
            order: newOrder == .ascending ? order : order.reversed
        )
    }
    
    static var unbounded: IndexRange<Bound> {
        IndexRange()
    }
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
    @inlinable
    public var order: RangeOrder { .ascending }
}

extension ClosedRange: IndexRangeExpression {
    @inlinable
    public var lowerBoundExpression: RangeBoundExpression<Bound> { .including(lowerBound) }
    @inlinable
    public var upperBoundExpression: RangeBoundExpression<Bound> { .including(upperBound) }
    @inlinable
    public var order: RangeOrder { .ascending }
}

extension PartialRangeUpTo: IndexRangeExpression {
    @inlinable
    public var lowerBoundExpression: RangeBoundExpression<Bound> { .extent }
    @inlinable
    public var upperBoundExpression: RangeBoundExpression<Bound> { .excluding(upperBound) }
    @inlinable
    public var order: RangeOrder { .ascending }
}

extension PartialRangeThrough: IndexRangeExpression {
    @inlinable
    public var lowerBoundExpression: RangeBoundExpression<Bound> { .extent }
    @inlinable
    public var upperBoundExpression: RangeBoundExpression<Bound> { .including(upperBound) }
    @inlinable
    public var order: RangeOrder { .ascending }
}

extension PartialRangeFrom: IndexRangeExpression {
    @inlinable
    public var lowerBoundExpression: RangeBoundExpression<Bound> { .including(lowerBound) }
    @inlinable
    public var upperBoundExpression: RangeBoundExpression<Bound> { .extent }
    @inlinable
    public var order: RangeOrder { .ascending }
}

/// A range of indices within an Index to fetch.
public struct IndexRange<Bound: Comparable>: IndexRangeExpression {
    /// The lower bound of the range.
    ///
    /// This must compare less than upper bound.
    public var lowerBoundExpression: RangeBoundExpression<Bound>
    
    /// The upper bound of the range.
    ///
    /// This must compare greater than the lower bound.
    public var upperBoundExpression: RangeBoundExpression<Bound>
    
    /// The order of the range to check.
    public var order: RangeOrder
    
    /// Construct a range of indices within an Index to fetch.
    ///
    /// The lower bound must compare less than the upper bound, though a descending range can be specified with an order
    public init(
        lower lowerBoundExpression: RangeBoundExpression<Bound> = .extent,
        upper upperBoundExpression: RangeBoundExpression<Bound> = .extent,
        order: RangeOrder = .ascending
    ) {
        self.lowerBoundExpression = lowerBoundExpression
        self.upperBoundExpression = upperBoundExpression
        self.order = order
    }
}

infix operator ..>
postfix operator ..>

extension Comparable {
    /// A range excluding the lower bound.
    @inlinable
    public static func ..> (minimum: Self, maximum: Self) -> some IndexRangeExpression<Self> {
        precondition(minimum == minimum, "Range cannot have an unordered lower bound.")
        precondition(maximum == maximum, "Range cannot have an unordered upper bound.")
        precondition(minimum <= maximum, "Range lower bound must be less than upper bound.")
        return IndexRange(
            lower: .excluding(minimum),
            upper: .including(maximum)
        )
    }
    
    /// A partial range excluding the lower bound.
    @inlinable
    public static postfix func ..> (minimum: Self) -> some IndexRangeExpression<Self> {
        precondition(minimum == minimum, "Range cannot have an unordered lower bound.")
        return IndexRange(
            lower: .excluding(minimum),
            upper: .extent
        )
    }
}
