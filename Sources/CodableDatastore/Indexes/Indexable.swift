//
//  Indexable.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-04-07.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// An alias representing the requirements for a property to be indexable, namely that they conform to both ``/Swift/Codable`` and ``/Swift/Comparable``.
public typealias Indexable = Comparable & Hashable & Codable & Sendable

/// A type-erased container for Indexable values
public struct AnyIndexable {
    /// The original indexable value.
    public var indexed: any Indexable
    
    /// Initialize a type-erased indexable value. Access it again with ``indexed``.
    public init(_ indexable: some Indexable) {
        indexed = indexable
    }
}

#if os(macOS) || os(iOS) || os(tvOS) || os(watchOS)
/// Matching implementation from https://github.com/apple/swift/pull/64899/files
extension Never: Codable {
    public init(from decoder: any Decoder) throws {
        let context = DecodingError.Context(
            codingPath: decoder.codingPath,
            debugDescription: "Unable to decode an instance of Never.")
        throw DecodingError.typeMismatch(Never.self, context)
    }
    public func encode(to encoder: any Encoder) throws {}
}
#endif

/// A marker protocol for types that can be used as a ranged index.
///
/// Ranged indexes are usually used for continuous values, where it is more desirable to retrieve intances who's indexed values lie between two other values.
///
/// - Note: If an existing type is not marked as ``RangedIndexable``, but it is advantageous for your use case for it to be marked as such and satisfies the main requirements (such as retriving a range of ordered UUIDs), simply conform that type as needed:
/// ```swift
/// extension UUID: RangedIndexable {}
/// ```
public protocol RangedIndexable: Comparable & Hashable & Codable & Sendable {}

/// A marker protocol for types that can be used as a discrete index.
///
/// Discrete indexes are usually used for specific values, where it is more desirable to retrieve intances who's indexed values match another value exactly.
///
/// - Note: If an existing type is not marked as ``DiscreteIndexable``, but it is advantageous for your use case for it to be marked as such and satisfies the main requirements (such as retriving a specific float value), simply conform that type as needed:
/// ```swift
/// extension Double: DiscreteIndexable {}
/// ```
public protocol DiscreteIndexable: Hashable & Codable & Sendable {}

// MARK: - Swift Standard Library Conformances

extension Bool: DiscreteIndexable {}
extension Double: RangedIndexable {}
@available(macOS 13.0, iOS 16, tvOS 16, watchOS 9, *)
extension Duration: RangedIndexable {}
extension Float: RangedIndexable {}
#if arch(arm64)
@available(macOS 11.0, iOS 14, tvOS 14, watchOS 7, *)
extension Float16: RangedIndexable {}
#endif
extension Int: DiscreteIndexable, RangedIndexable {}
extension Int8: DiscreteIndexable, RangedIndexable {}
extension Int16: DiscreteIndexable, RangedIndexable {}
extension Int32: DiscreteIndexable, RangedIndexable {}
extension Int64: DiscreteIndexable, RangedIndexable {}
extension Never: DiscreteIndexable, RangedIndexable {}
extension String: DiscreteIndexable, RangedIndexable {}
extension UInt: DiscreteIndexable, RangedIndexable {}
extension UInt8: DiscreteIndexable, RangedIndexable {}
extension UInt16: DiscreteIndexable, RangedIndexable {}
extension UInt32: DiscreteIndexable, RangedIndexable {}
extension UInt64: DiscreteIndexable, RangedIndexable {}

#if compiler(>=6)
extension Optional: @retroactive Comparable where Wrapped: Comparable {
    public static func < (lhs: Self, rhs: Self) -> Bool {
        if let lhs, let rhs { return lhs < rhs }
        return lhs == nil && rhs != nil
    }
}
#else
extension Optional: Comparable where Wrapped: Comparable {
    public static func < (lhs: Self, rhs: Self) -> Bool {
        if let lhs, let rhs { return lhs < rhs }
        return lhs == nil && rhs != nil
    }
}
#endif
extension Optional: DiscreteIndexable where Wrapped: DiscreteIndexable {}
extension Optional: RangedIndexable where Wrapped: RangedIndexable {}

// MARK: - Foundation Conformances

extension Date: RangedIndexable {}
#if canImport(Darwin)
extension Decimal: RangedIndexable {}
#else
extension Decimal: RangedIndexable, @unchecked Sendable {}
#endif
extension UUID: DiscreteIndexable {}
