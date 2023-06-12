//
//  Indexed.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-31.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// An alias representing the requirements for a property to be indexable, namely that they conform to both ``/Swift/Codable`` and ``/Swift/Comparable``.
public typealias Indexable = Comparable & Codable

/// A property wrapper to mark a property as one that is indexable by a data store.
///
/// Indexable properties must be ``/Swift/Codable`` so that their values can be encoded and decoded,
/// and be ``/Swift/Comparable`` so that a stable order may be formed when saving to a data store.
///
/// To mark a property as one that an index should be built against, mark it as such:
///
///     struct MyStruct {
///         var id: UUID
///
///         @Indexed
///         var name: String
///
///         @Indexed
///         var age: Int = 1
///
///         var other: [Int] = []
///
///         //@Indexed
///         //var nonCodable = NonCodable() // Not allowed!
///
///         //@Indexed
///         //var nonComparable = NonComparable() // Not allowed!
///     }
///
/// - Note: The `id` field from ``/Foundation/Identifiable`` does not need to be indexed, as it is indexed by default for instance uniqueness in a data store.
///
/// - Warning: Although changing which properties are indexed, including their names and types, is fully supported,
/// changing the ``/Swift/Comparable`` implementation of a type between builds can lead to problems. If ``/Swift/Comparable``
/// conformance changes, you should declare a new version along side it so you can force an index to be migrated at the same time.
///
/// > Attention:
/// > Only use this type as a property wrapper. Marking a computed property as returning an ``Indexed`` field is not supported, and will fail at runtime.
/// >
/// > This is because the index won't be properly detected when warming up a datastore and won't properly
/// migrate indices as a result of that failed detection:
/// >
/// >```
/// >struct MyStruct {
/// >    var id: UUID
/// >
/// >    @Indexed
/// >    var name: String
/// >
/// >    @Indexed
/// >    var age: Int = 1
/// >
/// >    /// Don't do this:
/// >    var composed: Indexed<String> { Indexed(wrappedValue: "\(name) \(age)") }
/// >}
/// >```
///
@propertyWrapper
public struct Indexed<T> where T: Indexable {
    /// The underlying value that the index will be based off of.
    ///
    /// This is ordinarily handled transparently when used as a property wrapper.
    public var wrappedValue: T
    
    /// Initialize an ``Indexed`` value with an initial value.
    ///
    /// This is ordinarily handled transparently when used as a property wrapper.
    public init(wrappedValue: T) {
        self.wrappedValue = wrappedValue
    }
    
    /// The projected value of the indexed property, which is a type-erased version of ourself.
    ///
    /// This allows the indexed property to be used in the data store using `.$property` syntax.
    public var projectedValue: _AnyIndexed { _AnyIndexed(indexed: self) }
}

/// A type-erased wrapper for indexed types.
///
/// You should not reach for this directly, and instead use the @``Indexed`` property wrapper.
public struct _AnyIndexed {
    var indexedType: String
    var indexed: any _IndexedProtocol
    
    init<T>(indexed: Indexed<T>) {
        self.indexed = indexed
        indexedType = String(describing: T.self)
    }
}

/// An internal protocol to use when evaluating types for indexed properties.
protocol _IndexedProtocol<T>: Indexable {
    associatedtype T: Indexable
    
    init(wrappedValue: T)
    
    var wrappedValue: T { get }
    var projectedValue: _AnyIndexed { get }
}

extension _IndexedProtocol {
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let wrappedValue = try container.decode(T.self)
        
        self.init(wrappedValue: wrappedValue)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(wrappedValue)
    }
    
    public static func < (lhs: Self, rhs: Self) -> Bool { lhs.wrappedValue < rhs.wrappedValue }
    public static func == (lhs: Self, rhs: Self) -> Bool { lhs.wrappedValue == rhs.wrappedValue }
}

extension Indexed: _IndexedProtocol {
}
