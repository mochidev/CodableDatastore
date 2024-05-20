//
//  IndexRepresentation.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-04-07.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A representation of an index for a given instance, with value information erased.
/// 
/// - Note: Although conforming to this type will construct an index, you don't be able to access this index using any of the usuall accessors. Instead, consider confirming to ``RetrievableIndexRepresentation`` or ``SingleInstanceIndexRepresentation`` as appropriate.
public protocol IndexRepresentation<Instance>: Hashable, Sendable {
    /// The instance the index belongs to.
    associatedtype Instance: Sendable
    
    /// The index type seriealized to the datastore to detect changes to index structure.
    var indexType: IndexType { get }
    
    /// Conditionally cast the index to one that matched the instance type T.
    /// - Parameter instance: The instance type that we would like to verify.
    /// - Returns: The casted index.
    func matches<T>(_ instance: T.Type) -> (any IndexRepresentation<T>)?
    
    /// The type erased values the index matches against for a given index.
    func valuesToIndex(for instance: Instance) -> [AnyIndexable]
}

extension IndexRepresentation {
    /// The index representation in a form suitable for keying in a dictionary.
    public var key: AnyIndexRepresentation<Instance> { AnyIndexRepresentation(indexRepresentation: self) }
    
    /// Check if two ``IndexRepresentation``s are equal.
    @usableFromInline
    func isEqual(rhs: some IndexRepresentation<Instance>) -> Bool {
        return self == rhs as? Self
    }
}

/// A representation of an index for a given instance, preserving value information.
public protocol RetrievableIndexRepresentation<Instance, Value>: IndexRepresentation {
    /// The value represented within the index.
    associatedtype Value: Indexable & Hashable
    
    /// The concrete values the index matches against for a given index.
    func valuesToIndex(for instance: Instance) -> Set<Value>
}

extension RetrievableIndexRepresentation {
    @inlinable
    public func valuesToIndex(for instance: Instance) -> [AnyIndexable] {
        valuesToIndex(for: instance).map { AnyIndexable($0)}
    }
}

/// A representation of an index for a given instance, where a single instance matches every provided value.
public protocol SingleInstanceIndexRepresentation<
    Instance,
    Value
>: RetrievableIndexRepresentation where Value: DiscreteIndexable {}

/// A representation of an index for a given instance, where multiple index values could point to one or more instances.
public protocol MultipleInputIndexRepresentation<
    Instance,
    Sequence,
    Value
>: RetrievableIndexRepresentation {
    /// The sequence of values represented in the index.
    associatedtype Sequence: Swift.Sequence<Value>
}

/// An index where every value matches at most a single instance.
///
/// This type of index is typically used for most unique identifiers, and may be useful if there is an alternative unique identifier a instance may be referenced under.
public struct OneToOneIndexRepresentation<
    Instance: Sendable,
    Value: Indexable & DiscreteIndexable
>: SingleInstanceIndexRepresentation, @unchecked Sendable {
    @usableFromInline
    let keypath: KeyPath<Instance, Value>
    
    /// Initialize a One-value to One-instance index.
    @inlinable
    public init(_ keypath: KeyPath<Instance, Value>) {
        self.keypath = keypath
    }
    
    @inlinable
    public var indexType: IndexType {
        IndexType("OneToOneIndex(\(String(describing: Value.self)))")
    }
    
    public func matches<T: Sendable>(_ instance: T.Type) -> (any IndexRepresentation<T>)? {
        guard let copy = self as? OneToOneIndexRepresentation<T, Value>
        else { return nil }
        return copy
    }
    
    @inlinable
    public func valuesToIndex(for instance: Instance) -> Set<Value> {
        [instance[keyPath: keypath as KeyPath<Instance, Value>]]
    }
}

/// An index where every value can match any number of instances, but every instance is represented by a single value.
///
/// This type of index is the most common, where multiple instances can share the same single value that is passed in.
public struct OneToManyIndexRepresentation<
    Instance: Sendable,
    Value: Indexable
>: RetrievableIndexRepresentation, @unchecked Sendable {
    @usableFromInline
    let keypath: KeyPath<Instance, Value>
    
    /// Initialize a One-value to Many-instance index.
    @inlinable
    public init(_ keypath: KeyPath<Instance, Value>) {
        self.keypath = keypath
    }
    
    @inlinable
    public var indexType: IndexType {
        IndexType("OneToManyIndex(\(String(describing: Value.self)))")
    }
    
    @inlinable
    public func matches<T: Sendable>(_ instance: T.Type) -> (any IndexRepresentation<T>)? {
        guard let copy = self as? OneToManyIndexRepresentation<T, Value>
        else { return nil }
        return copy
    }
    
    @inlinable
    public func valuesToIndex(for instance: Instance) -> Set<Value> {
        [instance[keyPath: keypath as KeyPath<Instance, Value>]]
    }
}

/// An index where every value matches at most a single instance., but every instance can be represented by more than a single value.
///
/// This type of index can be used if several alternative identifiers can reference an instance, and they all reside in a single property.
public struct ManyToOneIndexRepresentation<
    Instance: Sendable,
    Sequence: Swift.Sequence<Value>,
    Value: Indexable & DiscreteIndexable
>: SingleInstanceIndexRepresentation & MultipleInputIndexRepresentation, @unchecked Sendable {
    @usableFromInline
    let keypath: KeyPath<Instance, Sequence>
    
    /// Initialize a Many-value to One-instance index.
    @inlinable
    public init(_ keypath: KeyPath<Instance, Sequence>) {
        self.keypath = keypath
    }
    
    @inlinable
    public var indexType: IndexType {
        IndexType("ManyToOneIndex(\(String(describing: Value.self)))")
    }
    
    @inlinable
    public func matches<T: Sendable>(_ instance: T.Type) -> (any IndexRepresentation<T>)? {
        guard let copy = self as? ManyToOneIndexRepresentation<T, Sequence, Value>
        else { return nil }
        return copy
    }
    
    @inlinable
    public func valuesToIndex(for instance: Instance) -> Set<Value> {
        Set(instance[keyPath: keypath as KeyPath<Instance, Sequence>])
    }
}

/// An index where every value can match any number of instances, and every instance can be represented by more than a single value.
///
/// This type of index is common when building relationships between different instances, where one instance may be related to several others in some way.
public struct ManyToManyIndexRepresentation<
    Instance: Sendable,
    Sequence: Swift.Sequence<Value>,
    Value: Indexable
>: MultipleInputIndexRepresentation, @unchecked Sendable {
    @usableFromInline
    let keypath: KeyPath<Instance, Sequence>
    
    /// Initialize a Many-value to Many-instance index.
    @inlinable
    public init(_ keypath: KeyPath<Instance, Sequence>) {
        self.keypath = keypath
    }
    
    @inlinable
    public var indexType: IndexType {
        IndexType("ManyToManyIndex(\(String(describing: Value.self)))")
    }
    
    @inlinable
    public func matches<T: Sendable>(_ instance: T.Type) -> (any IndexRepresentation<T>)? {
        guard let copy = self as? ManyToManyIndexRepresentation<T, Sequence, Value>
        else { return nil }
        return copy
    }
    
    @inlinable
    public func valuesToIndex(for instance: Instance) -> Set<Value> {
        Set(instance[keyPath: keypath as KeyPath<Instance, Sequence>])
    }
}

/// A property wrapper for marking which indexes should store instances in their entirety without needing to do a secondary lookup.
///
/// - Note: Direct indexes are best used when reads are a #1 priority and disk space is not a concern, as each direct index duplicates the etirety of the data stored in the datastore to prioritize faster reads.
///
/// - Important: Do not include an index for `id` if your type is Identifiable — one is created automatically on your behalf.
@propertyWrapper
public struct Direct<Index: IndexRepresentation>: Sendable {
    /// The underlying value that the index will be based off of.
    ///
    /// This is ordinarily handled transparently when used as a property wrapper.
    public let wrappedValue: Index
    
    /// Initialize a ``Direct`` index with an initial ``IndexRepresentation`` value.
    ///
    /// This is ordinarily handled transparently when used as a property wrapper.
    @inlinable
    public init(wrappedValue: Index) {
        self.wrappedValue = wrappedValue
    }
}

/// An internal helper protocol for detecting direct indexes when reflecting the format for compatible properties.
protocol DirectIndexRepresentation<Instance> {
    associatedtype Instance
    
    /// The underlying index being wrapped, conditionally casted if the instance types match.
    /// - Parameter instance: The instance type to cast to.
    /// - Returns: The casted index
    func index<T>(matching instance: T.Type) -> (any IndexRepresentation<T>)?
}

extension Direct: DirectIndexRepresentation {
    typealias Instance = Index.Instance
    
    func index<T>(matching instance: T.Type) -> (any IndexRepresentation<T>)? {
        guard let index = wrappedValue.matches(instance) else { return nil }
        return index
    }
}

extension Encodable {
    /// Retrieve the type erased values for a given index.
    subscript<Index: IndexRepresentation<Self>>(index indexRepresentation: Index) -> [AnyIndexable] {
        return indexRepresentation.valuesToIndex(for: self)
    }
    
    /// Retrieve the concrete values for a given index.
    subscript<Index: RetrievableIndexRepresentation<Self, Value>, Value>(index indexRepresentation: Index) -> Set<Value> {
        return indexRepresentation.valuesToIndex(for: self)
    }
}

/// A type erased index representation to be used for keying indexes in a dictionary.
public struct AnyIndexRepresentation<Instance: Sendable>: Hashable, Sendable {
    @usableFromInline
    var indexRepresentation: any IndexRepresentation<Instance>
    
    @inlinable
    public static func == (lhs: AnyIndexRepresentation<Instance>, rhs: AnyIndexRepresentation<Instance>) -> Bool {
        return lhs.indexRepresentation.isEqual(rhs: rhs.indexRepresentation)
    }
    
    @inlinable
    public func hash(into hasher: inout Hasher) {
        hasher.combine(indexRepresentation)
    }
}
