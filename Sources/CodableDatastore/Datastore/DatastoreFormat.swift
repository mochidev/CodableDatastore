//
//  DatastoreFormat.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-04-07.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A representation of the underlying format of a ``Datastore``.
///
/// A ``DatastoreFormat`` will be instanciated and owned by the datastore associated with it to provide both type and index information to the store. It is expected to represent the ideal types for the latest version of the code that is instantiating the datastore.
///
/// This type also exists so implementers can conform a `struct` to it that declares a number of key paths as stored properties.
///
/// Conformers can create subtypes for their versioned models either in the body of their struct or in legacy extentions. Additionally, you are encouraged to make **static** properties available for things like the current version, or a configured ``Datastore`` — this allows easy access to them without mucking around declaring them in far-away places in your code base.
///
/// ```swift
/// struct BooksFormat {
///     static let defaultKey: DatastoreKey = "BooksStore"
///     static let currentVersion: Version = .one
///
///     enum Version: String {
///         case zero = "2024-04-01"
///         case one = "2024-04-09"
///     }
///
///     typealias Instance = Book
///
///     struct BookV1: Codable, Identifiable {
///         var id: UUID
///         var title: String
///         var author: String
///     }
///
///     struct Book: Codable, Identifiable {
///         var id: UUID
///         var title: SortableTitle
///         var authors: [AuthorID]
///         var isbn: ISBN
///     }
///
///     let title = Index(\.title)
///     let author = ManyToMany(\.author)
///     let isbn = OneToOne(\.isbn)
/// }
/// ```
///
/// - Note: If your ``Instance`` type is ``/Swift/Identifiable``,  you should _not_ declare an index for `id` — special accessors are created on your behalf that can be used instead.
///
/// - Important: We discourage declaring non-static stored and computed properties on your conforming type, as that will polute the key-path namespace of the format which is used for generating getters on the datastore.
public protocol DatastoreFormat<Version, Instance, Identifier> {
    /// A type representing the version of the datastore on disk.
    ///
    /// Best represented as an enum, this represents the every single version of the datastore you wish to be able to decode from disk. Assign a new version any time the codable representation or the representation of indexes is no longer backwards compatible.
    ///
    /// The various ``Datastore`` initializers take a disctionary that maps between these versions and the most up-to-date Instance type, and will provide an opportunity to use legacy representations to decode the data to the expected type.
    associatedtype Version: RawRepresentable & Hashable & CaseIterable where Version.RawValue: Indexable & Comparable
    
    /// The most up-to-date representation you use in your codebase.
    associatedtype Instance: Codable
    
    /// The identifier to be used when de-duplicating instances saved in the persistence.
    ///
    /// Although ``Instance`` does _not_ need to be ``Identifiable``, a consistent identifier must still be provided for every instance to retrive and persist them. This identifier can be different from `Instance.ID` if truly necessary, though most conformers can simply set it to `Instance.ID`
    associatedtype Identifier: Indexable & DiscreteIndexable
    
    /// A default initializer creating a format instance the datastore can use for evaluation.
    init()
    
    /// The default key to use when accessing the datastore for this type.
    static var defaultKey: DatastoreKey { get }
    
    /// The current version to normalize the persisted datastore to.
    static var currentVersion: Version { get }
    
    /// A One-value to Many-instance index.
    ///
    /// This type of index is the most common, where multiple instances can share the same single value that is passed in.
    typealias Index<Value: Indexable> = OneToManyIndexRepresentation<Instance, Value>
    
    /// A One-value to One-instance index.
    ///
    /// This type of index is typically used for most unique identifiers, and may be useful if there is an alternative unique identifier a instance may be referenced under.
    typealias OneToOneIndex<Value: Indexable & DiscreteIndexable> = OneToOneIndexRepresentation<Instance, Value>
    
    /// A Many-value to One-instance index.
    ///
    /// This type of index can be used if several alternative identifiers can reference an instance, and they all reside in a single property.
    typealias ManyToManyIndex<S: Sequence<Value>, Value: Indexable> = ManyToManyIndexRepresentation<Instance, S, Value>
    
    /// A Many-value to Many-instance index.
    ///
    /// This type of index is common when building relationships between different instances, where one instance may be related to several others in some way.
    typealias ManyToOneIndex<S: Sequence<Value>, Value: Indexable & DiscreteIndexable> = ManyToOneIndexRepresentation<Instance, S, Value>
    
    /// Map through the declared indexes, processing them as necessary.
    ///
    /// Although a default implementation is provided, this method can also be implemented manually by calling transform once for every index that should be registered.
    /// - Parameter transform: A transformation that will be called for every index.
    func mapIndexRepresentations(assertIdentifiable: Bool, transform: (GeneratedIndexRepresentation<Instance>) throws -> ()) rethrows
    
    /// Map through the declared indexes asynchronously, processing them as necessary.
    ///
    /// Although a default implementation is provided, this method can also be implemented manually by calling transform once for every index that should be registered.
    /// - Parameter transform: A transformation that will be called for every index.
    func mapIndexRepresentations(assertIdentifiable: Bool, transform: (GeneratedIndexRepresentation<Instance>) async throws -> ()) async rethrows
}

extension DatastoreFormat {
    public func mapIndexRepresentations(
        assertIdentifiable: Bool = false,
        transform: (GeneratedIndexRepresentation<Instance>) throws -> ()
    ) rethrows {
        let mirror = Mirror(reflecting: self)
        
        for child in mirror.children {
            guard let generatedIndex = generateIndexRepresentation(child: child, assertIdentifiable: assertIdentifiable)
            else { continue }
            
            try transform(generatedIndex)
        }
    }
    
    public func mapIndexRepresentations(
        assertIdentifiable: Bool = false,
        transform: (GeneratedIndexRepresentation<Instance>) async throws -> ()
    ) async rethrows {
        let mirror = Mirror(reflecting: self)
        
        for child in mirror.children {
            guard let generatedIndex = generateIndexRepresentation(child: child, assertIdentifiable: assertIdentifiable)
            else { continue }
            
            try await transform(generatedIndex)
        }
    }
    
    /// Generate an index representation for a given mirror's child, or return nil if no valid index was found.
    /// - Parameters:
    ///   - child: The child to introspect.
    ///   - assertIdentifiable: A flag to throw an assert if an `id` field was found when it would otherwise be a mistake.
    /// - Returns: The generated index representation, or nil if one could not be found.
    public func generateIndexRepresentation(
        child: Mirror.Child,
        assertIdentifiable: Bool = false
    ) -> GeneratedIndexRepresentation<Instance>? {
        guard let label = child.label else { return nil }
        
        let storage: IndexStorage
        let index: any IndexRepresentation<Instance>
        if let erasedIndexRepresentation = child.value as? any DirectIndexRepresentation,
           let matchingIndex = erasedIndexRepresentation.index(matching: Instance.self) {
            index = matchingIndex
            storage = .direct
        } else if let erasedIndexRepresentation = child.value as? any IndexRepresentation,
                  let matchingIndex = erasedIndexRepresentation.matches(Instance.self) {
            index = matchingIndex
            storage = .reference
        } else {
            return nil
        }
        
        let indexName = if label.prefix(1) == "_" {
            IndexName("\(label.dropFirst())")
        } else {
            IndexName(label)
        }
        
        /// If the type is identifiable, skip the `id` index as we always make one based on `id`
        if indexName == "id", Instance.self as? any Identifiable.Type != nil {
            if assertIdentifiable {
                assertionFailure("\(String(describing: Self.self)) declared `id` as an index, when the conformance is automatic since \(String(describing: Instance.self)) is Identifiable and \(String(describing: Self.self)).ID matches \(String(describing: Identifier.self)). Please remove the `id` member from the format.")
            }
            return nil
        }
        
        return GeneratedIndexRepresentation(
            indexName: indexName,
            index: index,
            storage: storage
        )
    }
}

extension DatastoreFormat where Instance: Identifiable, Instance.ID: Indexable & DiscreteIndexable {
    typealias Identifier = Instance.ID
}
