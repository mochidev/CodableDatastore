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
/// A ``DatastoreFormat`` will be instantiated and owned by the datastore associated with it to provide both type and index information to the store. It is expected to represent the ideal types for the latest version of the code that is instantiating the datastore.
///
/// This type also exists so implementers can conform a `struct` to it that declares a number of key paths as stored properties.
///
/// Conformers can create subtypes for their versioned models either in the body of their struct or in legacy extensions. Additionally, you are encouraged to make **static** properties available for things like the current version, or a configured ``Datastore`` — this allows easy access to them without mucking around declaring them in far-away places in your code base.
///
/// ```swift
/// struct BooksFormat {
///     static let defaultKey: DatastoreKey = "BooksStore"
///     static let currentVersion: Version = .current
///
///     enum Version: String {
///         case v1 = "2024-04-01"
///         case current = "2024-04-09"
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
///     static func datastore(for persistence: DiskPersistence<ReadWrite>) -> Datastore {
///         .JSONStore(
///             persistence: persistence,
///             migrations: [
///                 .v1: { data, decoder in
///                     Book(try decoder.decode(BookV1.self, from: data))
///                 },
///                 .current: { data, decoder in
///                     try decoder.decode(Book.self, from: data)
///                 }
///             ]
///         )
///     }
///
///     let title = Index(\.title)
///     let author = ManyToManyIndex(\.authors)
///     let isbn = OneToOneIndex(\.isbn)
/// }
///
/// typealias Book = BooksFormat.Book
///
/// extension Book {
///     init(_ bookV1: BooksFormat.BookV1) {
///         self.init(
///             id: id,
///             title: SortableTitle(title),
///             authors: [AuthorID(authors)],
///             isbn: ISBN.generate()
///         )
///     }
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
    
    /// Generate index representations for the datastore.
    ///
    /// The default implementation will create an entry for each member of the conforming type that is an ``IndexRepresentation`` type. If two members represent the _same_ type, only the one with the name that sorts _earliest_ will be used. Only stored members will be evaluated — computed members will be skipped.
    ///
    /// It is recommended that these results should be cached rather than re-generated every time.
    ///
    /// - Important: It is up to the implementer to ensure that no two _indexes_ refer to the same index name. Doing so is a mistake and will result in undefined behavior not guaranteed by the library, likely indexes being invalidated on different runs of your app.
    ///
    /// - Parameter assertIdentifiable: A flag to throw an assert if an `id` field was found when it would otherwise be a mistake.
    /// - Returns: A mapping between unique indexes and their usable metadata.
    func generateIndexRepresentations(assertIdentifiable: Bool) -> [AnyIndexRepresentation<Instance> : GeneratedIndexRepresentation<Instance>]
}

extension DatastoreFormat {
    public func generateIndexRepresentations(assertIdentifiable: Bool = false) -> [AnyIndexRepresentation<Instance> : GeneratedIndexRepresentation<Instance>] {
        let mirror = Mirror(reflecting: self)
        var results: [AnyIndexRepresentation<Instance> : GeneratedIndexRepresentation<Instance>] = [:]
        
        for child in mirror.children {
            guard 
                let generatedIndex = generateIndexRepresentation(child: child, assertIdentifiable: assertIdentifiable)
            else { continue }
            
            let key = AnyIndexRepresentation(indexRepresentation: generatedIndex.index)
            /// If two indexes share a name, use the one that sorts earlier.
            if let oldIndex = results[key], oldIndex.indexName < generatedIndex.indexName { continue }
            
            /// Otherwise replace it with the current index.
            results[key] = generatedIndex
        }
        
        return results
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

extension DatastoreFormat {
    /// A typealias of the read-write datastore this format describes.
    public typealias Datastore = CodableDatastore.Datastore<Self, ReadWrite>
    
    /// A typealias of the read-only datastore this format describes.
    public typealias ReadOnlyDatastore = CodableDatastore.Datastore<Self, ReadOnly>
}

//extension DatastoreFormat where Instance: Identifiable, Instance.ID: Indexable & DiscreteIndexable, Self.Identifier == Instance.ID {
//    @available(*, unavailable, message: "id is reserved on Identifiable Instance types.")
//    var id: Never { preconditionFailure("id is reserved on Identifiable Instance types.") }
//}

extension DatastoreFormat where Instance: Identifiable, Instance.ID: Indexable & DiscreteIndexable {
    public typealias Identifier = Instance.ID
}
