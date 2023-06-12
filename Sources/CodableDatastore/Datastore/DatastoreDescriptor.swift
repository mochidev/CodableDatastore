//
//  DatastoreDescriptor.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-11.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// A description of a ``Datastore``'s requirements of a persistence.
///
/// A persistence is expected to save a description and retrieve it when a connected ``Datastore`` requests it. The ``Datastore`` then uses it to compute if indexes need to be invalidated or re-built.
public struct DatastoreDescriptor: Codable, Equatable, Hashable {
    /// The version that was current at time of serialization.
    ///
    /// If a ``Datastore`` cannot decode this version, the datastore is presumed innaccessible, and any reads or writes will fail.
    public var version: Data
    
    /// The main type the ``Datastore`` serves.
    ///
    /// This type information is strictly informal — it can freely change between runs so long as the codable representations are compatible.
    public var codedType: String
    
    /// The type used to identify instances in the ``Datastore``.
    ///
    /// If this type changes, the ``Datastore`` will invalidate and re-built the primary index, which is likely to be expensive for large data sets. If the type does not change, but its conformance of ``/Swift/Comparable`` does, a migration must be manually forced.
    public var identifierType: String
    
    /// The direct indexes the ``Datastore`` uses.
    ///
    /// Direct indexes duplicate the entire instance in their entries, which is useful for quick ranged reads. The identifier is implicitely a direct index, though other properties may also be used should they be applicable.
    ///
    /// If the index produces the same value, the identifier of the instance is implicitely used as a secondary sort parameter.
    public var directIndexes: [String : IndexDescriptor]
    
    /// The secondary indexes the ``Datastore`` uses.
    ///
    /// Secondary indexes store just the value being indexed, and point to the object in the primary datastore.
    ///
    /// If the index produces the same value, the identifier of the instance is implicitely used as a secondary sort parameter.
    public var secondaryIndexes: [String : IndexDescriptor]
}

extension DatastoreDescriptor {
    /// A description of an Index used by a ``Datastore``.
    ///
    /// This information is used to determine which indexes must be invalidated or re-built, and which can be used as is. Additionally, it informs which properties must be reported along with any writes to keep existing indexes up to date.
    public struct IndexDescriptor: Codable, Equatable, Hashable, Comparable {
        /// The version that was first used to persist an index to disk.
        ///
        /// This is used to determine if an index must be re-built purely because something about how the index changed in a way that could not be automatically determined, sunce as Codable conformance changing.
        public var version: Data
        
        /// The key this index is based on.
        ///
        /// Each index is uniquely referred to by their key. If a ``Datastore`` reports a set of Indexes with a different set of keys than was used prior, the difference between them will be calculated, and older indexes will be invalidated while new ones will be built.
        public var key: String
        
        /// The type the index uses for ordering.
        ///
        /// The index will use the type to automatically determine if an index should be invalidated.
        public var indexType: String
        
        public static func < (lhs: Self, rhs: Self) -> Bool {
            lhs.key < rhs.key
        }
    }
}

extension DatastoreDescriptor {
    /// Initialize a descriptor from types a ``Datastore`` deals in directly.
    ///
    /// This will use Swift reflection to inder the indexable properties from those that use the @``Indexed`` property wrapper.
    ///
    /// - Parameters:
    ///   - version: The current version being used by a data store.
    ///   - sampleInstance: A sample instance to use reflection on.
    ///   - identifierType: The identifier type the data store was created with.
    ///   - directIndexKeypaths: A list of direct indexes to describe from the sample instance.
    ///   - computedIndexKeypaths: Additional secondary indexes to describe from the same instance.
    init<
        Version: RawRepresentable & Hashable & CaseIterable,
        CodedType: Codable,
        IdentifierType: Indexable
    >(
        version: Version,
        sampleInstance: CodedType,
        identifierType: IdentifierType.Type,
        directIndexes directIndexKeypaths: [KeyPath<CodedType, _AnyIndexed>],
        computedIndexes computedIndexKeypaths: [KeyPath<CodedType, _AnyIndexed>]
    ) throws where Version.RawValue: Indexable {
        let datastoreEncoder = JSONEncoder()
        datastoreEncoder.dateEncodingStrategy = .iso8601WithMilliseconds
        let versionData = try datastoreEncoder.encode(version.rawValue)
        
        var directIndexes: Set<IndexDescriptor> = []
        var secondaryIndexes: Set<IndexDescriptor> = []
        
        for keypath in computedIndexKeypaths {
            let indexDescriptor = IndexDescriptor(
                version: versionData,
                sampleInstance: sampleInstance,
                keypath: keypath
            )
            
            /// If the type is identifiable, skip the `id` index as we always make one based on `id`
            if indexDescriptor.key == "$id" && sampleInstance is any Identifiable {
                continue
            }
            
            secondaryIndexes.insert(indexDescriptor)
        }
        
        for keypath in directIndexKeypaths {
            let indexDescriptor = IndexDescriptor(
                version: versionData,
                sampleInstance: sampleInstance,
                keypath: keypath
            )
            
            /// If the type is identifiable, skip the `id` index as we always make one based on `id`
            if indexDescriptor.key == "$id" && sampleInstance is any Identifiable {
                continue
            }
            
            /// Make sure the secondary indexes don't contain any of the direct indexes
            secondaryIndexes.remove(indexDescriptor)
            directIndexes.insert(indexDescriptor)
        }
        
        let mirror = Mirror(reflecting: sampleInstance)
        
        for child in mirror.children {
            guard let label = child.label else { continue }
            guard let childValue = child.value as? any _Indexed else { continue }
            
            let actualKey: String
            if label.prefix(1) == "_" {
                actualKey = "$\(label.dropFirst())"
            } else {
                actualKey = label
            }
            
            let indexDescriptor = IndexDescriptor(
                version: versionData,
                key: actualKey,
                indexType: childValue.projectedValue.indexedType
            )
            
            /// If the type is identifiable, skip the `id` index as we always make one based on `id`
            if indexDescriptor.key == "$id" && sampleInstance is any Identifiable {
                continue
            }
            
            if !directIndexes.contains(indexDescriptor) {
                secondaryIndexes.insert(indexDescriptor)
            }
        }
        
        self.init(
            version: versionData,
            codedType: String(describing: type(of: sampleInstance)),
            identifierType: String(describing: identifierType),
            directIndexes: Dictionary(uniqueKeysWithValues: directIndexes.map({ ($0.key, $0) })),
            secondaryIndexes: Dictionary(uniqueKeysWithValues: secondaryIndexes.map({ ($0.key, $0) }))
        )
    }
}

extension DatastoreDescriptor.IndexDescriptor {
    /// Initialize a descriptor from a key path.
    ///
    /// - Parameters:
    ///   - version: The current version being used by a data store.
    ///   - sampleInstance: A sample instance to probe for type information.
    ///   - keypath: The keypath to the indexed property.
    init<CodedType>(
        version: Data,
        sampleInstance: CodedType,
        keypath: KeyPath<CodedType, _AnyIndexed>
    ) {
        let indexType = sampleInstance[keyPath: keypath].indexedType
        
        let fullKeyPath = String(describing: keypath)
        let rootComponent = "\\\(String(describing: CodedType.self))."
        let path = String(fullKeyPath.dropFirst(rootComponent.count))
        
        self.init(
            version: version,
            key: path,
            indexType: indexType
        )
    }
}
