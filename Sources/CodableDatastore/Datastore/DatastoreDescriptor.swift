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
public struct DatastoreDescriptor: Equatable, Hashable {
    /// The version that was current at time of serialization.
    ///
    /// If a ``Datastore`` cannot decode this version, the datastore is presumed inaccessible, and any reads or writes will fail.
    public var version: Data
    
    /// The main type the ``Datastore`` serves.
    ///
    /// This type information is strictly informational — it can freely change between runs so long as the codable representations are compatible.
    public var instanceType: String
    
    /// The type used to identify instances in the ``Datastore``.
    ///
    /// If this type changes, the ``Datastore`` will invalidate and re-built the primary index, which is likely to be expensive for large data sets. If the type does not change, but its conformance of ``/Swift/Comparable`` does, a migration must be manually forced.
    public var identifierType: String
    
    /// The direct indexes the ``Datastore`` uses.
    ///
    /// Direct indexes duplicate the entire instance in their entries, which is useful for quick ranged reads. The identifier is implicitly a direct index, though other properties may also be used should they be applicable.
    ///
    /// If the index produces the same value, the identifier of the instance is implicitly used as a secondary sort parameter.
    public var directIndexes: [String : IndexDescriptor]
    
    /// The secondary indexes the ``Datastore`` uses.
    ///
    /// Secondary indexes store just the value being indexed, and point to the object in the primary datastore.
    ///
    /// If the index produces the same value, the identifier of the instance is implicitly used as a secondary sort parameter.
    public var referenceIndexes: [String : IndexDescriptor]
    
    /// The number of instances the ``Datastore`` manages.
    public var size: Int
}

extension DatastoreDescriptor {
    @available(*, deprecated, renamed: "instanceType", message: "Deprecated in favor of instanceType.")
    public var codedType: String {
        get { instanceType }
        set { instanceType = newValue }
    }
    
    @available(*, deprecated, renamed: "referenceIndexes", message: "Deprecated in favor of referenceIndexes.")
    public var secondaryIndexes: [String : IndexDescriptor] {
        get { referenceIndexes }
        set { referenceIndexes = newValue }
    }
}

extension DatastoreDescriptor: Codable {
    enum CodingKeys: CodingKey {
        case version
        case instanceType
        case codedType // Deprecated
        case identifierType
        case directIndexes
        case referenceIndexes
        case secondaryIndexes // Deprecated
        case size
    }
    
    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.version = try container.decode(Data.self, forKey: .version)
        self.instanceType = try container.decodeIfPresent(String.self, forKey: .instanceType) ?? container.decode(String.self, forKey: .codedType)
        self.identifierType = try container.decode(String.self, forKey: .identifierType)
        self.directIndexes = try container.decode([String : DatastoreDescriptor.IndexDescriptor].self, forKey: .directIndexes)
        self.referenceIndexes = try container.decodeIfPresent([String : DatastoreDescriptor.IndexDescriptor].self, forKey: .referenceIndexes) ?? container.decode([String : DatastoreDescriptor.IndexDescriptor].self, forKey: .secondaryIndexes)
        self.size = try container.decode(Int.self, forKey: .size)
    }
    
    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(self.version, forKey: .version)
        try container.encode(self.instanceType, forKey: .instanceType)
        try container.encode(self.identifierType, forKey: .identifierType)
        try container.encode(self.directIndexes, forKey: .directIndexes)
        try container.encode(self.referenceIndexes, forKey: .referenceIndexes)
        try container.encode(self.size, forKey: .size)
    }
}

extension DatastoreDescriptor {
    /// A description of an Index used by a ``Datastore``.
    ///
    /// This information is used to determine which indexes must be invalidated or re-built, and which can be used as is. Additionally, it informs which properties must be reported along with any writes to keep existing indexes up to date.
    public struct IndexDescriptor: Codable, Equatable, Hashable, Comparable {
        /// The version that was first used to persist an index to disk.
        ///
        /// This is used to determine if an index must be re-built purely because something about how the index changed in a way that could not be automatically determined, such as Codable conformance changing.
        public var version: Data
        
        /// The key this index is based on.
        ///
        /// Each index is uniquely referred to by their name. If a ``Datastore`` reports a set of Indexes with a different set of keys than was used prior, the difference between them will be calculated, and older indexes will be invalidated while new ones will be built.
        public var name: IndexName
        
        /// The type the index uses for ordering.
        ///
        /// The index will use the type to automatically determine if an index should be invalidated.
        public var type: IndexType
        
        public static func < (lhs: Self, rhs: Self) -> Bool {
            lhs.name < rhs.name
        }
    }
}

extension DatastoreDescriptor {
    /// Initialize a descriptor from types a ``Datastore`` deals in directly.
    /// 
    /// This will use Swift reflection to infer the indexes from the conforming ``DatastoreFormat`` instance.
    ///
    /// - Parameters:
    ///   - format:The format of the datastore as described by the caller.
    ///   - version: The current version being used by a data store.
    init<Format: DatastoreFormat>(
        format: Format,
        version: Format.Version
    ) throws {
        let versionData = try Data(version)
        
        var directIndexes: [String : IndexDescriptor] = [:]
        var referenceIndexes: [String : IndexDescriptor] = [:]
        
        format.mapReferenceIndexes { indexName, index in
            guard Format.Instance.self as? any Identifiable.Type == nil || indexName != "id"
            else { return }
            
            let indexDescriptor = IndexDescriptor(
                version: versionData,
                name: indexName,
                type: index.indexType
            )
            
            referenceIndexes[indexName.rawValue] = indexDescriptor
        }
        
        format.mapDirectIndexes { indexName, index in
            guard Format.Instance.self as? any Identifiable.Type == nil || indexName != "id"
            else { return }
            
            let indexDescriptor = IndexDescriptor(
                version: versionData,
                name: indexName,
                type: index.indexType
            )
            
            /// Make sure the reference indexes don't contain any of the direct indexes
            referenceIndexes.removeValue(forKey: indexName.rawValue)
            directIndexes[indexName.rawValue] = indexDescriptor
        }
        
        self.init(
            version: versionData,
            instanceType: String(describing: Format.Instance.self),
            identifierType: String(describing: Format.Identifier.self),
            directIndexes: directIndexes,
            referenceIndexes: referenceIndexes,
            size: 0
        )
    }
}
