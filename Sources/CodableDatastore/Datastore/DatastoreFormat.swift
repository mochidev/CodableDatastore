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
/// Conformers can create subtypes for their versioned models either in the body of their struct or in legacy extentions. Additionally, you are encouraged to make **static** properties available for things like the current version, or a configured ``Datastore`` — this allows easy access to them without mucking around declaring them in far-away places in your code base.
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
    associatedtype Identifier: Indexable
    
    init()
}

extension DatastoreFormat where Instance: Identifiable, Instance.ID: Indexable {
    typealias Identifier = Instance.ID
}
