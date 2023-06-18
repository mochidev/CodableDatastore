//
//  IndexPath.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-13.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

/// A keypath with an associated name.
public struct IndexPath<Root>: Equatable, Hashable {
    /// The ``/Swift/KeyPath`` associated with the index path.
    public let keyPath: KeyPath<Root, _AnyIndexed>
    
    /// The path as a string.
    public let path: String
    
    /// Initialize a new ``IndexPath``.
    ///
    /// - Note: It is preferable to use the #indexPath macro instead, as it will infer the path automatically.
    /// - Parameters:
    ///   - uncheckedKeyPath: The keypath to bind to.
    ///   - path: The name of the path as a string, which should match the keypath itself.
    public init(uncheckedKeyPath: KeyPath<Root, _AnyIndexed>, path: String) {
        self.keyPath = uncheckedKeyPath
        self.path = path
    }
}

extension IndexPath: Comparable {
    public static func < (lhs: Self, rhs: Self) -> Bool {
        lhs.path < rhs.path
    }
}

extension Encodable {
    subscript(keyPath indexPath: IndexPath<Self>) -> _AnyIndexed {
        return self[keyPath: indexPath.keyPath]
    }
}
