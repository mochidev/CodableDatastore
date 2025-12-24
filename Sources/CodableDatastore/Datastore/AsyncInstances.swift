//
//  AsyncInstances.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-12.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

public protocol AsyncInstances<Element>: AsyncSequence, Sendable {}

extension AsyncInstances {
    /// Returns the first instance of the sequence, if it exists.
    ///
    /// - Returns: The first instance of the sequence, or `nil` if the sequence is empty.
    public var firstInstance: Element? {
        get async throws { try await first { _ in true } }
    }
    
    /// Copy and collect all instances in a given load operation into a single array.
    ///
    /// Do not use this method if you need to know if more instances exist past the proposed collection limit, and use a `for try await ... in ...` loop instead to properly handle that case.
    ///
    /// - Warning: This method is only safe to use from sequences vended by a Datastore ranged ``Datastore/load(range:order:)`` operation as they guarantee that the returned sequence won't stall due to unavailable instances. Do not use it when collecting observations as there is no guarantee observations will be returned!
    /// - Parameter collectionLimit: The maximum amount of entries to collect. Specify `.infinity` to _questionably_ collect all instances.
    /// - Returns: An array of instances up to the collection limit.
    public func collectInstances(upTo collectionLimit: Int) async throws -> [Element] {
        var instances: [Element] = []
        for try await instance in self {
            instances.append(instance)
            
            guard instances.count <= collectionLimit
            else { return instances }
        }
        return instances
    }
    
    /// Copy and collect all instances in a given load operation into a single array.
    ///
    /// Do not use this method if you need to know if more instances exist past the proposed collection limit, and use a `for try await ... in ...` loop instead to properly handle that case.
    ///
    /// - Warning: This method is only safe to use from sequences vended by a Datastore ranged ``Datastore/load(range:order:)`` operation as they guarantee that the returned sequence won't stall due to unavailable instances. Do not use it when collecting observations as there is no guarantee observations will be returned!
    /// - Parameter collectionLimit: The maximum amount of entries to collect. Specify `.infinity` to _questionably_  collect all instances.
    /// - Returns: An array of instances up to the collection limit.
    public func collectInstances(upTo collectionLimit: AsyncInstancesLimit) async throws -> [Element] {
        try await collectInstances(upTo: .max)
    }
}

public enum AsyncInstancesLimit {
    /// Load all the instances in a given range.
    ///
    /// - Warning: Unless you know in advance that the amount of entries absolutely fits in working memory, collecting all instances is extremently ill-advised.
    case infinity
}

// MARK: - Standard Library Conformances

extension AsyncMapSequence: AsyncInstances {}
extension AsyncThrowingMapSequence: AsyncInstances {}
extension AsyncCompactMapSequence: AsyncInstances {}
extension AsyncThrowingCompactMapSequence: AsyncInstances {}
extension AsyncFilterSequence: AsyncInstances {}
extension AsyncThrowingFilterSequence: AsyncInstances {}
