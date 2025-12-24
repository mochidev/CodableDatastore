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
}

// MARK: - Standard Library Conformances

extension AsyncMapSequence: AsyncInstances {}
extension AsyncThrowingMapSequence: AsyncInstances {}
extension AsyncCompactMapSequence: AsyncInstances {}
extension AsyncThrowingCompactMapSequence: AsyncInstances {}
extension AsyncFilterSequence: AsyncInstances {}
extension AsyncThrowingFilterSequence: AsyncInstances {}
