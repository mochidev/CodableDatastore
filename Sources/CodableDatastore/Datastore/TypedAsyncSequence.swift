//
//  AsyncInstances.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-12.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

public protocol AsyncInstances<Element>: AsyncSequence, Sendable {}

// MARK: - Standard Library Conformances

extension AsyncMapSequence: AsyncInstances {}
extension AsyncThrowingMapSequence: AsyncInstances {}
extension AsyncCompactMapSequence: AsyncInstances {}
extension AsyncThrowingCompactMapSequence: AsyncInstances {}
extension AsyncFilterSequence: AsyncInstances {}
extension AsyncThrowingFilterSequence: AsyncInstances {}
