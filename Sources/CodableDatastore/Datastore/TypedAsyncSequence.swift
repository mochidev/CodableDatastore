//
//  TypedAsyncSequence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-12.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

public protocol TypedAsyncSequence<Element>: AsyncSequence {}

// MARK: - Standard Library Conformances

extension AsyncMapSequence: TypedAsyncSequence {}
extension AsyncThrowingMapSequence: TypedAsyncSequence {}
extension AsyncCompactMapSequence: TypedAsyncSequence {}
extension AsyncThrowingCompactMapSequence: TypedAsyncSequence {}
extension AsyncFilterSequence: TypedAsyncSequence {}
extension AsyncThrowingFilterSequence: TypedAsyncSequence {}
