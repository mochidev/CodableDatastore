//
//  Cursor.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-17.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

/// An opaque type ``Persistence``s may use to indicate a position in their storage.
///
/// - Note: A cursor is only valid within the same transaction for the same persistence it was created for.
public protocol Cursor<P> {
    associatedtype P: Persistence
    var persistence: P { get }
    
//    var transaction: Transaction<P> { get }
}

/// An opaque type ``Persistence``s may use to indicate the position of an instance in their storage.
public protocol InstanceCursor: Cursor {}

/// An opaque type ``Persistence``s may use to indicate the position a new instance should be inserted in their storage.
public protocol InsertionCursor: Cursor {}
