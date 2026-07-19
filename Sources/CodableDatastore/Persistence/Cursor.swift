//
//  Cursor.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-17.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

/// An opaque type ``Persistence``s may use to indicate a position in their storage.
///
/// - Note: A cursor is only valid within the same transaction for the same persistence it was created for.
public protocol CursorProtocol<P>: Sendable {
    associatedtype P: Persistence
    var persistence: P { get }
    
//    var transaction: Transaction<P> { get }
}

/// An opaque type ``Persistence``s may use to indicate the position of an instance in their storage.
public protocol InstanceCursorProtocol<P>: InsertionCursorProtocol {}

/// An opaque type ``Persistence``s may use to indicate the position a new instance should be inserted in their storage.
public protocol InsertionCursorProtocol<P>: CursorProtocol {}
