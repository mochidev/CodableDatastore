//
//  AccessMode.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-10.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

/// An AccessMode marker type.
public protocol _AccessMode: Sendable {}

/// A marker type that indicates read-only access.
public enum ReadOnly: _AccessMode {}

/// A marker type that indicates read-write access.
public enum ReadWrite: _AccessMode {}
