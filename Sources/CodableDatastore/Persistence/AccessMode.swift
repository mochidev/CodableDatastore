//
//  AccessMode.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

/// An AccessMode marker type.
public protocol _AccessMode {}

/// A marker type that indicates read-only access.
public enum ReadOnly: _AccessMode {}

/// A marker type that indicates read-write access.
public enum ReadWrite: _AccessMode {}
