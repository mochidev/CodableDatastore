//
//  IndexStorage.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-04-10.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

/// Indicates how instances are stored in the presistence.
public enum IndexStorage: Sendable {
    /// Instances are stored in the index directly, requiring no further reads to access them.
    case direct
    
    /// Instances are only references in the index, and must be fetched in the principle index to complete a read.
    case reference
}
