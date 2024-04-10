//
//  IndexStorage.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-04-10.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

/// Indicates how instances are stored in the presistence.
public enum IndexStorage {
    /// Instances are stored in the index directly, requiring no further reads to access them.
    case direct
    
    /// Instances are only references in the index, and must be fetched in the principle index to complete a read.
    case reference
}
