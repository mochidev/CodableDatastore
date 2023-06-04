//
//  DiskPersistence.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

public actor DiskPersistence: Persistence {
    
}

extension DiskPersistence: _Persistence {
    public func withTransaction(_ transaction: (DiskPersistence) -> ()) async throws {
        
    }
}
