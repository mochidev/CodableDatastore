//
//  Mirror+Indexed.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-18.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

extension Mirror {
    static func indexedChildren<T: Codable>(
        from instance: T?,
        assertIdentifiable: Bool = false,
        transform: (_ indexName: String, _ value: any _IndexedProtocol) throws -> ()
    ) rethrows {
        guard let instance else { return }
        
        let mirror = Mirror(reflecting: instance)
        
        for child in mirror.children {
            guard let label = child.label else { continue }
            guard let childValue = child.value as? any _IndexedProtocol else { continue }
            
            let indexName: String
            if label.prefix(1) == "_" {
                indexName = "$\(label.dropFirst())"
            } else {
                indexName = label
            }
            
            /// If the type is identifiable, skip the `id` index as we always make one based on `id`
            if indexName == "$id" && instance is any Identifiable {
                if assertIdentifiable {
                    assertionFailure("\(type(of: instance)) declared `id` to be @Indexed, when the conformance is automatic. Please remove @Indexed from the `id` field.")
                }
                continue
            }
            
            try transform(indexName, childValue)
        }
    }
    
    static func indexedChildren<T: Codable>(
        from instance: T?,
        assertIdentifiable: Bool = false,
        transform: (_ indexName: String, _ value: any _IndexedProtocol) async throws -> ()
    ) async rethrows {
        guard let instance else { return }
        
        let mirror = Mirror(reflecting: instance)
        
        for child in mirror.children {
            guard let label = child.label else { continue }
            guard let childValue = child.value as? any _IndexedProtocol else { continue }
            
            let indexName: String
            if label.prefix(1) == "_" {
                indexName = "$\(label.dropFirst())"
            } else {
                indexName = label
            }
            
            /// If the type is identifiable, skip the `id` index as we always make one based on `id`
            if indexName == "$id" && instance is any Identifiable {
                assertionFailure("\(type(of: instance)) declared `id` to be @Indexed, when the conformance is automatic. Please remove @Indexed from the `id` field.")
                continue
            }
            
            try await transform(indexName, childValue)
        }
    }
}
