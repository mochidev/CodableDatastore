//
//  Dictionary+RawRepresentable.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-20.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

extension Dictionary {
    @usableFromInline
    subscript(key: some RawRepresentable<Key>) -> Value? {
        get {
            return self[key.rawValue]
        }
        set(newValue) {
            self[key.rawValue] = newValue
        }
        _modify {
            defer { _fixLifetime(self) }
            yield &self[key.rawValue]
        }
    }
    
    @discardableResult
    @usableFromInline
    mutating func removeValue(forKey key: some RawRepresentable<Key>) -> Value? {
        removeValue(forKey: key.rawValue)
    }
}
