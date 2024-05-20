//
//  DatastoreKey.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-01.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

public struct DatastoreKey: RawRepresentable, Hashable, Comparable, Sendable {
    public var rawValue: String
    
    public init(rawValue: String) {
        self.rawValue = rawValue
    }
    
    public init(_ rawValue: String) {
        self.rawValue = rawValue
    }
    
    public static func < (lhs: Self, rhs: Self) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

extension DatastoreKey: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(rawValue: value)
    }
}

extension DatastoreKey: Decodable {
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.init(rawValue: try container.decode(String.self))
    }
}

extension DatastoreKey: Encodable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }
}
