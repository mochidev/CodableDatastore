//
//  RawRepresentable+Codable.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-15.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

extension RawRepresentable where RawValue: Codable {
    init(_ data: Data) throws {
        let rawValue = try JSONDecoder.shared.decode(RawValue.self, from: data)
        guard let instance = Self(rawValue: rawValue) else {
            throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Raw value could not be used to initialize \(String(describing: Self.self))"))
        }
        self = instance
    }
}

extension Data {
    init<T: RawRepresentable>(_ rawRepresentable: T) throws where T.RawValue: Codable {
        self = try JSONEncoder.shared.encode(rawRepresentable.rawValue)
    }
}
