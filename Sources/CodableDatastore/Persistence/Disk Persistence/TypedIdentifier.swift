//
//  TypedIdentifier.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-10.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

struct TypedIdentifier<T>: TypedIdentifierProtocol {
    var rawValue: String
    
    init(rawValue: String) {
        self.rawValue = rawValue
    }
}

protocol TypedIdentifierProtocol: RawRepresentable, Codable, Equatable, Hashable, CustomStringConvertible {
    var rawValue: String { get }
    init(rawValue: String)
}

extension TypedIdentifierProtocol {
    init(from decoder: Decoder) throws {
        let rawValue = try decoder.singleValueContainer().decode(String.self)
        self.init(rawValue: rawValue)
    }
    
    func encode(to encoder: Encoder) throws {
        var encoder = encoder.singleValueContainer()
        try encoder.encode(rawValue)
    }
    
    var description: String { rawValue }
}
