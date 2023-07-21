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

protocol TypedIdentifierProtocol: RawRepresentable, Codable, Equatable, Hashable, CustomStringConvertible, Comparable {
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

extension TypedIdentifierProtocol {
    init(
        name: String,
        token: UInt64 = .random(in: UInt64.min...UInt64.max)
    ) {
        let fileSafeName = name.reduce(into: "") { partialResult, character in
            /// We only care about the first 16 characters
            guard partialResult.count < 16 else { return }
            
            /// Filter out any chatacters that could mess with filenames
            guard character.isASCII && (character.isLetter || character.isNumber || character == "_" || character == " ") else { return }
            
            partialResult.append(character)
        }
        
        let stringToken = String(token, radix: 16, uppercase: true)
        self.init(rawValue: "\(fileSafeName)-\(String(repeating: "0", count: 16-stringToken.count))\(stringToken)")
    }
    
    init(
        name: some RawRepresentable<String>,
        token: UInt64 = .random(in: UInt64.min...UInt64.max)
    ) {
        self.init(name: name.rawValue, token: token)
    }
}

extension TypedIdentifierProtocol {
    static func < (lhs: Self, rhs: Self) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}
