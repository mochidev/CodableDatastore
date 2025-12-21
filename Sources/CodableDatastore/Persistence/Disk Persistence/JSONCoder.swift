//
//  JSONCoder.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-14.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

extension JSONEncoder {
    static let shared: JSONEncoder = {
        let datastoreEncoder = JSONEncoder()
        datastoreEncoder.dateEncodingStrategy = .iso8601WithMilliseconds
#if DEBUG
        datastoreEncoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
#else
        datastoreEncoder.outputFormatting = [.withoutEscapingSlashes]
#endif
        return datastoreEncoder
    }()
}

extension JSONDecoder {
    static let shared: JSONDecoder = {
        let datastoreDecoder = JSONDecoder()
        datastoreDecoder.dateDecodingStrategy = .iso8601WithMilliseconds
        return datastoreDecoder
    }()
}

#if !canImport(Darwin) && compiler(<6.2)
extension JSONEncoder: @unchecked Sendable {}
extension JSONDecoder: @unchecked Sendable {}
#endif
