//
//  JSONCoder.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-14.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
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
