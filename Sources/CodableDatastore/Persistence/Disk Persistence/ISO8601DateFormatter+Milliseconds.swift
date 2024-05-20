//
//  ISO8601DateFormatter+Milliseconds.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-07.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

extension ISO8601DateFormatter {
    static let withMilliseconds: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.formatOptions = [
            .withInternetDateTime,
            .withDashSeparatorInDate,
            .withColonSeparatorInTime,
            .withTimeZone,
            .withFractionalSeconds
        ]
        return formatter
    }()
}

extension JSONDecoder.DateDecodingStrategy {
    static let iso8601WithMilliseconds: Self = custom { decoder in
        let container = try decoder.singleValueContainer()
        let string = try container.decode(String.self)
        guard let date = ISO8601DateFormatter.withMilliseconds.date(from: string) else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Invalid date: \(string)")
        }
        return date

    }
}

extension JSONEncoder.DateEncodingStrategy {
    static let iso8601WithMilliseconds: Self = custom { date, encoder in
        let string = ISO8601DateFormatter.withMilliseconds.string(from: date)
        var container = encoder.singleValueContainer()
        try container.encode(string)
    }
}

#if !canImport(Darwin)
extension ISO8601DateFormatter: @unchecked Sendable {}
extension JSONDecoder.DateDecodingStrategy: @unchecked Sendable {}
extension JSONEncoder.DateEncodingStrategy: @unchecked Sendable {}
#endif
