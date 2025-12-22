//
//  ISO8601DateFormatter+Milliseconds.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-07.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

private struct GlobalDateFormatter: Sendable {
    @available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *)
    static let cachedFormatter = Date.ISO8601FormatStyle(includingFractionalSeconds: true)
    
    static let parse: @Sendable (_ value: String) -> Date? = {
        if #available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *) {
            return { try? cachedFormatter.parse($0) }
        } else {
            return { ISO8601DateFormatter.withMilliseconds.date(from: $0) }
        }
    }()
    
    static let format: @Sendable (_ value: Date) -> String = {
        if #available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *) {
            return { cachedFormatter.format($0) }
        } else {
            return { ISO8601DateFormatter.withMilliseconds.string(from: $0) }
        }
    }()
}

private extension ISO8601DateFormatter {
    nonisolated(unsafe) static let withMilliseconds: ISO8601DateFormatter = {
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
        guard let date = GlobalDateFormatter.parse(string) else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Invalid date: \(string)")
        }
        return date

    }
}

extension JSONEncoder.DateEncodingStrategy {
    static let iso8601WithMilliseconds: Self = custom { date, encoder in
        let string = GlobalDateFormatter.format(date)
        var container = encoder.singleValueContainer()
        try container.encode(string)
    }
}

#if compiler(>=6) && compiler(<6.2)
extension ISO8601DateFormatter: @unchecked @retroactive Sendable {}
extension JSONDecoder.DateDecodingStrategy: @unchecked Sendable {}
extension JSONEncoder.DateEncodingStrategy: @unchecked Sendable {}
#endif
