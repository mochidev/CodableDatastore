//
//  ISO8601DateFormatter+Milliseconds.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-07.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Foundation

private struct GlobalDateFormatter: Sendable {
    @available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *)
    static let cachedFormatter = Date.ISO8601FormatStyle(includingFractionalSeconds: true)
    
    static let parse: @Sendable (_ value: String) -> Date? = {
        #if canImport(FoundationEssentials)
        return { try? cachedFormatter.parse($0) }
        #else
        if #available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *) {
            return { try? cachedFormatter.parse($0) }
        } else {
            return { ISO8601DateFormatter.withMilliseconds.date(from: $0) }
        }
        #endif
    }()
    
    static let format: @Sendable (_ value: Date) -> String = {
        #if canImport(FoundationEssentials)
        return { cachedFormatter.format($0) }
        #else
        if #available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *) {
            return { cachedFormatter.format($0) }
        } else {
            return { ISO8601DateFormatter.withMilliseconds.string(from: $0) }
        }
        #endif
    }()
}

#if !canImport(FoundationEssentials)
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
#endif // !canImport(FoundationEssentials)

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
