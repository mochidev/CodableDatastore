//
//  DatedIdentifier.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-08.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

struct Identifier<T>: DatedIdentifier {
    var rawValue: String
    
    init(rawValue: String) {
        self.rawValue = rawValue
    }
}

protocol DatedIdentifier: RawRepresentable, Codable, Equatable {
    var rawValue: String { get }
    init(rawValue: String)
}

extension DatedIdentifier {
    init(from decoder: Decoder) throws {
        let rawValue = try decoder.singleValueContainer().decode(String.self)
        self.init(rawValue: rawValue)
    }
    
    func encode(to encoder: Encoder) throws {
        var encoder = encoder.singleValueContainer()
        try encoder.encode(rawValue)
    }
    
    init(
        date: Date = Date(),
        token: UInt64 = .random(in: UInt64.min...UInt64.max)
    ) {
        let stringToken = String(token, radix: 16, uppercase: true)
        self.init(rawValue: "\(DateFormatter.datedFormatter.string(from: date)) \(String(repeating: "0", count: 16-stringToken.count))\(stringToken)")
    }
    
    var components: DatedIdentifierComponents {
        get throws {
            try DatedIdentifierComponents(self)
        }
    }
}

struct DatedIdentifierComponents {
    var year: String
    var month: String
    var day: String
    
    var hour: String
    var minute: String
    var second: String
    
    var token: String
    
    init(_ identifier: some DatedIdentifier) throws {
        let rawString = identifier.rawValue
        guard rawString.count == 36 else {
            throw DatedIdentifierError.invalidLength
        }
        
        year = rawString[0..<4]
        month = rawString[5..<7]
        day = rawString[8..<10]
        hour = rawString[11..<13]
        minute = rawString[14..<16]
        second = rawString[17..<19]
        token = rawString[20..<36]
    }
    
    var monthDay: String {
        "\(month)-\(day)"
    }
    var hourMinute: String {
        "\(hour)-\(minute)"
    }
}


enum DatedIdentifierError: LocalizedError, Equatable {
    case invalidLength
    
    var errorDescription: String? {
        switch self {
        case .invalidLength: return "The identifier must be 36 characters long."
        }
    }
}

private extension StringProtocol {
    subscript (intRange: Range<Int>) -> String {
        let lowerBound = self.index(self.startIndex, offsetBy: intRange.lowerBound)
        let upperBound = self.index(self.startIndex, offsetBy: intRange.lowerBound)
        
        return String(self[lowerBound..<upperBound])
    }
}

private extension DateFormatter {
    static let datedFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd HH-mm-ss"
        return formatter
    }()
}
