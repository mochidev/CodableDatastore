//
//  DatedIdentifier.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-08.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

struct DatedIdentifier<T>: DatedIdentifierProtocol {
    var rawValue: String
    
    init(rawValue: String) {
        self.rawValue = rawValue
    }
}

protocol DatedIdentifierProtocol: TypedIdentifierProtocol {
    var rawValue: String { get }
    init(rawValue: String)
}

extension DatedIdentifierProtocol {
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
    var millisecond: String
    
    var token: String
    
    init(_ identifier: some DatedIdentifierProtocol) throws {
        let rawString = identifier.rawValue
        guard rawString.count == Self.size else {
            throw DatedIdentifierError.invalidLength
        }
        
        year = rawString[0..<4]
        month = rawString[5..<7]
        day = rawString[8..<10]
        hour = rawString[11..<13]
        minute = rawString[14..<16]
        second = rawString[17..<19]
        millisecond = rawString[20..<23]
        token = rawString[24..<40]
    }
    
    var monthDay: String {
        "\(month)-\(day)"
    }
    var hourMinute: String {
        "\(hour)-\(minute)"
    }
    
    static let size = 40
}


enum DatedIdentifierError: LocalizedError, Equatable {
    case invalidLength
    
    var errorDescription: String? {
        switch self {
        case .invalidLength: return "The identifier must be \(DatedIdentifierComponents.size) characters long."
        }
    }
}

private extension StringProtocol {
    subscript (intRange: Range<Int>) -> String {
        let lowerBound = self.index(self.startIndex, offsetBy: intRange.lowerBound)
        let upperBound = self.index(self.startIndex, offsetBy: intRange.upperBound)
        
        return String(self[lowerBound..<upperBound])
    }
}

private extension DateFormatter {
    static let datedFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd HH-mm-ss-SSS"
        return formatter
    }()
}
