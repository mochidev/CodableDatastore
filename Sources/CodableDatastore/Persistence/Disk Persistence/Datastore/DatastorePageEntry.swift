//
//  DatastorePageEntry.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-27.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation
import AsyncSequenceReader
import Bytes

struct DatastorePageEntry: Hashable {
    var headers: [Bytes]
    var content: Bytes
    var isPartial: Bool = false
}

// MARK: - Decoding

extension DatastorePageEntry {
    init(bytes: Bytes, isPartial: Bool) throws {
        var iterator = bytes.makeIterator()
        
        var headers: [Bytes] = []
        
        let space = " ".utf8Bytes[0]
        let newline = "\n".utf8Bytes[0]
        
        repeat {
            /// First, check for a new line. If we get one, the header section is done.
            let nextByte = try iterator.next(Bytes.self, count: 1)[0]
            guard nextByte != newline else { break }
            
            /// Accumulate the following bytes until we encounter a space
            var headerSizeBytes = [nextByte]
            while let nextByte = iterator.next(), nextByte != space {
                headerSizeBytes.append(nextByte)
            }
            
            /// Decode those bytes as a decimal number
            let decimalSizeString = String(utf8Bytes: headerSizeBytes)
            guard let headerSize = Int(decimalSizeString), headerSize > 0, headerSize <= 8*1024
            else { throw DiskPersistenceError.invalidEntryFormat }
            
            /// Save the header
            headers.append(try iterator.next(Bytes.self, count: headerSize))
            
            /// Make sure it ends in a new line
            try iterator.check(utf8: "\n")
        } while true
        
        /// Just collect the rest of the bytes as the content.
        self.content = iterator.next(Bytes.self, max: bytes.count)
        self.headers = headers
        self.isPartial = isPartial
    }
}

// MARK: - Encoding

extension DatastorePageEntry {
    var bytes: Bytes {
        var bytes = Bytes()
        
        var headerBytes = Bytes()
        for header in headers {
            headerBytes.append(contentsOf: String(header.count).utf8Bytes)
            headerBytes.append(contentsOf: " ".utf8Bytes)
            headerBytes.append(contentsOf: header)
            headerBytes.append(contentsOf: "\n".utf8Bytes)
        }
        
        bytes.reserveCapacity(headerBytes.count + 1 + content.count)
        
        bytes.append(contentsOf: headerBytes)
        bytes.append(contentsOf: "\n".utf8Bytes)
        bytes.append(contentsOf: content)
        
        return bytes
    }
    
    func blocks(remainingPageSpace: Int, maxPageSpace: Int) -> [DatastorePageEntryBlock] {
        precondition(remainingPageSpace >= 0, "remainingPageSpace must be greater or equal to zero.")
        precondition(maxPageSpace > 4, "maxPageSpace must be greater or equal to 5.")
        
        var remainingSlice = bytes[...]
        var blocks: [DatastorePageEntryBlock] = []
        
        if remainingPageSpace > 4 {
            var usableSpace = remainingPageSpace - 4
            var threshold = 10
            while usableSpace >= threshold {
                usableSpace -= 1
                threshold *= 10
            }
            
            guard remainingSlice.count > usableSpace else {
                return [.complete(Bytes(remainingSlice))]
            }
            
            let slice = remainingSlice.prefix(usableSpace)
            remainingSlice = remainingSlice.dropFirst(usableSpace)
            blocks.append(.head(Bytes(slice)))
        }
        
        while remainingSlice.count > 0 {
            var usableSpace = maxPageSpace - 4
            var threshold = 10
            while usableSpace >= threshold {
                usableSpace -= 1
                threshold *= 10
            }
            
            guard remainingSlice.count > usableSpace else {
                guard !blocks.isEmpty else {
                    return [.complete(Bytes(remainingSlice))]
                }
                
                blocks.append(.tail(Bytes(remainingSlice)))
                return blocks
            }
            
            let slice = remainingSlice.prefix(usableSpace)
            remainingSlice = remainingSlice.dropFirst(usableSpace)
            
            if blocks.isEmpty {
                blocks.append(.head(Bytes(slice)))
            } else {
                blocks.append(.slice(Bytes(slice)))
            }
        }
        
        return blocks
    }
}
