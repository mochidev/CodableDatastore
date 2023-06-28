//
//  DatastoreEntry.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-27.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation
import AsyncSequenceReader
import Bytes

struct DatastoreEntry: Hashable {
    var headers: [Bytes]
    var content: Bytes
}


// MARK: - Encoding

extension DatastoreEntry {
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
    
    func blocks(remainingPageSpace: Int, maxPageSpace: Int) -> [DatastoreEntryBlock] {
        precondition(remainingPageSpace >= 0, "remainingPageSpace must be greater or equal to zero.")
        precondition(maxPageSpace > 4, "maxPageSpace must be greater or equal to 5.")
        
        var remainingSlice = bytes[...]
        var blocks: [DatastoreEntryBlock] = []
        
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
            
            let slice = remainingSlice[..<usableSpace]
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
            
            let slice = remainingSlice[..<usableSpace]
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
