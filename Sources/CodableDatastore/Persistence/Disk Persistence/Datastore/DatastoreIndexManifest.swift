//
//  DatastoreIndexManifest.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-26.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation
import AsyncSequenceReader
import Bytes

typealias DatastoreIndexManifestIdentifier = DatedIdentifier<DatastoreIndexManifest>

struct DatastoreIndexManifest: Equatable, Identifiable {
    /// The identifier for this manifest.
    var id: DatastoreIndexManifestIdentifier
    
    /// The version of the manifest, used when dealing with format changes at the library level.
    var version: Version = .alpha
    
    /// Pointers to the pages that make up the index.
    var orderedPages: [PageInfo]
    
    /// Pointers to the pageIDs currently in use by the index.
    var orderedPageIDs: some RandomAccessCollection<DatastorePageIdentifier?> {
        orderedPages.map { pageInfo -> DatastorePageIdentifier? in
            if case .removed = pageInfo { return nil }
            
            return pageInfo.id
        }
    }
    
    /// Pointers to the pageIDs removed by this iteration of the index.
    var removedPageIDs: some Sequence<DatastorePageIdentifier> {
        orderedPages.lazy.compactMap { pageInfo -> DatastorePageIdentifier? in
            guard case .removed(let id) = pageInfo else { return nil }
            return id
        }
    }
    
    /// Pointers to the pageIDs added by this iteration of the index.
    var addedPageIDs: some Sequence<DatastorePageIdentifier> {
        orderedPages.lazy.compactMap { pageInfo -> DatastorePageIdentifier? in
            guard case .added(let id) = pageInfo else { return nil }
            return id
        }
    }
}

// MARK: - Helper Types

extension DatastoreIndexManifest {
    enum Version {
        case alpha
    }
    
    enum PageInfo: Equatable, Identifiable {
        case existing(DatastorePageIdentifier)
        case removed(DatastorePageIdentifier)
        case added(DatastorePageIdentifier)
        
        var id: DatastorePageIdentifier {
            switch self {
            case .existing(let id),
                 .removed(let id),
                 .added(let id):
                return id
            }
        }
    }
}

// MARK: - Decoding

extension DatastoreIndexManifest {
    init(contentsOf url: URL, id: ID) async throws {
#if canImport(Darwin)
        if #available(macOS 12.0, iOS 15, watchOS 8, tvOS 15, *) {
            try await self.init(sequence: AnyReadableSequence(url.resourceBytes), id: id)
        } else {
            try await self.init(sequence: AnyReadableSequence(try Data(contentsOf: url)), id: id)
        }
#else
        try await self.init(sequence: AnyReadableSequence(try Data(contentsOf: url)), id: id)
#endif
    }
    
    init(sequence: AnyReadableSequence<UInt8>, id: ID) async throws {
        self.id = id
        
        var iterator = sequence.makeAsyncIterator()
        
        try await iterator.check(utf8: "INDEX\n")
        
        self.version = .alpha
        
        var pages: [PageInfo] = []
        
        while let pageStatus = try await iterator.nextIfPresent(utf8: String.self, count: 1) {
            /// Fail early if the page status indicator is not supported.
            switch pageStatus {
            case " ", "+", "-": break
            default:
                throw DiskPersistenceError.invalidIndexManifestFormat
            }
            
            guard let pageNameBytes = try await iterator.collect(upToIncluding: Character("\n").asciiValue!, throwsIfOver: 1024) else {
                throw DiskPersistenceError.invalidIndexManifestFormat
            }
            /// Drop the new-line and turn it into an ID.
            let pageID = DatastorePageIdentifier(rawValue: String(utf8Bytes: pageNameBytes.dropLast(1)))
            
            switch pageStatus {
            case " ":
                pages.append(.existing(pageID))
            case "+":
                pages.append(.added(pageID))
            case "-":
                pages.append(.removed(pageID))
            default:
                throw DiskPersistenceError.invalidIndexManifestFormat
            }
        }
        
        /// Make sure we are at the end of the file
        guard try await iterator.next() == nil
        else { throw DiskPersistenceError.invalidIndexManifestFormat }
        
        self.orderedPages = pages
    }
}

// MARK: - Encoding

extension DatastoreIndexManifest {
    var bytes: Bytes {
        var bytes = Bytes()
        /// 6 for the header, 1 for the page status, 36 for the ID, and 1 for the new line.
        bytes.reserveCapacity(6 + orderedPages.count*(1 + 36 + 1))
        
        bytes.append(contentsOf: "INDEX\n".utf8Bytes)
        
        for page in orderedPages {
            switch page {
            case .existing(let datastorePageIdentifier):
                bytes.append(contentsOf: " \(datastorePageIdentifier)\n".utf8Bytes)
            case .removed(let datastorePageIdentifier):
                bytes.append(contentsOf: "-\(datastorePageIdentifier)\n".utf8Bytes)
            case .added(let datastorePageIdentifier):
                bytes.append(contentsOf: "+\(datastorePageIdentifier)\n".utf8Bytes)
            }
        }
        
        return bytes
    }
}
