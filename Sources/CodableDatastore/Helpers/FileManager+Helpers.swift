//
//  FileManager+Helpers.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-09-08.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Foundation

enum DirectoryRemovalError: Error {
    case missingEnumerator
}

extension FileManager {
    @discardableResult
    func removeDirectoryIfEmpty(url: URL, recursivelyRemoveParents: Bool) throws -> Bool {
        guard let enumerator = self.enumerator(at: url, includingPropertiesForKeys: [], options: [.skipsHiddenFiles, .skipsSubdirectoryDescendants, .skipsPackageDescendants, .includesDirectoriesPostOrder])
        else { throw DirectoryRemovalError.missingEnumerator }
        
        for case _ as URL in enumerator {
            /// If this is called a single time, then we don't have an empty directory, and can stop
            return false
        }
        
        try self.removeItem(at: url)
        
        guard recursivelyRemoveParents else { return true }
        try self.removeDirectoryIfEmpty(url: url.deletingLastPathComponent(), recursivelyRemoveParents: recursivelyRemoveParents)
        return true
    }
}
