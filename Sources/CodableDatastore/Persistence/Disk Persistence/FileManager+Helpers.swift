//
//  FileManager+Helpers.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-09-08.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

enum DirectoryRemovalError: Error {
    case missingEnumerator
}

extension FileManager {
    func removeDirectoryIfEmpty(url: URL, recursivelyRemoveParents: Bool) throws {
        guard let enumerator = self.enumerator(at: url, includingPropertiesForKeys: [], options: [.skipsHiddenFiles, .skipsSubdirectoryDescendants, .skipsPackageDescendants, .includesDirectoriesPostOrder])
        else { throw DirectoryRemovalError.missingEnumerator }
        
        for case _ as URL in enumerator {
            /// If this is called a single time, then we don't have an empty directory, and can stop
            return
        }
        
        try self.removeItem(at: url)
        
        guard recursivelyRemoveParents else { return }
        try self.removeDirectoryIfEmpty(url: url.deletingLastPathComponent(), recursivelyRemoveParents: recursivelyRemoveParents)
    }
}
