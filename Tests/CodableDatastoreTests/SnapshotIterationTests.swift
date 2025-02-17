//
//  SnapshotIterationTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2025-02-16.
//  Copyright © 2023-25 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore

final class SnapshotIterationTests: XCTestCase, @unchecked Sendable {
    func testDecodingLegacyDatastoreRootReferences() throws {
        let data = Data("""
        {
          "addedDatastoreRoots" : [
            "2025-02-12 00-00-00-046 44BBE608B9CBF788"
          ],
          "addedDatastores" : [

          ],
          "creationDate" : "2025-02-12T00:00:00.057Z",
          "dataStores" : {
            "Store" : {
              "id" : "Store-FD9BA6F1BD3667C8",
              "key" : "Store",
              "root" : "2024-08-24 09-39-57-775 66004A6BA331B89C"
            }
          },
          "id" : "2025-02-12 00-00-00-057 0130730F8F6A1ACC",
          "precedingIteration" : "2025-02-11 23-59-54-727 447A1A1E1CF82177",
          "removedDatastoreRoots" : [
            "2025-02-11 23-59-54-721 2AAEA12A38303055"
          ],
          "removedDatastores" : [

          ],
          "successiveIterations" : [

          ],
          "version" : "alpha"
        }
        """.utf8)
        
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithMilliseconds
        _ = try decoder.decode(SnapshotIteration.self, from: data)
    }
    
    func testDecodingCurrentDatastoreRootReferences() throws {
        let data = Data("""
        {
          "addedDatastoreRoots" : [
            {
              "datastoreID" : "Store-FD9BA6F1BD3667C8",
              "datastoreRootID" : "2025-02-12 00-00-00-046 44BBE608B9CBF788"
            }
          ],
          "addedDatastores" : [

          ],
          "creationDate" : "2025-02-12T00:00:00.057Z",
          "dataStores" : {
            "Store" : {
              "id" : "Store-FD9BA6F1BD3667C8",
              "key" : "Store",
              "root" : "2024-08-24 09-39-57-775 66004A6BA331B89C"
            }
          },
          "id" : "2025-02-12 00-00-00-057 0130730F8F6A1ACC",
          "precedingIteration" : "2025-02-11 23-59-54-727 447A1A1E1CF82177",
          "removedDatastoreRoots" : [
            {
              "datastoreID" : "Store-FD9BA6F1BD3667C8",
              "datastoreRootID" : "2025-02-11 23-59-54-721 2AAEA12A38303055"
            }
          ],
          "removedDatastores" : [

          ],
          "successiveIterations" : [

          ],
          "version" : "alpha"
        }
        """.utf8)
        
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithMilliseconds
        _ = try decoder.decode(SnapshotIteration.self, from: data)
    }
    
    func testDecodingOptionalPrecedingSnapshotIdentifiers() throws {
        let data = Data("""
        {
          "addedDatastoreRoots" : [
            {
              "datastoreID" : "Store-FD9BA6F1BD3667C8",
              "datastoreRootID" : "2025-02-12 00-00-00-046 44BBE608B9CBF788"
            }
          ],
          "addedDatastores" : [

          ],
          "creationDate" : "2025-02-12T00:00:00.057Z",
          "dataStores" : {
            "Store" : {
              "id" : "Store-FD9BA6F1BD3667C8",
              "key" : "Store",
              "root" : "2024-08-24 09-39-57-775 66004A6BA331B89C"
            }
          },
          "id" : "2025-02-12 00-00-00-057 0130730F8F6A1ACC",
          "precedingIteration" : "2025-02-11 23-59-54-727 447A1A1E1CF82177",
          "precedingSnapshot" : "2024-04-14 13-09-27-739 A1EEB1A3AF102F15",
          "removedDatastoreRoots" : [
            {
              "datastoreID" : "Store-FD9BA6F1BD3667C8",
              "datastoreRootID" : "2025-02-11 23-59-54-721 2AAEA12A38303055"
            }
          ],
          "removedDatastores" : [

          ],
          "successiveIterations" : [

          ],
          "version" : "alpha"
        }
        """.utf8)
        
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601WithMilliseconds
        _ = try decoder.decode(SnapshotIteration.self, from: data)
    }
}
