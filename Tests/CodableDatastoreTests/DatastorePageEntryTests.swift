//
//  DatastorePageEntryTests.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-04.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import XCTest
@testable import CodableDatastore
import Bytes

final class DatastorePageEntryTests: XCTestCase {
    func testDecoding() {
        XCTAssertEqual(
            try DatastorePageEntry(
                bytes: """
                
                
                """.utf8Bytes,
                isPartial: false
            ),
            DatastorePageEntry(headers: [], content: [])
        )
        
        XCTAssertEqual(
            try DatastorePageEntry(
                bytes: """
                
                A
                """.utf8Bytes,
                isPartial: false
            ),
            DatastorePageEntry(headers: [], content: "A".utf8Bytes)
        )
        
        XCTAssertEqual(
            try DatastorePageEntry(
                bytes: """
                1 \u{1}
                
                A
                """.utf8Bytes,
                isPartial: false
            ),
            DatastorePageEntry(headers: [[1]], content: "A".utf8Bytes)
        )
        
        XCTAssertEqual(
            try DatastorePageEntry(
                bytes: """
                1 \u{1}
                
                
                """.utf8Bytes,
                isPartial: false
            ),
            DatastorePageEntry(headers: [[1]], content: [])
        )
        
        XCTAssertEqual(
            try DatastorePageEntry(
                bytes: """
                1 \u{1}
                2 \u{2}\u{2}
                3 \u{3}\u{3}\u{3}
                
                A
                """.utf8Bytes,
                isPartial: false
            ),
            DatastorePageEntry(headers: [[1], [2, 2], [3, 3, 3]], content: "A".utf8Bytes)
        )
        
        XCTAssertEqual(
            try DatastorePageEntry(
                bytes: """
                1 \u{1}
                16 A complex
                string
                3 \u{3}\u{3}\u{3}
                
                Some complex
                content
                """.utf8Bytes,
                isPartial: false
            ),
            DatastorePageEntry(headers: [[1], "A complex\nstring".utf8Bytes, [3, 3, 3]], content: "Some complex\ncontent".utf8Bytes)
        )
        
        XCTAssertThrowsError(
            try DatastorePageEntry(
                bytes: """
                 
                """.utf8Bytes,
                isPartial: false
            )
        ) { error in
            XCTAssertEqual(error as? DiskPersistenceError, .invalidEntryFormat)
        }
        
        XCTAssertThrowsError(
            try DatastorePageEntry(
                bytes: """
                A
                """.utf8Bytes,
                isPartial: false
            )
        ) { error in
            XCTAssertEqual(error as? DiskPersistenceError, .invalidEntryFormat)
        }
        
        XCTAssertThrowsError(
            try DatastorePageEntry(
                bytes: """
                1
                """.utf8Bytes,
                isPartial: false
            )
        ) { error in
            switch error {
            case BytesError.invalidMemorySize(targetSize: 1, targetType: _, actualSize: 0): break
            default:
                XCTFail("Unknown error \(error)")
            }
        }
        
        XCTAssertThrowsError(
            try DatastorePageEntry(
                bytes: """
                1 1
                """.utf8Bytes,
                isPartial: false
            )
        ) { error in
            switch error {
            case BytesError.checkedSequenceNotFound: break
            default:
                XCTFail("Unknown error \(error)")
            }
        }
        
        XCTAssertThrowsError(
            try DatastorePageEntry(
                bytes: """
                1\u{20}
                
                
                """.utf8Bytes,
                isPartial: false
            )
        ) { error in
            switch error {
            case BytesError.invalidMemorySize(targetSize: 1, targetType: _, actualSize: 0): break
            default:
                XCTFail("Unknown error \(error)")
            }
        }
        
        XCTAssertThrowsError(
            try DatastorePageEntry(
                bytes: """
                0\u{20}
                
                
                """.utf8Bytes,
                isPartial: false
            )
        ) { error in
            XCTAssertEqual(error as? DiskPersistenceError, .invalidEntryFormat)
        }
        
        XCTAssertThrowsError(
            try DatastorePageEntry(
                bytes: """
                1 1
                
                """.utf8Bytes,
                isPartial: false
            )
        ) { error in
            switch error {
            case BytesError.invalidMemorySize(targetSize: 1, targetType: _, actualSize: 0): break
            default:
                XCTFail("Unknown error \(error)")
            }
        }
    }
    
    func testEncoding() {
        XCTAssertEqual(
            DatastorePageEntry(headers: [], content: []).bytes,
            """
            
            
            """.utf8Bytes
        )
        
        XCTAssertEqual(
            DatastorePageEntry(headers: [], content: "A".utf8Bytes).bytes,
            """
            
            A
            """.utf8Bytes
        )
        
        XCTAssertEqual(
            DatastorePageEntry(headers: [[1]], content: "A".utf8Bytes).bytes,
            """
            1 \u{1}
            
            A
            """.utf8Bytes
        )
        
        XCTAssertEqual(
            DatastorePageEntry(headers: [[1], [2, 2], [3, 3, 3]], content: "A".utf8Bytes).bytes,
            """
            1 \u{1}
            2 \u{2}\u{2}
            3 \u{3}\u{3}\u{3}
            
            A
            """.utf8Bytes
        )
        
        XCTAssertEqual(
            DatastorePageEntry(headers: [[1], "A complex\nstring".utf8Bytes, [3, 3, 3]], content: "Some complex\ncontent".utf8Bytes).bytes,
            """
            1 \u{1}
            16 A complex
            string
            3 \u{3}\u{3}\u{3}
            
            Some complex
            content
            """.utf8Bytes
        )
    }
    
    func testBlockDecomposition() {
        let smallEntry = DatastorePageEntry(headers: [[1]], content: [1])
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 1024, maxPageSpace: 1024),
            [
                .complete(
                    """
                    1 \u{1}
                    
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 4, maxPageSpace: 1024),
            [
                .complete(
                    """
                    1 \u{1}
                    
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 5, maxPageSpace: 1024),
            [
                .head(
                    """
                    1
                    """.utf8Bytes
                ),
                .tail(
                    """
                     \u{1}
                    
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 6, maxPageSpace: 1024),
            [
                .head(
                    """
                    1\u{20}
                    """.utf8Bytes
                ),
                .tail(
                    """
                    \u{1}
                    
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 7, maxPageSpace: 1024),
            [
                .head(
                    """
                    1 \u{1}
                    """.utf8Bytes
                ),
                .tail(
                    """
                    
                    
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 8, maxPageSpace: 1024),
            [
                .head(
                    """
                    1 \u{1}
                    
                    """.utf8Bytes
                ),
                .tail(
                    """
                    
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 7, maxPageSpace: 7),
            [
                .head(
                    """
                    1 \u{1}
                    """.utf8Bytes
                ),
                .tail(
                    """
                    
                    
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 6, maxPageSpace: 7),
            [
                .head(
                    """
                    1\u{20}
                    """.utf8Bytes
                ),
                .slice(
                    """
                    \u{1}
                    
                    
                    """.utf8Bytes
                ),
                .tail(
                    """
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 6, maxPageSpace: 6),
            [
                .head(
                    """
                    1\u{20}
                    """.utf8Bytes
                ),
                .slice(
                    """
                    \u{1}
                    
                    """.utf8Bytes
                ),
                .tail(
                    """
                    
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 5, maxPageSpace: 6),
            [
                .head(
                    """
                    1
                    """.utf8Bytes
                ),
                .slice(
                    """
                     \u{1}
                    """.utf8Bytes
                ),
                .slice(
                    """
                    
                    
                    
                    """.utf8Bytes
                ),
                .tail(
                    """
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
        
        XCTAssertEqual(
            smallEntry.blocks(remainingPageSpace: 5, maxPageSpace: 5),
            [
                .head(
                    """
                    1
                    """.utf8Bytes
                ),
                .slice(
                    """
                     
                    """.utf8Bytes
                ),
                .slice(
                    """
                    \u{1}
                    """.utf8Bytes
                ),
                .slice(
                    """
                    
                    
                    """.utf8Bytes
                ),
                .slice(
                    """
                    
                    
                    """.utf8Bytes
                ),
                .tail(
                    """
                    \u{1}
                    """.utf8Bytes
                )
            ]
        )
    }
}
