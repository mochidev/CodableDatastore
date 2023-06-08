//
//  UUID+Comparable.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-04.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

/// Make UUIDs comparable, so that they can be used transparently as an index.
///
/// - SeeAlso: https://github.com/apple/swift-foundation/blob/5388acf1d929865d4df97d3c50e4d08bc4c6bdf0/Sources/FoundationEssentials/UUID.swift#L135-L156
extension UUID: Comparable {
    @inlinable
    public static func < (lhs: UUID, rhs: UUID) -> Bool {
        var leftUUID = lhs.uuid
        var rightUUID = rhs.uuid
        var result: Int = 0
        var diff: Int = 0
        withUnsafeBytes(of: &leftUUID) { leftPtr in
            withUnsafeBytes(of: &rightUUID) { rightPtr in
                for offset in (0 ..< MemoryLayout<uuid_t>.size).reversed() {
                    diff = Int(leftPtr.load(fromByteOffset: offset, as: UInt8.self)) -
                    Int(rightPtr.load(fromByteOffset: offset, as: UInt8.self))
                    // Constant time, no branching equivalent of
                    // if (diff != 0) {
                    //     result = diff;
                    // }
                    result = (result & (((diff - 1) & ~diff) >> 8)) | diff
                }
            }
        }
        
        return result < 0
    }
}
