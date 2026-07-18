//
//  UUID+Comparable.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-04.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

#if !canImport(FoundationEssentials)
public import Foundation

/// Make UUIDs comparable on platforms that shipped without it, so that they can be used transparently as an index.
extension UUID: @retroactive Comparable {
    @inlinable
    @_disfavoredOverload
    public static func < (lhs: UUID, rhs: UUID) -> Bool {
        lhs.uuid < rhs.uuid
    }
}

/// Make UUIDs comparable, so that they can be used transparently as an index.
///
/// - SeeAlso: https://github.com/apple/swift-foundation/blob/5388acf1d929865d4df97d3c50e4d08bc4c6bdf0/Sources/FoundationEssentials/UUID.swift#L135-L156
@inlinable
public func < (lhs: uuid_t, rhs: uuid_t) -> Bool {
    var lhs = lhs
    var rhs = rhs
    var result: Int = 0
    var diff: Int = 0
    withUnsafeBytes(of: &lhs) { leftPtr in
        withUnsafeBytes(of: &rhs) { rightPtr in
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
#endif
