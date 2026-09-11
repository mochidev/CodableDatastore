//
//  SparseIterationChain.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-10-16.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Foundation

/// An internal type for tracking all downstream iterations that need to be purged.
struct SparseIterationChain {
    typealias IterationProxy = (iteration: SnapshotIteration.ID, creationDate: Date)
    
    struct Group {
        var first: IterationProxy
        var last: IterationProxy
        var contents: [IterationProxy]?
        var count: Int
        
        init(iteration: SnapshotIteration) {
            first = (iteration.id, iteration.creationDate)
            last = (iteration.id, iteration.creationDate)
            contents = [(iteration.id, iteration.creationDate)]
            count = 1
        }
        
        init(iteration: IterationProxy) {
            first = iteration
            last = iteration
            contents = [iteration]
            count = 1
        }
        
        mutating func prepend(iteration: IterationProxy) {
            if contents != nil {
                contents?.insert(iteration, at: 0)
            }
            first = iteration
            count += 1
        }
        
        mutating func append(iteration: IterationProxy) {
            if contents != nil {
                contents?.append(iteration)
            }
            last = iteration
            count += 1
        }
    }
    
    var count: Int
    var groups: [Group]
    
    init() {
        self.count = 0
        self.groups = []
    }
    
    /// Add a snapshot iteration to the start of the chain, scheduled to be removed last.
    mutating func prepend(iteration: SnapshotIteration) {
        prepend(iteration: (iteration.id, iteration.creationDate))
    }
    
    /// Add a snapshot iteration to the start of the chain, scheduled to be removed last.
    mutating func prepend(iteration: IterationProxy) {
        count += 1
        if !groups.isEmpty {
            /// If the group happens to have room, prepend to it and return.
            guard groups[0].count >= chunkSize else {
                groups[0].prepend(iteration: iteration)
                return
            }
            /// Otherwise, empty out the subsequent group, and flow through to create a new one.
            if groups.count >= 2 {
                groups[0].contents = nil
            }
        }
        groups.insert(Group(iteration: iteration), at: 0)
    }
    
    /// Add a snapshot iteration to the end of the chain, scheduled to be removed first.
    mutating func append(iteration: SnapshotIteration) {
        append(iteration: (iteration.id, iteration.creationDate))
    }
    
    /// Add a snapshot iteration to the end of the chain, scheduled to be removed first.
    mutating func append(iteration: IterationProxy) {
        count += 1
        if !groups.isEmpty {
            /// If the group happens to have room, append to it and return.
            guard groups[groups.count-1].count >= chunkSize else {
                groups[groups.count-1].append(iteration: iteration)
                return
            }
            /// Otherwise, empty out the previous group, and flow through to create a new one.
            if groups.count >= 2 {
                groups[groups.count-1].contents = nil
            }
        }
        groups.append(Group(iteration: iteration))
    }
    
    /// Remove and return groups from the end of the chain that should no longer be retained. If individual iterations should still be retained, they should be re-appended to the end.
    mutating func removeIterations(
        failing snapshotRetentionPolicy: SnapshotRetentionPolicy,
        from iterationID: SnapshotIteration.ID,
        now: Date
    ) -> (distance: Int, removedGroups: [Group]) {
        var totalDistance = 0
        var startingGroupIndex = 0
        
        /// First, scan for the first iteration to anchor on. If we can't find the anchor, we can't make a reliable conclusion based on distance for this pruning operation, so artifitially pad it by setting a negative distance to be safe.
        var foundAnchor = false
        groupIterator: for group in groups {
            if let contents = group.contents {
                for proxy in contents {
                    if proxy.iteration == iterationID {
                        foundAnchor = true
                        break groupIterator
                    }
                    totalDistance -= 1
                }
            } else {
                /// We ran out of populated groups, so just check the first entry of the next one in case it happens to be the iteration we want before giving up.
                if group.first.iteration == iterationID {
                    foundAnchor = true
                }
                break
            }
        }
        /// If we still haven't found the anchor, scan backwards counting the iterations that are definitely safe to prune based on distance.
        if !foundAnchor, totalDistance != -count {
            totalDistance = -count
            groupIterator: for group in groups.reversed() {
                if let contents = group.contents {
                    for proxy in contents.reversed() {
                        totalDistance += 1
                        if proxy.iteration == iterationID {
                            foundAnchor = true
                            break groupIterator
                        }
                    }
                } else {
                    /// We ran out of populated groups, so just check the last entry of the previous one in case it happens to be the iteration we want before giving up.
                    totalDistance += 1
                    if group.last.iteration == iterationID {
                        foundAnchor = true
                    }
                    break
                }
            }
        }
        
        /// Determine the first group that fails the check by crawling them in reverse.
        totalDistance += count
        startingGroupIndex = groups.count
        for group in groups.reversed() {
            totalDistance -= group.count
            count -= group.count
            startingGroupIndex -= 1
            if !snapshotRetentionPolicy.shouldIterationBePruned(now: now, creationDate: group.first.creationDate, distance: totalDistance) {
                break
            }
        }
        
        guard startingGroupIndex < groups.count
        else { return (distance: totalDistance, removedGroups: []) }
        
        /// Collect the groups that will be returned, and remove them.
        let groupsToRemove = Array(groups.suffix(groups.count - startingGroupIndex))
        groups.removeLast(groups.count - startingGroupIndex)
        return (distance: totalDistance, removedGroups: groupsToRemove)
    }
    
    var first: IterationProxy? {
        groups.first?.first
    }
    
    var last: IterationProxy? {
        groups.last?.last
    }
    
    /// The ideal size of a group before it is emptied and a new group is formed.
    var chunkSize: Int {
        /// Mapping count => group size => number of groups
        /// `<1 (2^0)` => `1 << max(-9, 8)` = `256` => `1` group
        /// `2 (2^1)` => `1 << max(-8, 8)` = `256` => `1` group
        /// `4 (2^2)` => `1 << max(-7, 8)` = `256` => `1` group
        /// `8 (2^3)` => `1 << max(-6, 8)` = `256` => `1` group
        /// `16 (2^4)` => `1 << max(-5, 8)` = `256` => `1` group
        /// `32 (2^5)` => `1 << max(-4, 8)` = `256` => `1` group
        /// `64 (2^6)` => `1 << max(-3, 8)` = `256` => `1` group
        /// `128 (2^7)` => `1 << max(-2, 8)` = `256` => `1` group
        /// `256 (2^8)` => `1 << max(-1, 8)` = `256` => `1` group
        /// `512 (2^9)` => `1 << max(0, 8)` = `256` => `2` groups
        /// `1,024 (2^10)` => `1 << max(1, 8)` = `256` => `4` groups
        /// `2,048 (2^11)` => `1 << max(2, 8)` = `256` => `8` groups
        /// `4,096 (2^12)` => `1 << max(3, 8)` = `256` => `16` groups
        /// `8,192 (2^13)` => `1 << max(4, 8)` = `256` => `32` groups
        /// `16,384 (2^14)` => `1 << max(5, 8)` = `256` => `64` groups
        /// `32,768 (2^15)` => `1 << max(6, 8)` = `256` => `128` groups
        /// `65,536 (2^16)` => `1 << max(7, 8)` = `256` => `256` groups
        /// `131,072 (2^17)` => `1 << max(8, 8)` = `256` => `512` groups
        /// `262,144 (2^18)` => `1 << max(9, 8)` = `512` => `512` groups
        /// `524,288 (2^19)` => `1 << max(10, 8)` = `1024` => `512` groups
        /// `...`
        1 << max(Int.bitWidth - 1 - (max(count - 1, 0)).leadingZeroBitCount - 8, 8)
    }
}

extension SparseIterationChain {
    enum State {
        case forwardEditsOnly
        case crawling(Task<Void, Never>)
        case complete
    }
}
