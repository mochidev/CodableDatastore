//
//  SortOrder.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-03.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

enum SortOrder {
    case ascending
    case equal
    case descending
}

extension Comparable {
    func sortOrder(comparedTo rhs: Self) -> SortOrder {
        if self < rhs { return .ascending }
        if self == rhs { return .equal }
        return .descending
    }
}
