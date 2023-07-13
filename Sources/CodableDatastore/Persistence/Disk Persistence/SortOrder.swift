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

extension RangeBoundExpression {
    func sortOrder(comparedTo rhs: Bound, order: RangeOrder) -> SortOrder {
        switch order {
        case .ascending:
            switch self {
            case .extent:
                return .ascending
            case .excluding(let bound):
                return bound < rhs ? .ascending : .descending
            case .including(let bound):
                return bound <= rhs ? .ascending : .descending
            }
        case .descending:
            switch self {
            case .extent:
                return .descending
            case .excluding(let bound):
                return bound <= rhs ? .ascending : .descending
            case .including(let bound):
                return bound < rhs ? .ascending : .descending
            }
        }
    }
}
