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
    
    init(_ order: RangeOrder) {
        switch order {
        case .ascending: self = .ascending
        case .descending: self = .descending
        }
    }
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
        switch (order, self) {
        case (.ascending, .extent):                 .ascending
        case (.ascending, .excluding(let bound)):   bound < rhs ? .ascending : .descending
        case (.ascending, .including(let bound)):   bound <= rhs ? .ascending : .descending
        case (.descending, .extent):                .descending
        case (.descending, .excluding(let bound)):  rhs < bound ? .descending : .ascending
        case (.descending, .including(let bound)):  rhs <= bound ? .descending : .ascending
        }
    }
}
