//
//  Progress.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-15.
//  Copyright © 2023-24 Mochi Development, Inc. All rights reserved.
//

import Foundation

public typealias ProgressHandler = @Sendable (_ progress: Progress) async -> Void

public enum Progress: Sendable {
    case evaluating
    case working(current: Int, total: Int)
    case complete(total: Int)
    
    func adding(current: Int, total: Int) -> Self {
        switch self {
        case .evaluating: return .evaluating
        case .working(let oldCurrent, let oldTotal):
            return .working(current: oldCurrent + current, total: oldTotal + total)
        case .complete(let oldTotal):
            if current == total {
                return .complete(total: oldTotal + total)
            } else {
                return .working(current: oldTotal + current, total: oldTotal + total)
            }
        }
    }
    
    func adding(_ other: Progress) -> Self {
        switch other {
        case .evaluating: return .evaluating
        case .working(let newCurrent, let newTotal):
            return adding(current: newCurrent, total: newTotal)
        case .complete(let newTotal):
            return adding(current: newTotal, total: newTotal)
        }
    }
}
