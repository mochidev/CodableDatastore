//
//  GlobalTimer.swift
//  CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-05.
//  Copyright © 2023 Mochi Development, Inc. All rights reserved.
//

import Foundation

actor GlobalTimer {
    var totalTime: TimeInterval = 0
    var totalSamples: Int = 0
    static var global = GlobalTimer()
    
    func submit(time: TimeInterval) {
        totalTime += time
        totalSamples += 1
        
        if totalSamples % 10 == 0 {
            print("        Time: \((100000*totalTime).rounded()/100)ms for 10 samples")
            totalTime = 0
            totalSamples = 0
        }
    }
    
    static func run<T>(task: () async throws -> T) async rethrows -> T {
        let time = ProcessInfo.processInfo.systemUptime
        do {
            let result = try await task()
            await Self.global.submit(time: ProcessInfo.processInfo.systemUptime - time)
            return result
        } catch {
            await Self.global.submit(time: ProcessInfo.processInfo.systemUptime - time)
            throw error
        }
    }
}
