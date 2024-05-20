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
    static let global = GlobalTimer()
    
    func submit(time: TimeInterval, sampleRate: Int = 100) {
        precondition(sampleRate > 0)
        totalTime += time
        totalSamples += 1
        
        if totalSamples % sampleRate == 0 {
            print("        Time: \((100000*totalTime).rounded()/100)ms for \(sampleRate) samples")
            totalTime = 0
            totalSamples = 0
        }
    }
    
    static func run<T>(sampleRate: Int = 100, task: @Sendable () async throws -> T) async rethrows -> T {
        let time = ProcessInfo.processInfo.systemUptime
        do {
            let result = try await task()
            await Self.global.submit(time: ProcessInfo.processInfo.systemUptime - time, sampleRate: sampleRate)
            return result
        } catch {
            await Self.global.submit(time: ProcessInfo.processInfo.systemUptime - time, sampleRate: sampleRate)
            throw error
        }
    }
}
