//
//  URLCollector.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2026-09-06.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Foundation
import QuestionableConcurrency

class URLCollector: @unchecked Sendable {
    private var gate = UnfairLock()
    private var urls: Set<URL> = []
    
    init() {}
    
    func insertURL(_ url: URL) {
        gate.withLock {
            _ = urls.insert(url)
        }
    }
    
    /// Return all URLs, sorted by longest.
    func removeAllURLs() -> [URL] {
        let urls = gate.withLock { self.urls }
        return urls.sorted { $0.absoluteString > $1.absoluteString }
    }
}
