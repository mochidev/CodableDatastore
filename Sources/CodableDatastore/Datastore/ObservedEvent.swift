//
//  ObservedEvent.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-07-12.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Foundation

public enum ObservedEvent<IdentifierType, Entry> {
    case created(id: IdentifierType, newEntry: Entry)
    case updated(id: IdentifierType, oldEntry: Entry, newEntry: Entry)
    case deleted(id: IdentifierType, oldEntry: Entry)
    
    public var id: IdentifierType {
        switch self {
        case .created(let id, _), .updated(let id, _, _), .deleted(let id, _):
            return id
        }
    }
    
    func with<ID>(id: ID) -> ObservedEvent<ID, Entry> {
        switch self {
        case .created(_, let newEntry):
            return .created(id: id, newEntry: newEntry)
        case .updated(_, let oldEntry, let newEntry):
            return .updated(id: id, oldEntry: oldEntry, newEntry: newEntry)
        case .deleted(_, let oldEntry):
            return .deleted(id: id, oldEntry: oldEntry)
        }
    }
    
    func mapEntries<T>(transform: (_ entry: Entry) async throws -> T) async rethrows -> ObservedEvent<IdentifierType, T> {
        switch self {
        case .created(let id, let newEntry):
            return try await .created(id: id, newEntry: transform(newEntry))
        case .updated(let id, let oldEntry, let newEntry):
            return try await .updated(id: id, oldEntry: transform(oldEntry), newEntry: transform(newEntry))
        case .deleted(let id, let oldEntry):
            return try await .deleted(id: id, oldEntry: transform(oldEntry))
        }
    }
}

extension ObservedEvent: Identifiable where IdentifierType: Hashable {}
extension ObservedEvent: Sendable where IdentifierType: Sendable, Entry: Sendable {}

public struct ObservationEntry: Sendable {
    var versionData: Data
    var instanceData: Data
}

