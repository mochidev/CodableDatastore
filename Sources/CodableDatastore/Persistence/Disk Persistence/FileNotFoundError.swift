//
//  FileNotFoundError.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2026-08-30.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Bytes
import Foundation

struct FileNotFoundError: Error {
    static func ~= (lhs: FileNotFoundError, rhs: any Error) -> Bool {
        (rhs as any FileError).isFileNotFound == true
        || ((rhs as? any ByteIterationError)?.iterationError as? any FileError)?.isFileNotFound == true
    }
}

protocol FileError {
    var isFileNotFound: Bool { get }
}

extension NSError: FileError {
    var isFileNotFound: Bool {
        URLError.fileDoesNotExist ~= self || CocoaError.fileReadNoSuchFile ~= self || CocoaError.fileNoSuchFile ~= self || POSIXError.ENOENT ~= self
    }
}

protocol ByteIterationError {
    var iterationError: (any Error)? { get }
}

extension BytesError.IterationError: ByteIterationError {
    var iterationError: (any Error)? {
        switch self {
        case .castingFailure: nil
        case .iterationFailure(let error): error
        }
    }
}
