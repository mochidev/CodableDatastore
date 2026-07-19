// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

//
//  Package@swift-6.0.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2024-06-19.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//


import PackageDescription

let package = Package(
    name: "CodableDatastore",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .tvOS(.v13),
        .watchOS(.v6),
    ],
    products: [
        .library(
            name: "CodableDatastore",
            targets: ["CodableDatastore"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/mochidev/AsyncSequenceReader.git", .upToNextMinor(from: "0.3.1")),
        .package(url: "https://github.com/mochidev/Bytes.git", .upToNextMinor(from: "0.3.0")),
    ],
    targets: [
        .target(
            name: "CodableDatastore",
            dependencies: [
                "AsyncSequenceReader",
                "Bytes"
            ]
        ),
        .testTarget(
            name: "CodableDatastoreTests",
            dependencies: ["CodableDatastore"]
        ),
    ]
)
