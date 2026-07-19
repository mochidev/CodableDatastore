// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

//
//  Package.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-05-10.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//


import PackageDescription

let swiftSettings: [PackageDescription.SwiftSetting] = [
    .enableUpcomingFeature("ExistentialAny"),
    .enableUpcomingFeature("InternalImportsByDefault"),
    .enableUpcomingFeature("MemberImportVisibility"),
    .enableUpcomingFeature("InferIsolatedConformances"),
    .enableUpcomingFeature("ImmutableWeakCaptures"),
]

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
        .package(url: "https://github.com/mochidev/AsyncSequenceReader.git", .upToNextMinor(from: "0.5.0")),
        .package(url: "https://github.com/mochidev/Bytes.git", .upToNextMinor(from: "0.6.2")),
    ],
    targets: [
        .target(
            name: "CodableDatastore",
            dependencies: [
                "AsyncSequenceReader",
                "Bytes"
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "CodableDatastoreTests",
            dependencies: ["CodableDatastore"],
            swiftSettings: swiftSettings
        ),
    ]
)
