// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

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
        .package(url: "https://github.com/mochidev/AsyncSequenceReader.git", .upToNextMinor(from: "0.3.0")),
        .package(url: "https://github.com/mochidev/Bytes.git", .upToNextMinor(from: "0.3.0")),
    ],
    targets: [
        .target(
            name: "CodableDatastore",
            dependencies: [
                "AsyncSequenceReader",
                "Bytes"
            ],
            swiftSettings: [
                .swiftLanguageVersion(.v6),
            ]
        ),
        .testTarget(
            name: "CodableDatastoreTests",
            dependencies: ["CodableDatastore"],
            swiftSettings: [
                .swiftLanguageVersion(.v6),
            ]
        ),
    ]
)
