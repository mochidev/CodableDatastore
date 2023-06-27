// swift-tools-version: 5.7
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
        .package(url: "https://github.com/mochidev/AsyncSequenceReader.git", .upToNextMinor(from: "0.1.2")),
        .package(url: "https://github.com/mochidev/Bytes.git", .upToNextMinor(from: "0.2.3")),
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
