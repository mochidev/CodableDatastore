# CodableDatastore

<p align="center">
    <a href="https://swiftpackageindex.com/mochidev/CodableDatastore">
        <img src="https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fmochidev%2FCodableDatastore%2Fbadge%3Ftype%3Dswift-versions" />
    </a>
    <a href="https://swiftpackageindex.com/mochidev/CodableDatastore">
        <img src="https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fmochidev%2FCodableDatastore%2Fbadge%3Ftype%3Dplatforms" />
    </a>
    <a href="https://github.com/mochidev/CodableDatastore/actions?query=workflow%3A%22Test+CodableDatastore%22">
        <img src="https://github.com/mochidev/CodableDatastore/workflows/Test%20CodableDatastore/badge.svg" alt="Test Status" />
    </a>
</p>

A pure-Swift implementation of a database-like persistent store for use in apps and other single-write-multiple-read environments.

## Quick Links

- [Documentation](https://swiftpackageindex.com/mochidev/CodableDatastore/documentation)
- [Updates on Mastodon](https://mastodon.social/tags/CodableDatastore)

## Installation

Add `CodableDatastore` as a dependency in your `Package.swift` file to start using it. Then, add `import CodableDatastore` to any file you wish to use the library in.

Please check the [releases](https://github.com/mochidev/CodableDatastore/releases) for recommended versions.

```swift
dependencies: [
    .package(
        url: "https://github.com/mochidev/CodableDatastore.git", 
        .upToNextMinor(from: "0.3.0")
    ),
],
...
targets: [
    .target(
        name: "MyPackage",
        dependencies: [
            "CodableDatastore",
        ]
    )
]
```

## Usage

There are three basic steps to use `CodableDatastore`:
- First, declare a _format_ for your datastore by conforming a struct to [`DatastoreFormat`](https://swiftpackageindex.com/mochidev/codabledatastore/main/documentation/codabledatastore/datastoreformat).
- Then, instanciate the [persistence](https://swiftpackageindex.com/mochidev/codabledatastore/main/documentation/codabledatastore/diskpersistence) on disk where it will be stored.
- Finally, [read](https://swiftpackageindex.com/mochidev/codabledatastore/main/documentation/codabledatastore/datastore/load%28_:%29-6uzkw) and [write](https://swiftpackageindex.com/mochidev/codabledatastore/main/documentation/codabledatastore/datastore/persist%28_:%29) to it as necessary.

### Declaring a Format

First, you must decide on the shape of your model. Each datastore can contain exactly one type, though a persistence can coordinate access across different types. Since `CodableDatastore` was designed with value types in mind, avoid using classes for your model objects.

You'll also need a corresponding _format_ for your datastore, that describes how your type is identified, versioning information, and any indexes you'd like to be automatically created on your behalf.

`CodableDatastore` encourages a pattern where the _format_ you declare becomes the primary namespace for your type and its related meta types:

```swift
import Foundation
import CodableDatastore

struct BookStore: DatastoreFormat {
    // These are both required, and used by the initializer for `Datastore` below.
    static let defaultKey: DatastoreKey = "BooksStore"
    static let currentVersion: Version = .current

    // A required description of your current and past versions you support
    // decoding. Note that the current version should always have a sensible
    // value, as it will get encoded to the persistence. 
    // Either `Int`s or `String`s are supported.
    enum Version: String {
        case v1 = "2024-04-01"
        case current = "2024-04-09"
    }

    // A required "pointer" to the latest representation of your type.
    typealias Instance = Book
    // Optional if the Instance above is Identifiable, but required otherwise.
    // typealias Identifier = UUID

    // The first version we shipped with and need to support.
    struct BookV1: Codable, Identifiable {
        var id: UUID
        var title: String
        var author: String
    }

    // The current version we use in the app.
    struct Book: Codable, Identifiable {
        var id: UUID
        var title: SortableTitle
        var authors: [AuthorID]
        var isbn: ISBN
    }

    // A non-required helper for instanciating a datastore. Note many
    // parameters are inferred, since `DatastoreFormat` declares a specialized
    // `Self.Datastore` with all the generic parameters filled out.
    // `CodableDatastore` comes with initializers for JSON and Property list
    // stores, though completely custom coders can easily be supported by using
    // the default initializers on `Datastore`.
    static func datastore(for persistence: DiskPersistence<ReadWrite>) -> Self.Datastore {
        .JSONStore(
            persistence: persistence,
            // This is where all the migrations for different versions are
            // defined; Simply decode the type you know if stored for a given 
            // version, and convert it to the modern type you use in the app.
            // Conversions are typically only done at read time.
            migrations: [
                .v1: { data, decoder in try Book(decoder.decode(BookV1.self, from: data)) },
                .current: { data, decoder in try decoder.decode(Book.self, from: data) }
            ]
        )
    }
    
    // Declare your indexes here by declaring stored properties in one of the
    // provided `Index` types. The keypaths refer to the main Instance for the
    // format you declared. Both stored and computed keypaths are supported.
    // Indexes are automatically re-computed whenever the names or types of
    // these declarations change, though the key path itself can change silently.
    // Note: Manual migrations of these is not currently supported but is planned.
    let title = Index(\.title)
    let author = ManyToManyIndex(\.authors)
    // A one to one index where every isbn points to exactly one book.
    // Note the index is marked with `@Direct`, which optimizes reads by isbn by
    // trading off the additional storage space needed to store full copies of
    // the `Book` struct in that index.
    @Direct var isbn = OneToOneIndex(\.isbn)
}

// A convenience alias for the rest of the app to use.
typealias Book = BookStore.Book

// Declare any necessary conversions to make old stored instances continue working.
extension Book {
    init(_ bookV1: BookStore.BookV1) {
        self.init(
            id: id,
            title: SortableTitle(title),
            authors: [AuthorID(authors)],
            isbn: ISBN.generate()
        )
    }
}
```

### Instanciating Your Persistence

Next, setup an actor or other manager to "own" your persistence and datastores in a way that makes sense for your app. Note that persistences and datastores don't need an async or throwable context to be instantiated, and provide deferred methods to do this when you can optionally show UI around these actions. Keep a reference to the persistence and datastore actors you create and either pass them around in your app individually, or keep them abstracted away in a single manager with getters and setters for common operations.

```swift
import Foundation
import CodableDatastore

actor LibraryPersistence {
    // Keep these around so we can access them as necessary
    let persistence: DiskPersistence<ReadWrite>
    
    let bookDatastore: BookStore.Datastore
    let authorDatastore: AuthorStore.Datastore
    let shelfDatastore: ShelfStore.Datastore

    init() async throws {
        // Initialize the persistence to Application Support, or pass in a readWriteURL
        persistence = try DiskPersistence.defaultStore()
        // Make sure we can write and access it
        try await persistence.createPersistenceIfNecessary()
        
        // Initialize the datastores so we can refer back to them
        bookDatastore = BookStore.datastore(for: persistence)
        authorDatastore = AuthorStore.datastore(for: persistence)
        shelfDatastore = ShelfStore.datastore(for: persistence)
        
        // Warm the datastores to re-build any indexes changed suring development.
        // This is an excellend opportunity to show migration UI if the process takes longer than a second.
        try await bookDatastore.warm { progress in
            switch progress {
            case .evaluating:
                // Always called
                print("Checking Books…") 
            case .working(let current, let total):
                // Only called if migrating. Signal some UI and log the values.
                // `current` is 0-based.
                print("  → Migrating \(current+1)/\(total) Books…")
            case .complete(let total):
                // Always called
                print("  ✓ Finished checking \(total) Books!") 
            }
        }
        try await authorDatastore.warm()
        try await shelfDatastore.warm()
    }
}
```

### Access the Datastores

Once you have a datastore, you can read from it in any async throwing context:

```swift
let bookByID = try await bookDatastore.load(bookID)
let bookByISBN = try await bookDatastore.load(isbn, from: \.isbn)
```

Note that in the above examples, bookID is of type `BookStore.Identifier`, aka `Book.ID`, and `\.isbn` is the keypath on `BookStore` that points to the ISBN index, a one-to-one index. These both return optionals, and thus `nil` if the instance for the given key 

A range of results can also be attained as an asynchronous sequence:

```swift
for try await book in bookDatastore.load("A"..<"B", from: \.title) {
    print("Book that starts with A: \(book.title)")
}

guard let dimitri = authorDatastore.load("Dimitri Bouniol", from: \.fullname).first(where: { _ in true })
else { throw NotFoundError() }
for try await book in bookDatastore.load(dimitri.id, from: \.author) {
    print("Book written by Dimitri: \(book.title)")
}

let allShelves = try await shelfDatastore.load(...).reduce(into: []) { $0.append($1) }
```

Writing or deleteing is equally as straight-forward:
```swift
let oldValue = try await bookDatastore.persist(newBook)
let oldValue = try await bookDatastore.delete(oldBookID)
let oldOptionalValue = try await bookDatastore.deleteIfPresent(oldBookID)

// Passing an Identifiable instance also works:
let oldValue = try await bookDatastore.delete(oldBook)
```

If you are reading and writing multiple things, you can wrap them in a transaction to ensure they all get written to the persistence together, ensuring that you either have all the data, or none of the data if an error occurs:
```swift
try await persistence.perform {
    try await authorDatastore.persist(newAuthor)
    for newBook in newBooks {
        guard let shelf = try await shelfDatastore.load(newBook.genre, from: \.genre)
        else { throw ShelfNotFoundError() }
        
        newBook.shelfID = shelf
        try await authorDatastore.persist(newBook)
    }
}
```

Note that in the example above, even though the author is persisted first, if an error occurs fetching the shelf for the book, the author will _not_ be present in the datastore in future reads. Additionally, no two writes can occur simultaneously no matter the async context, as all individual operations are themselves full transactions.

## What is `CodableDatastore`?

`CodableDatastore` is a collection of types that make it easy to interface with large data stores of independent types without loading the entire data store in memory.

> **Warning**
> THINK CAREFULLY ABOUT USING THIS IN PRODUCTION PROJECTS. As this project only just entered its beta phase, I cannot stress how important it is to be very careful about shipping anything that relies on this code, as you may experience data loss migrating to a newer version. Although less likely, there is a chance the underlying model may change in an incompatible way that is not worth supporting with migrations.
> Until then, please enjoy the code as a spectator or play around with it in toy projects to submit feedback! If you would like to be notified when `CodableDatastore` enters a production-ready state, please follow [#CodableDatastore](https://mastodon.social/tags/CodableDatastore) on Mastodon.

### Road to 1.0

As this project matures towards release, the project will focus on the functionality and work listed below:
- Force migration methods
- Composite indexes (via macros?)
- Cleaning up old resources on disk
- Ranged deletes
- Controls for the edit history
- Helper types to use with SwiftUI/Observability/Combine that can make data available on the main actor and filter and stay up to date
- Comprehensive test coverage
- Comprehensive usage guides
- An example app
- A memory persistence useful for testing apps with
- A pre-configured data store tuned to storing pure Data, useful for types like Images
- Cleaning up memory leaks

The above list will be kept up to date during development and will likely see additions during that process.

### Beyond 1.0

Once the 1.0 release has been made, it'll be time to start working on additional features:
- Snapshots and Backups
- A companion app to open, inspect, and modify datastores
- Other kinds of persistences, such as a distributed one for multi-server deployments
- Compression and encryption on a per-datastore basis
- External writes to a shared inbox

### Original Goals

<details open>
<summary><strong>Use Codable and Identifiable to as the only requirements for types saved to the data store.</strong></summary>

Having types conform to Codable and Identifiable as their only requirements means that many types won't need additional conformances or transformations to be used in other layers of the app, including at the view and network layers. Types must however conform to Identifiable so they can be updated when indexes require.

</details>

<details open>
<summary><strong>Allow the user to specify the Data-conforming Coder to use.</strong></summary>

Since `CodableDatastore` works with Codable types, it can flexibly support different types of coders. Out of the box, we plan on supporting both JSON and Property List coders as they provide an easy way for users to investigate the data saved to the store should they require doing so.

</details>

<details open>
<summary><strong>Guarantee consistency across writes, using the filesystem to snapshot and make operations atomic.</strong></summary>

All file operations will operate on copies of the files being modified, ultimately being persisted by updating the root file with a pointer to the updated set of files, and deleting the old file references once they are no longer referenced. This means that if the process is interrupted for any reason, data integrity is maintained and consistent.

Additionally, if any unreferenced filed are identified, they could be placed in a Recovered Files directory allowing the developer of an app to help their users recover data should catastrophe arise.

</details>

<details open>
<summary><strong>Enable other processes to concurrently read from the data store.</strong></summary>

A common pattern is for App Extensions to need to read data from the main app, but not write to it. In this case, the data store can safely be opened as read only at the time of initialization, allowing the contents of that data store to be read by the app extension.

For cases where the Extension needs to write data for the app, it is suggested a separate persistence be used to communicate that flow of data, as persistences do not support multiple writing processes.

</details>

<details open>
<summary><strong>Offer an API than can make performance promises.</strong></summary>

As the `CodableDatastore` is configured directly with indexes that the user specified, `CodableDatastore` can make performance guarantees without any hidden gotchas, as data can only be accessed via one of those indexes, and data cannot be loaded by a non-indexed key.

</details>

<details open>
<summary><strong>Build on existing paradigms of the Swift language, using Swift concurrency to make operations async, and offer loading large amounts of data via Async Sequences.</strong></summary>

`CodableDatastore` makes liberal use of Swift's concurrency APIs, with all reads and writes being async operations that can fail in a way the user can do something about, and offers streams to data being loaded via AsyncSequences, allowing data to be loaded efficiently at the rate the consumer expects it.

</details>

<details open>
<summary><strong>Allow re-indexing at any time, even for an existing data store.</strong></summary>

Apps change how they access data during development, and indexes evolve as a result of that. Since indexes are configured in code, they can change between builds or releases, so `CodableDatastore` supports re-indexing data should it determine that indexes have been re-configured. A method is provided to allow the app to await the re-indexing process with progress so a user interface can be shown to the user while this is happening.

</details>

<details open>
<summary><strong>Allow type-safe migrations for evolving datasets.</strong></summary>

As apps evolve, the type of data they store evolves along with it. `CodableDatastore` provides no hassle migrations between older types and newer ones with typed versions to help you make sure you are covering all your bases. All you need to do is make sure to version older types and provide a translation between them and the type you expect.

This migration can even be done on save if desired, meaning the user doesn't need to wait to perform a migration so long as the types are supported and indexes don't need to be re-calculated.

Additionally, we aim to make sure that testing migrations against data snapshots is just as easy, allowing users to evolve their types with confidence.

</details>

<details open>
<summary><strong>Allow transactional reads and writes.</strong></summary>

Supporting atomic transactions is important when consistency between multiple data models is key to an app functioning correctly. A transaction being in progress means the objects updated by that transaction are locked for the duration of that transaction (other transactions will wait for this one to complete), and that all data is written to disk in a single final atomic write before returning that the transaction was complete. Importantly, this is done across data stores that share a common configuration, allowing the user to save independent types together. This also means that if a transaction fails, any updates it made will be reverted in the process.

</details>

<details open>
<summary><strong>Have all configuration be described in code.</strong></summary>

Instead of spreading the configuration across multiple different types or files, `CodableDatastore` aims to allow users of the library to have all configuration be defined in code, ideally in one place in an app.

</details>

<details open>
<summary><strong>Enable easy testing with out-of-the-box mocks.</strong></summary>

A configuration can describe either an on-disk persistence or an in-memory persistence, allowing app-based tests to be written against the in-memory version with little reconfiguration necessary. Additionally, since all access to the data store is made through a common actor, stubbing a new data store with compatible types should be easily attainable.

</details>

### Future Goals

<details open>
<summary><strong>Allow indexing to be described using variadic generics.</strong></summary>

Swift 5.9 will introduce variadic generics allowing multiple indexes with different key paths to be described on the same data store. For now, we'll hard-code them as needed.

</details>

<details open>
<summary><strong>Snapshotting and backups.</strong></summary>

Although not planned for 1.0, this system should support light-weight snapshotting fairly easily by duplicating the file structure, making use of APFS snapshots to make sure data is not actually duplicated. Support for doing this via the API will be coming soon.

</details>

<details open>
<summary><strong>Data integrity.</strong></summary>

Although `CodableDatastore` aims to maintain consistency for what is saved to the filesystem, it does nothing to maintain that the filesystem has not corrupted the data in the interim. This can be solved using additional Error-Correcting Codes saved along-side every file to correct bit errors should they ever occur.

</details>

<details open>
<summary><strong>Encryption.</strong></summary>

Encrypting the data store on disk could be supported in the future.

</details>

### Non-goals

<details open>
<summary><strong>Describe inter-dependencies between types.</strong></summary>

This usually dramatically increases the complexity of index structures and allows users of the API to not understand the performance implications of creating inter-dependent relationships between disparate types.

Instead, `CodableDatastore` aims to provide robust transactions between different data stores with the same configuration, allowing the user to build their own relationships by updating two or more data stores instead of these relationships being automatically built.

</details>

<details open>
<summary><strong>Safely allow multiple writing processes.</strong></summary>

Although multiple readers are supported, `CodableDatastore` intends a single process to write to disk persistence at once. This means that behavior is undefined if multiple writes happen from different processes. Ordinarily, this would be a problem in server-based deployments since server applications are traditionally run on multiple processes on a single machine, but most Swift-based server apps use a single process and multiple threads to achieve better performance, and would thus be compatible.

If you are designing a scalable system that runs multiple processes, consider setting up a single instance with the data store, or multiple instances with their own independent data stores to maintain these promises. Although not impossible, sharding and other strategies to keep multiple independent data stores in sync are left as an exercise to the user of this library.

</details>

## Contributing

Contribution is welcome! Please take a look at the issues already available, or start a new discussion to propose a new feature. Although guarantees can't be made regarding feature requests, PRs that fit within the goals of the project and that have been discussed beforehand are more than welcome!

Please make sure that all submissions have clean commit histories, are well documented, and thoroughly tested. **Please rebase your PR** before submission rather than merge in `main`. Linear histories are required, so merge commits in PRs will not be accepted.

## Support

To support this project, consider following [@dimitribouniol](https://mastodon.social/@dimitribouniol) on Mastodon, listening to Spencer and Dimitri on [Code Completion](https://mastodon.social/@codecompletion), or downloading Dimitri's wife Linh's app, [Not Phở](https://notpho.app/).
