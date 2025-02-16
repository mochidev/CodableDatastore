import Foundation

/// A retention policy describing which snapshot iterations should be kept around on disk.
///
/// Every write is made as a part of a top-level transaction that gets recorded atomically to disk as a snapshot iteration. These iterations can domtain edits to one or more datastores, and represent a complete view of all data at any one moment in time. Keeping iterations around allows you to rewind the datastores in a consistent and non-breaking way, though they take up disk space for all pages that are no longer current, ie. those containing deletions or older versions of records persisted to disk.
///
/// A retention policy allows the disk persistence to automatically clean up these older iterations according to the policy you need for your app. The retention policy is only enforced when a write transaction completes, though the persistence may defer cleanup until later if write volumes are high.
public struct SnapshotRetentionPolicy: Sendable {
    /// Internal predicate that tests if an iteration should be pruned.
    /// 
    /// - Parameter iteration: The iteration to check.
    /// - Parameter distance: How far the iteration is from the current root. The current root is `0` away from itself, while the next oldest iteration has a distance of `1`.
    /// - Returns: `true` if the iteration, all its ancestors, and all it's other decedents should be pruned, `false` if the next iteration should be checked.
    typealias PrunePredicate = @Sendable (_ iteration: SnapshotIteration, _ distance: Int) -> Bool
    
    /// Internal marker indicating if the retention policy refers to the ``none`` policy.
    let isNone: Bool
    
    /// Internal marker indicating if the retention policy refers to the ``indefinite`` policy.
    let isIndefinite: Bool
    
    /// Internal predicate that tests if an iteration should be pruned.
    ///
    /// - Parameter iteration: The iteration to check.
    /// - Parameter distance: How far the iteration is from the current root. The current root is `0` away from itself, while the next oldest iteration has a distance of `1`.
    /// - Returns: `true` if the iteration, all its ancestors, and all it's other decedents should be pruned, `false` if the next iteration should be checked.
    let shouldPrune: PrunePredicate
    
    /// Internal initializer for creating a retention policy from flags and a predicate.
    /// - Parameters:
    ///   - isNone: Wether this represents a ``none`` policy.
    ///   - isIndefinite: Wether this represents an ``indefinite`` policy.
    ///   - shouldPrune: The predicate to use when testing retention.
    init(
        isNone: Bool = false,
        isIndefinite: Bool = false,
        shouldPrune: @escaping PrunePredicate
    ) {
        self.isNone = isNone
        self.isIndefinite = isIndefinite
        self.shouldPrune = shouldPrune
    }
    
    /// A retention policy that only the most recent iteration should be kept around on disk, and all other iterations should be discarded.
    /// 
    /// - Note: It will not be possible to rewind the datastore to a previous state using this policy, and other processes won't be able to read from a read-only datastore while the main one is writing to it.
    public static let none = SnapshotRetentionPolicy(isNone: true) { _, _ in true }
    
    /// A retention policy that includes all iterations.
    ///
    /// - Note: This policy may incur a large amount of disc usage, especially on datastores with many writes.
    public static let indefinite = SnapshotRetentionPolicy(isIndefinite: true) { _, _ in false }
    
    /// A retention policy that retains the specified number of transactions, including the most recent transaction.
    ///
    /// To retain only the most recent transaction, specify a count of `0`. To retain the last 10 transactions, in addition to the current one (leaving up to 11 on disk at once), specify a count of `10`. Specifying a negative number will assert at runtime if assertions are enabled.
    ///
    /// This is a useful way to ensure a minimum number of transactions will always be accessible on disk at once for other processes to read, though the exact number an app will need will depend on how often write transactions occur, and how much disk space each write transaction occupies.
    ///
    /// - Parameter count: The number of additional transactions to retain.
    /// - Returns: A policy retaining at most `count` additional transactions.
    public static func transactionCount(_ count: Int) -> Self {
        assert(count >= 0, "Transaction count must be larger or equal to 0")
        return SnapshotRetentionPolicy { _, distance in distance > count}
    }
    
    /// A retention policy that retains transactions younger than a specified duration.
    /// 
    /// A retention cutoff is calculated right at the moment the last write transaction takes place, subtracting the specified `timeInterval` from this moment in time. Note that this policy is sensitive to time changes on the host, as previous transactions record their creation date in a runtime agnostic way that relies on an absolute date and time.
    /// 
    /// - Note: This policy may be more stable than ``transactionCount(_:)``, but may incur a non-constant amount of additional disk space depending on write volume.
    /// - Parameter timeInterval: The time interval in seconds to indicate an acceptable retention window.
    /// - Returns: A policy retaining transactions as old as the specified `timeInterval`.
    public static func duration(_ timeInterval: TimeInterval) -> Self {
        SnapshotRetentionPolicy { iteration, _ in iteration.creationDate < Date(timeIntervalSinceNow: -timeInterval)}
    }
    
    /// A retention policy that retains transactions younger than a specified duration.
    ///
    /// A retention cutoff is calculated right at the moment the last write transaction takes place, subtracting the specified `duration` from this moment in time. Note that this policy is sensitive to time changes on the host, as previous transactions record their creation date in a runtime agnostic way that relies on an absolute date and time.
    ///
    /// - Note: This policy may be more stable than ``transactionCount(_:)``, but may incur a non-constant amount of additional disk space depending on write volume.
    /// - Parameter duration: The duration to indicate an acceptable retention window.
    /// - Returns: A policy retaining transactions as old as the specified `duration`.
    @_disfavoredOverload
    @available(macOS 13.0, *)
    public static func duration(_ duration: Duration) -> Self {
        .duration(TimeInterval(duration.components.seconds))
    }
    
    /// A retention policy that retains transactions younger than a specified duration.
    ///
    /// A retention cutoff is calculated right at the moment the last write transaction takes place, subtracting the specified `duration` from this moment in time. Note that this policy is sensitive to time changes on the host, as previous transactions record their creation date in a runtime agnostic way that relies on an absolute date and time.
    ///
    /// - Note: This policy may be more stable than ``transactionCount(_:)``, but may incur a non-constant amount of additional disk space depending on write volume.
    /// - Parameter duration: The duration in seconds to indicate an acceptable retention window.
    /// - Returns: A policy retaining transactions as old as the specified `duration`.
    public static func duration(_ duration: RetentionDuration) -> Self {
        .duration(TimeInterval(duration.timeInterval))
    }
    
    /// A retention policy ensuring both specified policies are enforced before pruning a snapshot.
    /// 
    /// This policy is useful to indicate that at least the specified number of transactions should be kept around, for at least a specified amount of time:
    ///
    ///     persistence.retentionPolicy = .both(.transactionCount(10), and: .duration(.days(2)))
    ///
    /// As a result, this policy errs on the side of keeping transactions around when compared with ``either(_:or:)``.
    ///
    /// - Parameters:
    ///   - lhs: A policy to evaluate.
    ///   - rhs: Another policy to evaluate.
    /// - Returns: A policy that ensures both `lhs` and `rhs` allow a transaction to be pruned before actually pruning it.
    public static func both(_ lhs: SnapshotRetentionPolicy, and rhs: SnapshotRetentionPolicy) -> Self {
        guard !lhs.isIndefinite, !rhs.isIndefinite else { return .indefinite }
        if lhs.isNone { return rhs }
        if rhs.isNone { return lhs }
        return SnapshotRetentionPolicy { lhs.shouldIterationBePruned(iteration: $0, distance: $1) && rhs.shouldIterationBePruned(iteration: $0, distance: $1)}
    }
    
    /// A retention policy ensuring either specified policies are enforced before pruning a snapshot.
    ///
    /// This policy is useful to indicate that at most the specified number of transactions should be kept around, for at most a specified amount of time:
    ///
    ///     persistence.retentionPolicy = .either(.transactionCount(10), or: .duration(.days(2)))
    ///
    /// As a result, this policy errs on the side of removing transactions when compared with ``both(_:and:)``.
    ///
    /// - Parameters:
    ///   - lhs: A policy to evaluate.
    ///   - rhs: Another policy to evaluate.
    /// - Returns: A policy that ensures either `lhs` or `rhs` allow a transaction to be pruned before actually pruning it.
    public static func either(_ lhs: SnapshotRetentionPolicy, or rhs: SnapshotRetentionPolicy) -> Self {
        guard !lhs.isNone, !rhs.isNone else { return .none }
        if lhs.isIndefinite { return rhs }
        if rhs.isIndefinite { return lhs }
        return SnapshotRetentionPolicy { lhs.shouldIterationBePruned(iteration: $0, distance: $1) || rhs.shouldIterationBePruned(iteration: $0, distance: $1)}
    }
    
    /// Internal method to check if an iteration should be pruned and removed from disk.
    ///
    /// - Parameter iteration: The iteration to check.
    /// - Parameter distance: How far the iteration is from the current root. The current root is `0` away from itself, while the next oldest iteration has a distance of `1`.
    /// - Returns: `true` if the iteration, all its ancestors, and all it's other decedents should be pruned, `false` if the next iteration should be checked.
    func shouldIterationBePruned(iteration: SnapshotIteration, distance: Int) -> Bool {
        shouldPrune(iteration, distance)
    }
}

/// The duration in time snapshot iterations should be retained for.
public struct RetentionDuration: Hashable, Sendable {
    /// Internal representation of a retention duration.
    @usableFromInline
    var timeInterval: TimeInterval
    
    /// Internal initializer for creating a retention duration from a time interval.
    @usableFromInline
    init(timeInterval: TimeInterval) {
        self.timeInterval = timeInterval
    }
    
    /// A retention duration in seconds.
    @inlinable
    public static func seconds<Int: BinaryInteger>(_ seconds: Int) -> Self {
        RetentionDuration(timeInterval: TimeInterval(seconds))
    }
    
    /// A retention duration in seconds.
    @inlinable
    public static func seconds<Float: BinaryFloatingPoint>(_ seconds: Float) -> Self {
        RetentionDuration(timeInterval: TimeInterval(seconds))
    }
    
    /// A retention duration in minutes.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with minutes when leap seconds are applied.
    @inlinable
    public static func minutes<Int: BinaryInteger>(_ minutes: Int) -> Self {
        RetentionDuration(timeInterval: TimeInterval(minutes)*60)
    }
    
    /// A retention duration in minutes.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with minutes when leap seconds are applied.
    @inlinable
    public static func minutes<Float: BinaryFloatingPoint>(_ minutes: Float) -> Self {
        RetentionDuration(timeInterval: TimeInterval(minutes)*60)
    }
    
    /// A retention duration in hours.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with hours on a calendar across events like seasonal time changes dependent on timezone.
    @inlinable
    public static func hours<Int: BinaryInteger>(_ hours: Int) -> Self {
        RetentionDuration(timeInterval: TimeInterval(hours)*60*60)
    }
    
    /// A retention duration in hours.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with hours on a calendar across events like seasonal time changes dependent on timezone.
    @inlinable
    public static func hours<Float: BinaryFloatingPoint>(_ hours: Float) -> Self {
        RetentionDuration(timeInterval: TimeInterval(hours)*60*60)
    }
    
    /// A retention duration in 24 hour days.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with days on a calendar across events like seasonal time changes dependent on timezone.
    @inlinable
    public static func days<Int: BinaryInteger>(_ days: Int) -> Self {
        RetentionDuration(timeInterval: TimeInterval(days)*60*60*24)
    }
    
    /// A retention duration in 24 hour days.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with days on a calendar across events like seasonal time changes dependent on timezone.
    @inlinable
    public static func days<Float: BinaryFloatingPoint>(_ days: Float) -> Self {
        RetentionDuration(timeInterval: TimeInterval(days)*60*60*24)
    }
    
    /// A retention duration in weeks, defined as seven 24 hour days.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with days on a calendar across events like seasonal time changes dependent on timezone.
    @inlinable
    public static func weeks<Int: BinaryInteger>(_ weeks: Int) -> Self {
        RetentionDuration(timeInterval: TimeInterval(weeks)*60*60*24*7)
    }
    
    /// A retention duration in weeks, defined as seven 24 hour days.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with days on a calendar across events like seasonal time changes dependent on timezone.
    @inlinable
    public static func weeks<Float: BinaryFloatingPoint>(_ weeks: Float) -> Self {
        RetentionDuration(timeInterval: TimeInterval(weeks)*60*60*24*7)
    }
    
    /// A retention duration in months, defined as thirty 24 hour days.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with days or even months on a calendar across events like seasonal time changes dependent on timezone, different length months, or leap days.
    @inlinable
    public static func months<Int: BinaryInteger>(_ months: Int) -> Self {
        RetentionDuration(timeInterval: TimeInterval(months)*60*60*24*30)
    }
    
    /// A retention duration in months, defined as thirty 24 hour days.
    ///
    /// - Warning: This duration does not take into account timezones or calendar dates, and strictly represents a duration of time. It therefore makes no guarantees to line up with days or even months on a calendar across events like seasonal time changes dependent on timezone, different length months, or leap days.
    @inlinable
    public static func months<Float: BinaryFloatingPoint>(_ months: Float) -> Self {
        RetentionDuration(timeInterval: TimeInterval(months)*60*60*24*30)
    }
}

extension RetentionDuration: Comparable {
    @inlinable
    public static func < (lhs: Self, rhs: Self) -> Bool {
        lhs.timeInterval < rhs.timeInterval
    }
}

extension RetentionDuration: AdditiveArithmetic {
    public static let zero = RetentionDuration(timeInterval: 0)
    
    @inlinable
    public prefix static func + (rhs: Self) -> Self {
        rhs
    }
    
    @inlinable
    public prefix static func - (rhs: Self) -> Self {
        RetentionDuration(timeInterval: -rhs.timeInterval)
    }
    
    @inlinable
    public static func + (lhs: Self, rhs: Self) -> Self {
        RetentionDuration(timeInterval: lhs.timeInterval + rhs.timeInterval)
    }
    
    @inlinable
    public static func += (lhs: inout Self, rhs: Self) {
        lhs.timeInterval += rhs.timeInterval
    }
    
    @inlinable
    public static func - (lhs: Self, rhs: Self) -> Self {
        RetentionDuration(timeInterval: lhs.timeInterval - rhs.timeInterval)
    }
    
    @inlinable
    public static func -= (lhs: inout Self, rhs: Self) {
        lhs.timeInterval -= rhs.timeInterval
    }
}
