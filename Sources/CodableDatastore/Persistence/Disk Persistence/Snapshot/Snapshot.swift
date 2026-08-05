//
//  Snapshot.swift
//  https://github.com/mochidev/CodableDatastore
//
//  Created by Dimitri Bouniol on 2023-06-09.
//  Copyright © 2023-26 Mochi Development, Inc. All rights reserved.
//  mochidev-codable-datastore: 8A3D87799CB24B2BA7A7661369B88325
//

import Foundation
import QuestionableConcurrency

typealias SnapshotIdentifier = DatedIdentifier<Snapshot<ReadOnly>>

@DebugDescription extension SnapshotIdentifier {
    var debugDescription: String { "SnapshotIdentifier(\(rawValue))" }
}

/// A type that manages access to a snapshot on disk.
actor Snapshot<AccessMode: _AccessMode> {
    /// The identifier of the snapshot.
    ///
    /// This is used to determine where on disk the snapshot is stored.
    let id: SnapshotIdentifier
    
    /// The persistence the stapshot is a part of.
    ///
    /// Prefer to access ``Snapshot/persistence`` instead, which offers non-optional access to the same persistence.
    unowned let persistence: DiskPersistence<AccessMode>
    
    /// A flag indicating if this is a backup snapshot.
    ///
    /// This is used to determine which parent directory on disk the snapshot is stored.
    let isBackup: Bool
    
    /// A cached instance of the manifest as last loaded from disk.
    var cachedManifest: SnapshotManifest?
    
    /// Cache for the loaded iterations as last loaded from disk. ``isExtendedIterationCacheEnabled`` controls if multiple iterations are cached or not.
    var cachedIterations: [SnapshotIterationIdentifier : SnapshotIteration] = [:]
    var isExtendedIterationCacheEnabled: Bool
    private var nextSnapshotIterationCandidateToEnforce: (iterationID: SnapshotIteration.ID, retentionPolicy: SnapshotRetentionPolicy, taskPriority: TaskPriority)?
    private var snapshotIterationPruningTask: Task<Void, Never>?
    
    /// A transaction stream for manifest updates, so reads and writes can be serialized in request order.
    var manifestTransactionStream = TransactionStream()
    
    /// The loaded datastores.
    var datastores: [DatastoreIdentifier: DiskPersistence<AccessMode>.Datastore] = [:]
    
    /// The chain of iterations
    var iterationChain: SparseIterationChain
    var iterationChainState: SparseIterationChain.State
    
    private var pruningWatermark = 0
    private var lastPruningTask: Task<Void, any Error>?
    
    init(
        id: SnapshotIdentifier,
        persistence: DiskPersistence<AccessMode>,
        isBackup: Bool = false,
        isExtendedIterationCacheEnabled: Bool = false
    ) {
        self.id = id
        self.persistence = persistence
        self.isBackup = isBackup
        self.isExtendedIterationCacheEnabled = isExtendedIterationCacheEnabled
        
        self.iterationChain = SparseIterationChain()
        self.iterationChainState = .forwardEditsOnly
    }
    
    deinit {
        snapshotIterationPruningTask?.cancel()
    }
}

// MARK: - Common URL Accessors
extension Snapshot {
    /// The URL that points to the Snapshot directory.
    nonisolated var snapshotURL: URL {
        guard let components = try? id.components else { preconditionFailure("Components could not be determined for Snapshot.") }
        
        let baseURL = isBackup ? persistence.backupsURL : persistence.snapshotsURL
        
        return baseURL
            .appendingPathComponent(components.year, isDirectory: true)
            .appendingPathComponent(components.monthDay, isDirectory: true)
            .appendingPathComponent(components.hourMinute, isDirectory: true)
            .appendingPathComponent("\(id).snapshot", isDirectory: true)
    }
    
    /// The URL that points to the Manifest.json file.
    nonisolated var manifestURL: URL {
        snapshotURL.appendingPathComponent("Manifest.json", isDirectory: false)
    }
    
    /// The URL that points to the Dirty file.
    nonisolated var dirtyURL: URL {
        snapshotURL.appendingPathComponent("Dirty", isDirectory: false)
    }
    
    /// The URL that points to the `Iterations` directory.
    nonisolated var iterationsURL: URL {
        snapshotURL.appendingPathComponent("Iterations", isDirectory: true)
    }
    
    nonisolated func iterationURL(for id: SnapshotIterationIdentifier) -> URL {
        guard let components = try? id.components else { preconditionFailure("Components could not be determined for Snapshot.") }
        
        return iterationsURL
            .appendingPathComponent(components.year, isDirectory: true)
            .appendingPathComponent(components.monthDay, isDirectory: true)
            .appendingPathComponent(components.hourMinute, isDirectory: true)
            .appendingPathComponent("\(id).json", isDirectory: false)
    }
    
    /// The URL that points to the Datastores directory.
    nonisolated var datastoresURL: URL {
        snapshotURL.appendingPathComponent("Datastores", isDirectory: true)
    }
    
    /// The URL for a specific datastore within the snapshot.
    nonisolated func datastoreURL(for id: DatastoreIdentifier) -> URL {
        datastoresURL.appendingPathComponent("\(id).datastore", isDirectory: true)
    }
    
    /// The URL that points to the Inbox directory.
    nonisolated var inboxURL: URL {
        snapshotURL.appendingPathComponent("Inbox", isDirectory: true)
    }
}

// MARK: - Snapshot Manifest Management
extension Snapshot {
    /// Load the manifest from disk, or create a suitable starting value if such a file does not exist.
    private func loadManifest() throws -> SnapshotManifest {
        do {
            let data = try Data(contentsOf: manifestURL)

            let manifest = try JSONDecoder.shared.decode(SnapshotManifest.self, from: data)

            cachedManifest = manifest
            return manifest
        } catch FileNotFoundError() {
            return SnapshotManifest(id: id, modificationDate: Date())
        } catch {
            throw error
        }
    }
    
    func setExtendedIterationCacheEnabled(_ isEnabled: Bool) async {
        isExtendedIterationCacheEnabled = isEnabled
        
        /// If the extended cache is being disabled, and we are currently pruning, immediately stop and cancel the process.
        if !isEnabled {
            snapshotIterationPruningTask?.cancel()
            await snapshotIterationPruningTask?.value
        }
        
        await invalidateIterationChainState()
    }
    
    private func invalidateIterationChainState() async {
        let persistence = persistence
        switch iterationChainState {
        case .forwardEditsOnly:
            /// If we are currently only collecting forward edits, and the extended cache was just enabled, start the crawling process. Leave it empty until we make our first iteration read though.
            if isExtendedIterationCacheEnabled, iterationChain.first != nil {
                iterationChainState = .crawling(Task(name: "Iteration Chain Crawler") {
                    do {
                        try await crawlIterations()
                        iterationChainState = .complete
                    } catch {
                        print("Error crawling iterations: \(error)")
                        iterationChainState = .forwardEditsOnly
                    }
                    extendLifetime(persistence)
                })
            }
        case .crawling(let task):
            /// If we are currently crawling, but the extended cache was just disabled, cancel the crawling process and swap back to the incomplete state. Everything we have should still be valid.
            if !isExtendedIterationCacheEnabled {
                /// The state is managed by the task, and doesn't need to be set here, so long as we wait for it to complete up to the cancellation point.
                task.cancel()
                await task.value
            }
        case .complete:
            break
        }
    }
    
    private func crawlIterations() async throws {
        guard let currentIterationID = iterationChain.last?.iteration
        else { throw CrawlingCouldNotStartError() }
        
        /// Make sure the last known iteration is fresh from disk.
        var currentIteration = try self.loadIterationNoCache(for: currentIterationID)
        
        /// Walk the preceding iteration chain to the oldest iteration we can open, collecting the ones that form the train.
        while let precedingIterationID = currentIteration.precedingIteration, let precedingIteration = try? await loadIteration(for: precedingIterationID) {
            try Task.checkCancellation()
            
            currentIteration = precedingIteration
            iterationChain.append(iteration: precedingIteration)
        }
    }
    
    /// Load an iteration from cache or disk, or create a suitable starting value if such a file does not exist.
    func loadIteration(for iterationID: SnapshotIterationIdentifier?) async throws -> SnapshotIteration? {
        guard let iterationID else { return nil }
        if let iteration = cachedIterations[iterationID] {
            return iteration
        }
        return try loadIterationNoCache(for: iterationID)
    }
    
    /// Load an iteration from disk ignoring the current cached value, or create a suitable starting value if such a file does not exist.
    func loadIterationNoCache(for iterationID: SnapshotIterationIdentifier) throws -> SnapshotIteration {
        do {
            let data = try Data(contentsOf: iterationURL(for: iterationID))
            
            let iteration = try JSONDecoder.shared.decode(SnapshotIteration.self, from: data)
            
            if !isExtendedIterationCacheEnabled {
                cachedIterations.removeAll()
            }
            /// Make sure not to grow the cache unecessarily. 256 represents the smallest chunk in the chain that we care to have in memory at once
            if cachedIterations.count >= 256, let firstKey = cachedIterations.keys.first {
                cachedIterations.removeValue(forKey: firstKey)
            }
            cachedIterations[iteration.id] = iteration
            return iteration
        } catch {
            throw error
        }
    }
    
    /// Let the snapshot know it should enforce the specified retention policy from a given iteration. A task that can be awaited is returned
    @discardableResult
    func enforce(
        retentionPolicy: SnapshotRetentionPolicy,
        fromIteration iterationID: SnapshotIteration.ID,
        taskPriority: TaskPriority = .background
    ) -> Task<Void, Never> where AccessMode == ReadWrite {
//        print("Enforcing based on \(iterationID)")
        /// Since a previous request may have used a higher priority, make sure we maintain that priority since we are replacing that work.
        let resolvedTaskPriority = max(nextSnapshotIterationCandidateToEnforce?.taskPriority ?? taskPriority, taskPriority)
        nextSnapshotIterationCandidateToEnforce = nil
        
        let persistence = persistence
        
        if let pruningTask = snapshotIterationPruningTask {
            /// A pruning task is already in progress, so bookmark the iteration that needs to cleanup, and simply wait for the pruning task that eventually replaces the current one.
            nextSnapshotIterationCandidateToEnforce = (
                iterationID: iterationID,
                retentionPolicy: retentionPolicy,
                taskPriority: resolvedTaskPriority,
            )
            return Task.detached(name: "RetentionPolicyEnforcementWatcher") {
                await pruningTask.value
                await self.snapshotIterationPruningTask?.value
                extendLifetime(persistence)
            }
        }
        
        let pruningTask = Task.detached(name: "RetentionPolicyEnforcement", priority: resolvedTaskPriority) {
            do {
                try await self._enforce(retentionPolicy: retentionPolicy, fromIteration: iterationID)
            } catch {
                print("Error pruning: \(error)")
                
                /// Wait for any in-progress pruning tasks to finish.
                try? await self.drainPrunedIterations()
                
                /// The iteration chain is no longer complete, so set it back to `.forwardEditsOnly` so we can re-build it the next time.
                await self.resetIterationChainState()
            }
            /// Either enqueue the next policy enforcement, or reset task state if there is no more work slated.
            await self.enqueueNextPolicyEnforcement()
            extendLifetime(persistence)
        }
        
        snapshotIterationPruningTask = pruningTask
        return pruningTask
    }
    
    /// A private method for scheduling pruning tasks based on the retention policy to enforce.
    private func _enforce(
        retentionPolicy: SnapshotRetentionPolicy,
        fromIteration iterationID: SnapshotIteration.ID
    ) async throws where AccessMode == ReadWrite {
//        print("Pruning started for \(iterationID).")
        guard !retentionPolicy.isIndefinite else {
//            print("Current policy doesn't require any pruning, stopping early.")
            return
        }
        
        /// Enable the extended cache, and wait for it to be filled out before doing any work.
        await setExtendedIterationCacheEnabled(true)
        switch iterationChainState {
        case .forwardEditsOnly:
            /// The chain isn't in a state that can support proper pruning. Stop here.
            throw CancellationError()
        case .crawling(let task):
            await task.value
            /// If the chain doesn't settle on a `.complete`state, then it was prematurely cancelled. Simply pass that failure state along.
            guard case .complete = iterationChainState else {
                throw CancellationError()
            }
        case .complete:
            break
        }
        
        try Task.checkCancellation()
        
        let now = Date()
        print("Chain has \(iterationChain.count) entries over \(iterationChain.groups.count) groups.")
        
        /// Get a starting point from which we will start pruning iterations, without walking the entire graph.
        let (startingDistance, removedGroups) = iterationChain.removeIterations(failing: retentionPolicy, from: iterationID, now: now)
        
        var totalIterationCount = iterationChain.count
        var iterations: [SnapshotIteration.ID] = []
        var distance = startingDistance
        var nextIterationID = removedGroups.first?.first.iteration
        var mainlineRootIteration = try await loadIteration(for: iterationChain.last?.iteration)
        
        /// Walk the preceding iteration chain to the oldest iteration we can open, collecting the ones that should be pruned, and re-adding the ones that shouldn't back to the iteration chain.
        while let precedingIterationID = nextIterationID, let precedingIteration = try? await loadIteration(for: precedingIterationID) {
            try Task.checkCancellation()
            
            if !iterations.isEmpty || retentionPolicy.shouldIterationBePruned(now: now, creationDate: precedingIteration.creationDate, distance: distance) {
                iterations.append(precedingIteration.id)
            } else {
                /// The iteration isn't actually ready to be pruned, so add it back to the chain.
                iterationChain.append(iteration: precedingIteration)
                totalIterationCount += 1
                mainlineRootIteration = precedingIteration
            }
            
            nextIterationID = precedingIteration.precedingIteration
            
            distance += 1
            
            if (totalIterationCount + iterations.count) % 100 == 0 {
                print("Found \(iterations.count) iterations to prune. Keeping \(totalIterationCount) iterations.")
            }
        }
        
        guard
            iterations.count > 0,
            let mainlineRootIteration
        else {
            print("There were no iteration to prune, stopping early.")
            return
        }
        
        print("Will prune \(iterations.count) iterations. Keeping \(totalIterationCount) iterations.")
        
        /// Prune iterations from oldest to newest along the mainline.
        while let iterationID = iterations.popLast(), let iteration = try await loadIteration(for: iterationID) {
            /// The current index, since we just removed the last element.
            let index = iterations.count
            let mainlineSuccessorIterationID = index > 0 ? iterations[index-1] : mainlineRootIteration.id
            
            if index % 100 == 0 {
                print("\(index) iterations left to delete.")
            }
            
            var iterationsToPrune: [SnapshotIteration] = []
            var successorCandidatesToCheck = iteration.successiveIterations
            successorCandidatesToCheck.removeAll { $0 == mainlineSuccessorIterationID }
            
            /// Walk the non-mainline successor candidates all the way back up so newer iterations are pruned before the ones that reference them. We pull items off from the end, and add new ones to the beginning to make sure they stay in graph order.
            while let successorCandidateID = successorCandidatesToCheck.popLast() {
                try Task.checkCancellation()
                guard let successorIteration = try? await loadIteration(for: successorCandidateID)
                else { continue }
                
                iterationsToPrune.append(successorIteration)
                successorCandidatesToCheck.insert(contentsOf: successorIteration.successiveIterations, at: 0)
            }
            
            /// First, remove the branch of iterations based on the one we are removing, but representing a history that was previously reverted (the non-mainline successors, from newest to oldest).
            /// Prune the iterations in atomic tasks so they don't get cancelled mid-way, and instead check for cancellation in between iterations.
            while let iteration = iterationsToPrune.popLast() {
                try await pruneIteration(iteration, mode: .pruneAdded, shouldDelete: true)
            }
            
            /// Finally, prune and delete the iteration itself.
            try await pruneIteration(iteration, mode: .pruneRemoved, shouldDelete: true)
        }
        
        /// Once we deleted all iterations that fall outside the set policy, prune the last iteration that we are keeping.
        try await pruneIteration(mainlineRootIteration, mode: .pruneRemoved, shouldDelete: false)
        /// Wait for all in-progress pruning operations to finish.
        try await drainPrunedIterations()
        print("Pruning complete!")
    }
    
    /// Reset the iteration chain from an unknown `.complete` state to a known waiting state.
    private func resetIterationChainState() {
        switch iterationChainState {
        case .forwardEditsOnly:
            break
        case .crawling:
            preconditionFailure("Iteration chain is currently crawling when it should have been complete.")
        case .complete:
            break
        }
        
        /// The iteration chain is no longer complete, so set it back to `.forwardEditsOnly` so we can re-build it the next time.
        iterationChainState = .forwardEditsOnly
    }
    
    /// Swap the current pruning task with one for the next candidate to enforce, if available.
    private func enqueueNextPolicyEnforcement() where AccessMode == ReadWrite {
        snapshotIterationPruningTask = nil
        if let nextCandidate = nextSnapshotIterationCandidateToEnforce {
            enforce(
                retentionPolicy: nextCandidate.retentionPolicy,
                fromIteration: nextCandidate.iterationID,
                taskPriority: nextCandidate.taskPriority,
            )
        }
    }
    
    /// Wait for all pruning tasks currently enqueued to finish.
    func checkPruningFinished() async {
        while let pruningTask = snapshotIterationPruningTask {
            await pruningTask.value
        }
    }
    
    /// Cancel the current pruning task.
    nonisolated func cancelPruning() {
        Task {
            await snapshotIterationPruningTask?.cancel()
        }
    }
    
    /// Concurrently prune iterations, but force deletions to happen serially, and only after their associated prune succeeds.
    func pruneIteration(_ iteration: SnapshotIteration, mode: SnapshotPruneMode, shouldDelete: Bool) async throws {
        let persistence = persistence
        let pruneTask = Task(name: "Concurrent Prune Iteration \(iteration.id)") {
            try await pruneIteration(iteration, mode: mode)
            extendLifetime(persistence)
            return iteration
        }
        lastPruningTask = Task(name: "Serial Prune Iteration \(iteration.id)") { [lastPruningTask] in
            try await lastPruningTask?.value
            let iteration = try await pruneTask.value
            if shouldDelete {
                deleteIteration(iteration)
            }
            extendLifetime(persistence)
        }
        pruningWatermark += 1
        
        /// If we've enqueued at least 64 tasks, pause before returning control so we can drain the pool, checking for cancellation in the process.
        if pruningWatermark >= 64 {
            try Task.checkCancellation()
            try await drainPrunedIterations()
        }
    }
    
    /// An internal method for making sure all pruning tasks complete before returning.
    func drainPrunedIterations() async throws {
        pruningWatermark = 0
        try await lastPruningTask?.value
    }
    
    private func pruneIteration(_ iteration: SnapshotIteration, mode: SnapshotPruneMode) async throws {
        /// Collect the datastores and related roots we'll be deleting.
        /// - For datastores, only collect the ones we'll be deleting since the ones we are keeping won't be making references to other deletable assets.
        /// - For the datastore roots, we'll be deleting the entries that are being removed (relative to the direction we are removing from, so the removed ones from the oldest edge, and the added ones from the newest edge, as determined by the caller), while we'll be checking for more assets to remove from entries that have just been added, but only when removing from the oldest edge. We only do this for the oldest edge because entries that have been "removed" from the newest edge are actually being _restored_ and not replaced, which maintains symmetry in a non-obvious way.
        let datastoresToPruneAndDelete = iteration.datastoresToPrune(for: mode)
        var datastoreRootsToPruneAndDelete = iteration.datastoreRootsToPrune(for: mode, options: .pruneAndDelete)
        var datastoreRootsToPrune = iteration.datastoreRootsToPrune(for: mode, options: .pruneOnly)
        
        /// Start by deleting and pruning roots as needed. We attempt to do this twice, as older versions of the persistence (prior to 0.4) didn't record the datastore ID along with the root id, which would therefore require extra work.
        /// First, delete the root entries we know to be removed.
        for datastoreRoot in datastoreRootsToPruneAndDelete {
            guard let datastoreID = datastoreRoot.datastoreID else { continue }
            let datastore = datastores[datastoreID] ?? DiskPersistence<AccessMode>.Datastore(id: datastoreID, snapshot: self)
            do {
                try await datastore.pruneRootObject(with: datastoreRoot.datastoreRootID, mode: mode, shouldDelete: true)
            } catch FileNotFoundError() {
                /// This datastore root is already gone.
            } catch {
                print("Could not delete datastore root \(datastoreRoot): \(error)")
                throw error
            }
            datastoreRootsToPruneAndDelete.remove(datastoreRoot)
        }
        /// Prune the root entries that were just added, as they themselves refer to other deleted assets.
        for datastoreRoot in datastoreRootsToPrune {
            guard let datastoreID = datastoreRoot.datastoreID else { continue }
            let datastore = datastores[datastoreID] ?? DiskPersistence<AccessMode>.Datastore(id: datastoreID, snapshot: self)
            do {
                try await datastore.pruneRootObject(with: datastoreRoot.datastoreRootID, mode: mode, shouldDelete: false)
            } catch FileNotFoundError() {
                /// This datastore root is already gone.
            } catch {
                print("Could not prune datastore root \(datastoreRoot): \(error)")
                throw error
            }
            datastoreRootsToPrune.remove(datastoreRoot)
        }
        
        /// If any references remain, funnel into this code path for very old persistences.
        if !datastoreRootsToPruneAndDelete.isEmpty || !datastoreRootsToPrune.isEmpty {
            for (_, datastoreInfo) in iteration.dataStores {
                /// Skip any roots for datastores being deleted, since we'll just unlink the whole directory in that case.
                guard !datastoresToPruneAndDelete.contains(datastoreInfo.id) else { continue }
                
                let datastore = datastores[datastoreInfo.id] ?? DiskPersistence<AccessMode>.Datastore(id: datastoreInfo.id, snapshot: self)
                
                /// Delete the root entries we know to be removed.
                for datastoreRoot in datastoreRootsToPruneAndDelete {
                    do {
                        try await datastore.pruneRootObject(with: datastoreRoot.datastoreRootID, mode: mode, shouldDelete: true)
                        datastoreRootsToPruneAndDelete.remove(datastoreRoot)
                    } catch FileNotFoundError() {
                        /// This datastore did not contain the specified root, skip it for now.
                    } catch {
                        print("Could not delete datastore root \(datastoreRoot): \(error).")
                        throw error
                    }
                }
                
                /// Prune the root entries that were just added, as they themselves refer to other deleted assets.
                for datastoreRoot in datastoreRootsToPrune {
                    do {
                        try await datastore.pruneRootObject(with: datastoreRoot.datastoreRootID, mode: mode, shouldDelete: false)
                        datastoreRootsToPrune.remove(datastoreRoot)
                    } catch FileNotFoundError() {
                        /// This datastore did not contain the specified root, skip it for now.
                    } catch {
                        print("Could not prune datastore root \(datastoreRoot): \(error).")
                        throw error
                    }
                }
            }
        }
        
        /// Delete any datastores in their entirety.
        for datastoreID in datastoresToPruneAndDelete {
            try? FileManager.default.removeItem(at: datastoreURL(for: datastoreID))
        }
    }
    
    /// Delete the iteration. Note that an iteration should be pruned first to delete related files that are specific to the iteration itself.
    private func deleteIteration(_ iteration: SnapshotIteration) {
        cachedIterations.removeValue(forKey: iteration.id)
        
        let iterationURL = iterationURL(for: iteration.id)
        try? FileManager.default.removeItem(at: iterationURL)
        try? FileManager.default.removeDirectoryIfEmpty(url: iterationURL.deletingLastPathComponent(), recursivelyRemoveParents: true)
    }
    
    /// Write the specified manifest to the store, and cache the results in ``Snapshot/cachedManifest``.
    private func write(manifest: SnapshotManifest) throws where AccessMode == ReadWrite {
        /// Make sure the directories exists first.
        if cachedManifest == nil {
            try FileManager.default.createDirectory(at: snapshotURL, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: datastoresURL, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(at: inboxURL, withIntermediateDirectories: true)
        }

        /// Encode the provided manifest, and write it to disk.
        let data = try JSONEncoder.shared.encode(manifest)
        try data.write(to: manifestURL, options: .atomic)

        /// Update the cache since we know what it should be.
        cachedManifest = manifest
    }
    
    /// Write the specified iteration to the store, and cache the results in ``Snapshot/cachedIterations``.
    private func write(iteration: SnapshotIteration) throws where AccessMode == ReadWrite {
        let iterationURL = iterationURL(for: iteration.id)
        /// Make sure the directories exists first.
        try FileManager.default.createDirectory(at: iterationURL.deletingLastPathComponent(), withIntermediateDirectories: true)

        /// Encode the provided iteration, and write it to disk.
        let data = try JSONEncoder.shared.encode(iteration)
        try data.write(to: iterationURL, options: [])

        /// Update the cache since we know what it should be.
        if !isExtendedIterationCacheEnabled {
            cachedIterations.removeAll()
        }
        cachedIterations[iteration.id] = iteration
    }

    /// Load and update the manifest in an updater.
    ///
    /// This method loads the ``SnapshotManifest`` from cache, offers it to be mutated, then writes it back to disk, if it changed. It is up to the caller to update the modification date of the store.
    ///
    /// - Note: Calling this method when no manifest exists on disk will create it, even if no changes occur in the block.
    /// - Parameter updater: An updater that takes a mutable reference to a manifest, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `updater`.
    func updatingManifest<T: Sendable>(
        updater: (_ manifest: inout SnapshotManifest, _ iteration: inout SnapshotIteration) async throws -> T
    ) async throws -> T where AccessMode == ReadWrite {
        if let (manifest, iteration) = SnapshotTaskLocals.manifest(for: persistence) {
            var updatedManifest = manifest
            var updatedIteration = iteration
            let returnValue = try await updater(&updatedManifest, &updatedIteration)
            
            guard updatedManifest == manifest, updatedIteration == iteration
            else { throw DiskPersistenceInternalError.nestedSnapshotWrite }
            
            return returnValue
        }
        
        return try await manifestTransactionStream.withTransaction {
            /// Load the manifest so we have a fresh copy, unless we have a cached copy already.
            var manifest = try cachedManifest ?? self.loadManifest()
            let precedingIteration = try await self.loadIteration(for: manifest.currentIteration)
            var iteration = precedingIteration ?? SnapshotIteration()

            /// Let the updater do something with the manifest, storing the variable on the Task Local stack.
            let returnValue = try await SnapshotTaskLocals.with(manifest: manifest, iteration: iteration, for: persistence) {
                try await updater(&manifest, &iteration)
            }
            
            /// Only write to the store if we changed the manifest for any reason
            if iteration.isMeaningfullyChanged(from: precedingIteration) {
                iteration.creationDate = Date()
                iteration.id = SnapshotIterationIdentifier(date: iteration.creationDate)
                iteration.precedingIteration = precedingIteration?.id
                
                try write(iteration: iteration)
            }
            
            manifest.currentIteration = iteration.id
            
            /// Only write to the store if we changed the manifest for any reason
            if manifest != cachedManifest {
                try write(manifest: manifest)
                
                /// Add the latest iteration to the chain now that it's been written to disk for this snapshot.
                iterationChain.prepend(iteration: iteration)
                await invalidateIterationChainState()
            }
            return returnValue
        }
    }

    /// Load the manifest in an updater.
    ///
    /// This method loads the ``SnapshotManifest`` from cache.
    ///
    /// - Parameter accessor: An accessor that takes an immutable reference to a manifest, and will forward the returned value to the caller.
    /// - Returns: The value returned from the `accessor`.
    @_disfavoredOverload
    func readingManifest<T: Sendable>(
        accessor: (_ manifest: SnapshotManifest, _ iteration: SnapshotIteration) async throws -> T
    ) async throws -> T {
        if let (manifest, iteration) = SnapshotTaskLocals.manifest(for: persistence) {
            return try await accessor(manifest, iteration)
        }
        
        return try await manifestTransactionStream.withTransaction {
            /// Load the manifest so we have a fresh copy, unless we have a cached copy already.
            let manifest = try cachedManifest ?? self.loadManifest()
            let iteration = try await self.loadIteration(for: manifest.currentIteration) ?? SnapshotIteration()

            /// Let the accessor do something with the manifest, storing the variable on the Task Local stack.
            return try await SnapshotTaskLocals.with(manifest: manifest, iteration: iteration, for: persistence) {
                try await accessor(manifest, iteration)
            }
        }
    }
}

private enum SnapshotTaskLocals {
    @TaskLocal
    static var manifestStorage: [ObjectIdentifier : (SnapshotManifest, SnapshotIteration)] = [:]
    
    static func manifest<AccessMode: _AccessMode>(for persistence: DiskPersistence<AccessMode>) -> (SnapshotManifest, SnapshotIteration)? {
        manifestStorage[ObjectIdentifier(persistence)]
    }
    
    static func with<AccessMode: _AccessMode, R>(
        isolation actor: isolated (any Actor)? = #isolation,
        manifest: SnapshotManifest,
        iteration: SnapshotIteration,
        for persistence: DiskPersistence<AccessMode>,
        operation: () async throws -> R
    ) async rethrows -> R {
        var currentStorage = manifestStorage
        currentStorage[ObjectIdentifier(persistence)] = (manifest, iteration)
        
        return try await $manifestStorage.withValue(currentStorage, operation: operation)
    }
}

enum SnapshotPruneMode {
    case pruneRemoved
    case pruneAdded
}

enum SnapshotPruneOptions {
    case pruneAndDelete
    case pruneOnly
}

// MARK: - Datastore Management
extension Snapshot {
    /// Load the datastore for the given key.
    func loadDatastore(
        for key: DatastoreKey,
        from iteration: SnapshotIteration
    ) -> (DiskPersistence<AccessMode>.Datastore, DatastoreRootIdentifier?) {
        let datastoreInfo = if let info = iteration.dataStores[key] {
            (id: info.id, root: info.root)
        } else {
            (id: DatastoreIdentifier(name: key.rawValue), root: DatastoreRootIdentifier?.none)
        }
        
        if let datastore = datastores[datastoreInfo.id] {
            return (datastore, datastoreInfo.root)
        }
        
        let datastore = DiskPersistence<AccessMode>.Datastore(id: datastoreInfo.id, snapshot: self)
        datastores[datastoreInfo.id] = datastore
        
        return (datastore, datastoreInfo.root)
    }
}

// MARK: - Snapshotting

extension Snapshot {
    @discardableResult
    func copy(
        into persistence: DiskPersistence<ReadWrite>,
        actionName: String? = nil,
        newSnapshotIdentifier: SnapshotIdentifier? = nil,
        targetPageSize: Int
    ) async throws -> Snapshot<ReadWrite> {
        try await readingManifest { manifest, iteration in
            /// Create a new snapshot and iteration to load data into
            let newSnapshot = Snapshot<ReadWrite>(id: newSnapshotIdentifier ?? SnapshotIdentifier(), persistence: persistence)
            
            let creationDate = (try? newSnapshot.id.components)?.date ?? Date()
            var newIteration = SnapshotIteration(
                id: SnapshotIterationIdentifier(rawValue: newSnapshot.id.rawValue),
                creationDate: creationDate,
                precedingIteration: iteration.id,
                precedingSnapshot: id,
                successiveIterations: [],
                actionName: actionName,
                dataStores: [:],
                addedDatastores: [],
                removedDatastores: [],
                addedDatastoreRoots: [],
                removedDatastoreRoots: []
            )
            
            /// Iterate through each datastore and copy the data over
            for (_, datastoreInfo) in iteration.dataStores {
                let (datastore, _) = loadDatastore(for: datastoreInfo.key, from: iteration)
                try await datastore.copy(
                    rootIdentifier: datastoreInfo.root,
                    datastoreKey: datastoreInfo.key,
                    into: newSnapshot,
                    iteration: &newIteration,
                    targetPageSize: targetPageSize
                )
            }
            
            /// Create a new manifest with our written data.
            let newManifest = SnapshotManifest(
                id: newSnapshot.id,
                modificationDate: creationDate,
                currentIteration: newIteration.id
            )
            
            /// Write the iteration and manifest records so the persistence is complete.
            try await newSnapshot.write(iteration: newIteration)
            try await newSnapshot.write(manifest: newManifest)
            return newSnapshot
        }
    }
}

fileprivate struct CrawlingCouldNotStartError: Error {}
