#include <Core/Defines.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>

#include <Core/Names.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MutationCommands.h>
#include <base/hex.h>
#include <base/interpolate.h>
#include <base/isSharedPtrUnique.h>
#include <Common/DateLUTImpl.h>
#include <Common/FailPoint.h>
#include <Common/Macros.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEventsScope.h>
#include <Common/StringUtils.h>
#include <Common/ThreadFuzzer.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/noexcept_scope.h>
#include <Common/Jemalloc.h>
#include <Common/JemallocMergeTreeArena.h>
#include <Common/randomDelay.h>
#include <Common/thread_local_rng.h>
#include <Common/typeid_cast.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/ThreadStatus.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerUUID.h>

#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/setThreadName.h>

#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/Common.h>
#include <Storages/MergeTree/Compaction/PartitionStatistics.h>
#include <Storages/MergeTree/Compaction/MergePredicates/DistributedMergePredicate.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Core/Settings.h>
#include <Core/UUID.h>

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/SingleDiskVolume.h>

#include <base/sleep.h>
#include <base/sort.h>

#include <Storages/buildQueryTreeForShard.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Freeze.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/LeaderElection.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/OverlappingPartCovering.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/Compaction/ConstructFuturePart.h>
#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/AmateurMergeTreeSink.h>
#include <Storages/MergeTree/ZeroCopyLock.h>
#include <Storages/PartitionCommands.h>
#include <Storages/StorageAmateurMergeTree.h>
#include <Storages/VirtualColumnUtils.h>

#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseReplicated.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sinks/EmptySink.h>

#include <Planner/Utils.h>

#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/Expect404ResponseScope.h>

#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Backups/BackupEntriesCollector.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/IBackupEntry.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>

#include <Common/scope_guard_safe.h>
#include <IO/SharedThreadPools.h>

#include <base/types.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>

#include <ctime>
#include <filesystem>
#include <vector>


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event InsertedWideParts;
    extern const Event InsertedCompactParts;
    extern const Event InsertedInMemoryParts;
    extern const Event MergedIntoWideParts;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NOT_INITIALIZED;
    extern const int NO_ZOOKEEPER;
    extern const int READONLY;
    extern const int TOO_LESS_LIVE_REPLICAS;
    extern const int REPLICA_IS_ACTIVE;
    extern const int LOGICAL_ERROR;
    extern const int TABLE_IS_DROPPED;
    extern const int TABLE_IS_READ_ONLY;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int REPLICA_IS_NOT_ACTIVE;
    extern const int NETWORK_ERROR;
    extern const int NO_SUCH_REPLICA;
    extern const int REPLICA_STATUS_CHANGED;
    extern const int ALL_REPLICAS_ARE_STALE;
    extern const int ALL_REPLICAS_LOST;
    extern const int REPLICA_ALREADY_EXISTS;
    extern const int INCOMPATIBLE_COLUMNS;
}

namespace FailPoints
{
    extern const char amateur_merge_commit_zk_fail_after_op[];
    extern const char amateur_merge_commit_zk_fail_before_op[];
}

namespace Setting
{
    extern const SettingsUInt64 max_partitions_per_insert_block;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsUInt64 max_number_of_merges_with_ttl_in_pool;
    extern const MergeTreeSettingsUInt64 cleanup_delay_period;
}

class AmateurMergePredicate : public DistributedMergePredicate<ActiveDataPartSet, StorageAmateurMergeTree>
{
    std::shared_ptr<ActiveDataPartSet> prev_virtual_parts_storage;
    std::shared_ptr<CommittingBlocks> committing_blocks_storage;
    std::shared_ptr<ActiveDataPartSet> virtual_parts_storage;
public:
    AmateurMergePredicate(
        const StorageAmateurMergeTree & storage,
        zkutil::ZooKeeperPtr zookeeper,
        const String & zookeeper_path,
        std::optional<PartitionIdsHint> partitions_hint = std::nullopt)
        : DistributedMergePredicate(std::move(partitions_hint))
    {
        prev_virtual_parts_storage = std::make_shared<ActiveDataPartSet>(
            storage.format_version,
            zookeeper->getChildren(fs::path(zookeeper_path) / "parts"));
        prev_virtual_parts_ptr = prev_virtual_parts_storage.get();

        committing_blocks_storage = std::make_shared<CommittingBlocks>(getCommittingBlocks(
            zookeeper, zookeeper_path, this->partition_ids_hint, false));
        committing_blocks_ptr = committing_blocks_storage.get();

        virtual_parts_storage = std::make_shared<ActiveDataPartSet>(
            storage.format_version,
            zookeeper->getChildren(fs::path(zookeeper_path) / "parts"));
        virtual_parts_ptr = virtual_parts_storage.get();

        mutations_state_ptr = &storage;
    }
};

class AmateurMergeTreePartsCollector final : public IPartsCollector
{
    const StorageAmateurMergeTree & storage;
    std::shared_ptr<const AmateurMergePredicate> merge_pred;
public:
    AmateurMergeTreePartsCollector(
        const StorageAmateurMergeTree & storage_,
        std::shared_ptr<const AmateurMergePredicate> merge_pred_)
        : storage(storage_), merge_pred(std::move(merge_pred_)) {}

    CollectedPartsRanges grabAllPossibleRanges(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr &,
        const time_t & current_time,
        const std::optional<PartitionIdsHint> & partitions_hint,
        LogSeriesLimiter & series_log) const override
    {
        auto parts = filterByPartitions(
            storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active}),
            partitions_hint);

        auto partitions_stats = calculateStatisticsForParts(parts, current_time);

        auto can_use = [&](const MergeTreeDataPartPtr & part) -> std::expected<void, PreformattedMessage>
        {
            if (storage.currently_merging_mutating_parts.contains(part))
                return std::unexpected(PreformattedMessage::create("Part {} is currently being merged", part->name));

            return merge_pred->canUsePartInMerges(part->name, part->info);
        };

        auto ranges_vec = splitRangeByPredicate(std::move(parts), std::move(can_use), series_log);
        return {constructPartsRanges(std::move(ranges_vec), metadata_snapshot, storage.getStoragePolicy(), current_time), std::move(partitions_stats)};
    }

    std::expected<PartsRange, PreformattedMessage> grabAllPartsInsidePartition(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr &,
        const time_t & current_time,
        const std::string & partition_id) const override
    {
        auto parts = filterByPartitions(
            storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active}),
            PartitionIdsHint{partition_id});

        auto can_use = [&](const MergeTreeDataPartPtr & part) -> std::expected<void, PreformattedMessage>
        {
            if (storage.currently_merging_mutating_parts.contains(part))
                return std::unexpected(PreformattedMessage::create("Part {} is currently being merged", part->name));

            return merge_pred->canUsePartInMerges(part->name, part->info);
        };

        if (auto result = checkAllPartsSatisfyPredicate(parts, std::move(can_use)); !result)
            return std::unexpected(std::move(result.error()));

        auto ranges = constructPartsRanges({std::move(parts)}, metadata_snapshot, storage.getStoragePolicy(), current_time);
        chassert(ranges.size() == 1);
        return std::move(ranges.front());
    }
};



zkutil::ZooKeeperPtr StorageAmateurMergeTree::tryGetZooKeeper() const
{
    std::lock_guard lock(current_zookeeper_mutex);
    return current_zookeeper;
}

zkutil::ZooKeeperPtr StorageAmateurMergeTree::getZooKeeper() const
{
    auto zk = tryGetZooKeeper();
    if (!zk)
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "No ZooKeeper session for table {}", getStorageID().getFullTableName());
    return zk;
}

zkutil::ZooKeeperPtr StorageAmateurMergeTree::getZooKeeperAndAssertNotReadonly() const
{
    assertNotReadonly();
    return getZooKeeper();
}

zkutil::ZooKeeperPtr StorageAmateurMergeTree::getZooKeeperAndAssertNotStaticStorage() const
{
    assertNotStaticStorage();
    return getZooKeeper();
}

zkutil::ZooKeeperPtr StorageAmateurMergeTree::getZooKeeperIfTableShutDown() const
{
    return tryGetZooKeeper();
}

void StorageAmateurMergeTree::setZooKeeper()
{
    auto zookeeper = getContext()->getZooKeeper();
    if (zookeeper->expired())
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "ZooKeeper session expired for table {}", getStorageID().getFullTableName());

    {
        std::lock_guard lock(current_zookeeper_mutex);
        current_zookeeper = zookeeper;
    }
}

String StorageAmateurMergeTree::getEndpointName() const
{
    return getStorageID().getFullTableName();
}

CursorPromotersMap StorageAmateurMergeTree::buildPromoters()
{
    return {};
}

StorageAmateurMergeTree::StorageAmateurMergeTree(
    const TableZnodeInfo & zookeeper_info_,
    LoadingStrictnessLevel mode,
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_,
    bool need_check_structure,
    const ZooKeeperRetriesInfo & create_query_zookeeper_retries_info_)
    : MergeTreeData(
        table_id_,
        metadata_,
        context_,
        date_column_name,
        merging_params_,
        std::move(settings_),
        true,
        mode,
        [](const std::string &) { })
    , zookeeper_info(zookeeper_info_)
    , zookeeper_path(zookeeper_info_.full_path)
    , replica_name(zookeeper_info_.replica_name)
    , replica_path(fs::path(zookeeper_info_.full_path) / "replicas" / zookeeper_info_.replica_name)
    , create_query_zookeeper_retries_info(create_query_zookeeper_retries_info_)
    , writer(*this)
    , merger_mutator(*this)
    , deduplication_hashes_cache(*this, "deduplication_hashes")
    , async_block_ids_cache(*this, "async_blocks")
{
    auto table_disks = getDisks();
    if (table_disks.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AmateurMergeTree doesn't support multiple disks");

    for (const auto & disk : table_disks)
    {
        if (disk->getDataSourceDescription().metadata_type != MetadataStorageType::Amateur)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "AmateurMergeTree works only with s3 disk type with amateur metadata");
    }

    // TODO: initialize background tasks
    parts_syncing_task = getContext()->getSchedulePool().createTask(
        getStorageID(), getStorageID().getFullTableName() + " (StorageAmateurMergeTree::partsSyncingTask)", [this]{ partsSyncingTask(); });

    parts_syncing_task->deactivate();

    cleanup_task = getContext()->getSchedulePool().createTask(
        getStorageID(), getStorageID().getFullTableName() + " (StorageAmateurMergeTree::cleanupTask)", [this]{ cleanupTask(); });

    cleanup_task->deactivate();

    bool has_zookeeper = getContext()->hasZooKeeper() || getContext()->hasAuxiliaryZooKeeper(zookeeper_info.zookeeper_name);
    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::StorageAmateurMergeTree");
    if (has_zookeeper)
    {
        /// It's possible for getZooKeeper() to timeout if zookeeper host(s) can't
        /// be reached. In such cases Poco::Exception is thrown after a connection
        /// timeout - refer to src/Common/ZooKeeper/ZooKeeperImpl.cpp:866 for more info.
        ///
        /// Side effect of this is that the CreateQuery gets interrupted and it exits.
        /// But the data Directories for the tables being created aren't cleaned up.
        /// This unclean state will hinder table creation on any retries and will
        /// complain that the Directory for table already exists.
        ///
        /// To achieve a clean state on failed table creations, catch this error and
        /// call dropIfEmpty() method only if the operation isn't ATTACH then proceed
        /// throwing the exception. Without this, the Directory for the tables need
        /// to be manually deleted before retrying the CreateQuery.
        try
        {
            setZooKeeper();
        }
        catch (...)
        {
            if (mode < LoadingStrictnessLevel::ATTACH)
            {
                dropIfEmpty();
                throw;
            }

            current_zookeeper = nullptr;
        }
    }

    if (relative_data_path_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageAmateurMergeTree requires a non-empty data path");

    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
    has_metadata_in_zookeeper = true;

    try
    {
        bool is_first_replica = createTableIfNotExists(metadata_snapshot, getCreateQueryZooKeeperRetriesInfo());

        try
        {
            /// NOTE If it's the first replica, these requests to ZooKeeper look redundant, we already know everything.

            /// Allow structure mismatch for secondary queries from Replicated database.
            /// It may happen if the table was altered just after creation.
            /// Metadata will be updated in cloneMetadataIfNeeded(...), metadata_version will be 0 for a while.
            int32_t metadata_version = 0;
            bool same_structure = checkTableStructure(zookeeper_path, metadata_snapshot, &metadata_version, need_check_structure, getCreateQueryZooKeeperRetriesInfo());

            if (same_structure)
            {
                /** We change metadata_snapshot so that `createReplica` method will create `metadata_version` node in ZooKeeper
                  * with version of table '/metadata' node in Zookeeper.
                  *
                  * Otherwise `metadata_version` for not first replica will be initialized with 0 by default.
                  */
                setInMemoryMetadata(metadata_snapshot->withMetadataVersion(metadata_version));
                metadata_snapshot = getInMemoryMetadataPtr(getContext(), true);
            }
        }
        catch (Coordination::Exception & e)
        {
            if (!is_first_replica && e.code == Coordination::Error::ZNONODE)
                throw Exception(ErrorCodes::ALL_REPLICAS_LOST, "Table {} was suddenly removed.", zookeeper_path);
            throw;
        }

        if (!is_first_replica)
            createReplica(metadata_snapshot, getCreateQueryZooKeeperRetriesInfo());

        if (!has_metadata_in_zookeeper.has_value() || *has_metadata_in_zookeeper)
            createSharedDataPath(relative_data_path_, getCreateQueryZooKeeperRetriesInfo());
    }
    catch (...)
    {
        /// If replica was not created, rollback creation of data directory.
        dropIfEmpty();
        throw;
    }

    relative_data_path = getSharedDataPath();
    initializeDirectoriesAndFormatVersion(relative_data_path, false, date_column_name);

    std::optional<std::unordered_set<std::string>> expected_parts;
    std::vector<std::string> parts_in_zookeeper;
    if (current_zookeeper->tryGetChildren(fs::path(zookeeper_path) / "parts", parts_in_zookeeper) == Coordination::Error::ZOK)
    {
        expected_parts.emplace();
        for (const auto & part : parts_in_zookeeper)
            expected_parts->insert(part);
    }

    loadDataParts(false, expected_parts);
    prewarmCaches(getActivePartsLoadingThreadPool().get(), getCachesToPrewarm(0));

    LOG_INFO(log, "StorageAmateurMergeTree created for {}", table_id_.getNameForLogs());

    initialization_done = true;
}


StorageAmateurMergeTree::~StorageAmateurMergeTree()
{
    LOG_TRACE(log, "~StorageAmateurMergeTree");
    try
    {
        if (!shutdown_called)
            shutdown(false);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}


void StorageAmateurMergeTree::partsSyncingTask()
try
{
    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::partsSyncingTask");
    Stopwatch watch;

    auto disk = getStoragePolicy()->getDisks().front();

    auto zookeeper = getZooKeeper();

    // First, get node stats from ZK, then get node contents. Node stats will be used
    // as a current epoch later, when local state is synced, so it should guarantee
    // that storage won't use any data below the epoch.
    Coordination::Stat parts_stat;
    zookeeper->get(fs::path(zookeeper_path) / "parts", &parts_stat);
    Int64 parts_pzxid = parts_stat.pzxid;

    const auto active_parts = zookeeper->getChildren(fs::path(zookeeper_path) / "parts");

    PartLoadingTreeNodes parts_to_add;
    std::vector<MergeTreePartInfo> drop_ranges_to_add;
    {
        auto part_lock = lockParts();
        for (const auto & part_name : active_parts)
        {
            if (auto part_info = MergeTreePartInfo::tryParsePartName(part_name, format_version))
            {
                if (part_info->isFakeDropRangePart())
                    drop_ranges_to_add.emplace_back(*part_info);
                else if (auto it = data_parts_by_info.find(*part_info); it == data_parts_by_info.end())
                    parts_to_add.emplace_back(std::make_shared<PartLoadingTree::Node>(*part_info, part_name, disk));
            }
        }
    }

    for (const auto & my_part : parts_to_add)
    {
        auto res = loadDataPartWithRetries(
            my_part->info, my_part->name, disk,
            DataPartState::PreActive, data_parts_mutex, 100,
            5000, 3);

        if (res.is_broken)
        {
            LOG_ERROR(log, "The new data part {} appears broken - skip loading", res.part->name);
        }
        else
        {
            {
                auto part_lock = lockParts();
                Transaction transaction(*this, nullptr);
                preparePartForCommit(res.part, transaction, part_lock, false, false);
                transaction.commit(part_lock);
            }
        }
    }

    for (const auto & drop_range : drop_ranges_to_add)
    {
        LOG_TRACE(log, "Should process drop range {}", drop_range.getPartNameForLogs());

        PartsToRemoveFromZooKeeper parts_to_remove;
        {
            auto data_parts_lock = lockParts();
            parts_to_remove = removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(NO_TRANSACTION_RAW, drop_range, data_parts_lock, false, false);
        }
    }

    // After in-memory state is synced with an active parts set received
    // from ZK, it's safe to store its epoch as a current one
    current_epoch.store(parts_pzxid, std::memory_order_release);

    // Publish the minimum active read epoch: if there are in-flight snapshots
    // (active reads), we cannot advance past the oldest one — that snapshot's
    // epoch is the true minimum active read epoch of this replica.
    cleanupStaleSnapshotGuards();
    Int64 min_epoch = 0;
    {
        std::lock_guard lock(active_snapshot_epochs_mutex);
        if (active_snapshot_epochs.empty())
            min_epoch = parts_pzxid;
        else
            min_epoch = *active_snapshot_epochs.begin();
    }

    if (min_epoch > last_published_epoch.load(std::memory_order_acquire))
    {
        String epoch_str = toString(min_epoch);
        auto code = zookeeper->trySet(fs::path(replica_path) / "epoch", epoch_str, -1);
        if (code == Coordination::Error::ZNONODE)
        {
            zookeeper->create(fs::path(replica_path) / "epoch", epoch_str, zkutil::CreateMode::Persistent);
        }
        else if (code != Coordination::Error::ZOK)
        {
            throw Coordination::Exception::fromPath(code, fs::path(replica_path) / "epoch");
        }
        last_published_epoch.store(min_epoch, std::memory_order_release);
    }

    watch.stop();

    parts_syncing_task->scheduleAfter(1000);
}
catch (...)
{
    tryLogCurrentException(log, "Failed to refresh parts");
}


void StorageAmateurMergeTree::cleanupTask()
{
    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::cleanupTask");

    auto storage_settings = getSettings();
    auto cleanup_delay = (*storage_settings)[MergeTreeSetting::cleanup_delay_period] * 1000;

    try
    {
        auto zookeeper = getZooKeeper();

        // Find all known stale parts with their epochs
        auto global_stale_parts = zookeeper->getChildren(fs::path(zookeeper_path) / "parts_stale");
        std::unordered_map<String, Int64> global_stale_parts_epochs;
        global_stale_parts_epochs.reserve(global_stale_parts.size());
        for (const auto & name : global_stale_parts)
        {
            Coordination::Stat stat;
            if (!zookeeper->exists(fs::path(zookeeper_path) / "parts_stale" / name, &stat))
                continue;

            global_stale_parts_epochs[name] = stat.czxid;
        }

        // Find existing drop ranges with their epochs
        auto global_parts = zookeeper->getChildren(fs::path(zookeeper_path) / "parts");
        std::unordered_map<String, Int64> global_drop_ranges_epochs;
        for (const auto & name : global_parts)
        {
            Coordination::Stat stat;
            if (!zookeeper->exists(fs::path(zookeeper_path) / "parts" / name, &stat))
                continue;

            if (!MergeTreePartInfo::tryParsePartName(name, format_version)->isFakeDropRangePart())
                continue;

            global_drop_ranges_epochs[name] = stat.czxid;
        }

        // Discover minimal active epoch across the replicas
        auto replica_names = zookeeper->getChildren(fs::path(zookeeper_path) / "replicas");
        Int64 min_active_epoch = std::numeric_limits<Int64>::max();
        for (const auto & replica : replica_names)
        {
            String epoch_str;
            if (!zookeeper->tryGet(fs::path(zookeeper_path) / "replicas" / replica / "epoch", epoch_str))
                continue;
            if (epoch_str.empty())
                continue;
            Int64 epoch = parseFromString<Int64>(epoch_str);
            min_active_epoch = std::min(epoch, min_active_epoch);
        }

        if (min_active_epoch == std::numeric_limits<Int64>::max())
        {
            cleanup_task->scheduleAfter(cleanup_delay);
            return;
        }

        // Identify which parts could be deleted, and which must be kept based on min active epoch
        std::unordered_set<String> global_stale_parts_to_delete;
        std::unordered_set<String> global_stale_parts_to_keep;
        for (const auto & [name, epoch] : global_stale_parts_epochs)
        {
            if (epoch <= min_active_epoch)
                global_stale_parts_to_delete.insert(name);
            else
                global_stale_parts_to_keep.insert(name);
        }

        // For parts allowed to be deleted, just delete them immediately
        for (const auto & part_name : global_stale_parts_to_delete)
        {
            const auto disk = getDisks().front();
            disk->removeRecursive(fs::path(relative_data_path) / part_name);
        }

        Coordination::Requests remove_stale_parts_ops;
        for (const auto & part : global_stale_parts_to_delete)
            remove_stale_parts_ops.emplace_back(zkutil::makeRemoveRequest(
                fs::path(zookeeper_path) / "parts_stale" / part, -1));

        Coordination::Responses remove_stale_parts_responses;
        auto remove_code = zookeeper->tryMulti(remove_stale_parts_ops, remove_stale_parts_responses);
        if (remove_code != Coordination::Error::ZOK)
        {
            LOG_WARNING(log, "Failed to remove some /parts_stale entries: {}",
                Coordination::errorMessage(remove_code));
        }

        // Identify which drop ranges could be deleted based on min active epoch
        std::unordered_set<String> global_drop_ranges_to_delete;
        for (const auto & [name, epoch] : global_drop_ranges_epochs)
        {
            if (epoch <= min_active_epoch)
                global_drop_ranges_to_delete.insert(name);
        }

        Coordination::Requests remove_drop_ranges_ops;
        for (const auto & part : global_drop_ranges_to_delete)
            remove_drop_ranges_ops.emplace_back(zkutil::makeRemoveRequest(
                fs::path(zookeeper_path) / "parts" / part, -1));

        Coordination::Responses remove_drop_ranges_responses;
        auto remove_drop_ranges_code = zookeeper->tryMulti(remove_drop_ranges_ops, remove_drop_ranges_responses);
        if (remove_drop_ranges_code != Coordination::Error::ZOK)
        {
            LOG_WARNING(log, "Failed to remove some drop ranges /parts entries: {}",
                Coordination::errorMessage(remove_drop_ranges_code));
        }

        // Cleanup local in-memory parts if some, unless it's forbidden by epoch based cleanup
        grabOldParts(true);

        DataPartsVector to_delete;
        {
            auto part_lock = lockParts();
            auto deleting_range = getDataPartsStateRange(DataPartState::Deleting);
            for (const auto & it : deleting_range)
            {
                if (global_stale_parts_to_keep.contains(it->name))
                    continue;

                to_delete.push_back(it);
            }
        }

        if (!to_delete.empty())
            clearPartsFromFilesystemAndRollbackIfError(to_delete, "old");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to cleanup stale parts");
    }

    cleanup_task->scheduleAfter(cleanup_delay);
}


void StorageAmateurMergeTree::cleanupStaleSnapshotGuards() const
{
    std::lock_guard lock(snapshot_guard_cache_mutex);
    for (auto it = snapshot_guard_cache.begin(); it != snapshot_guard_cache.end(); )
    {
        if (!it->first.lock())
            it = snapshot_guard_cache.erase(it);
        else
            ++it;
    }
}


StorageSnapshotPtr StorageAmateurMergeTree::getStorageSnapshot(
    const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    // Capture current epoch before acquiring the storage snapshot. This epoch
    // may lag behind the actual snapshot state, which is acceptable, as its
    // main goal is to prevent cleanup of stale parts being used by the snapshot.
    Int64 epoch = current_epoch.load(std::memory_order_acquire);

    auto snapshot = MergeTreeData::getStorageSnapshot(metadata_snapshot, query_context);

    // Reuse an existing guard for this parent snapshot if one is still alive,
    // otherwise create a new one. The guard is cached in snapshot_guard_cache
    // and released when the parent snapshot is destroyed (weak_ptr expires).
    std::shared_ptr<void> guard;
    {
        std::lock_guard lock(snapshot_guard_cache_mutex);
        for (const auto & [weak_snap, cached_guard] : snapshot_guard_cache)
        {
            auto locked = weak_snap.lock();
            if (locked && locked.get() == snapshot.get())
            {
                guard = cached_guard;
                break;
            }
        }

        if (!guard)
        {
            {
                std::lock_guard epoch_lock(active_snapshot_epochs_mutex);
                active_snapshot_epochs.insert(epoch);
            }

            guard = std::shared_ptr<Int64>(
                new Int64(epoch),
                [epoch, this](const Int64 * p)
                {
                    std::lock_guard epoch_lock(active_snapshot_epochs_mutex);
                    active_snapshot_epochs.erase(active_snapshot_epochs.find(epoch));
                    delete p;
                });

            snapshot_guard_cache.emplace_back(snapshot, guard);
        }
    }

    return snapshot;
}


void StorageAmateurMergeTree::startup()
{
    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::startup");
    LOG_INFO(log, "Starting up StorageAmateurMergeTree for {}", getStorageID().getNameForLogs());

    parts_syncing_task->activateAndSchedule();
    cleanup_task->activateAndSchedule();

    background_operations_assignee.start();

    LOG_INFO(log, "Startup completed for {}", getStorageID().getNameForLogs());

    bool old_val = true;
    is_readonly.compare_exchange_strong(old_val, false);
}


void StorageAmateurMergeTree::flushAndPrepareForShutdown()
{
    LOG_TRACE(log, "Flush and prepare for shutdown");
}


void StorageAmateurMergeTree::partialShutdown()
{
    LOG_TRACE(log, "Partial shutdown");
}


void StorageAmateurMergeTree::shutdown(bool is_drop)
{
    LOG_TRACE(log, "Shutdown, is_drop={}", is_drop);
    shutdown_called.store(true, std::memory_order_release);
    cleanup_task->deactivate();
}


std::string StorageAmateurMergeTree::getName() const
{
    return "Amateur" + merging_params.getModeName() + "MergeTree";
}


std::unique_ptr<MergeTreeSettings> StorageAmateurMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}


bool StorageAmateurMergeTree::canUseAdaptiveGranularity() const
{
    return MergeTreeData::canUseAdaptiveGranularity();
}


void StorageAmateurMergeTree::assertNotReadonly() const
{
    if (is_readonly)
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode");
}


void StorageAmateurMergeTree::assertNotStaticStorage() const
{
    if (isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table uses static storage");
}


bool StorageAmateurMergeTree::createTableIfNotExists(
    const StorageMetadataPtr & metadata_snapshot, const ZooKeeperRetriesInfo & zookeeper_retries_info)
{
    bool table_created = false;
    if (zookeeper_retries_info.max_retries > 0)
    {
        ZooKeeperRetriesControl retries_ctl{"StirageAmateurMergeTree::createTableIfNotExists", log.load(), zookeeper_retries_info};
        retries_ctl.retryLoop([&]
        {
            /// Refresh current_zookeeper on retry since it's not auto-updated during creation (RestartingThread not yet running).
            if (retries_ctl.isRetry())
                setZooKeeper();
            table_created = createTableIfNotExistsAttempt(metadata_snapshot, zookeeper_retries_info.query_status);
        });
    }
    else
    {
        table_created = createTableIfNotExistsAttempt(metadata_snapshot, zookeeper_retries_info.query_status);
    }
    return table_created;
}


bool StorageAmateurMergeTree::createTableIfNotExistsAttempt(
    const StorageMetadataPtr & metadata_snapshot, QueryStatusPtr process_list_element) const
{
    auto zookeeper = getZooKeeper();
    zookeeper->createAncestors(zookeeper_path);

    for (size_t i = 0; i < 1000; ++i)
    {
        /// Check if the query was cancelled.
        if (process_list_element)
            process_list_element->checkTimeLimit();

        /// Invariant: "replicas" does not exist if there is no table or if there are leftovers from incompletely dropped table.
        if (zookeeper->exists(zookeeper_path + "/replicas"))
        {
            LOG_DEBUG(log, "This table {} is already created, will add new replica", zookeeper_path);
            return false;
        }

        /// There are leftovers from incompletely dropped table.
        if (zookeeper->exists(zookeeper_path + "/dropped"))
        {
            /// This condition may happen when the previous drop attempt was not completed
            ///  or when table is dropped by another replica right now.
            /// This is Ok because another replica is definitely going to drop the table.

            LOG_WARNING(log, "Removing leftovers from table {} (this might take several minutes)", zookeeper_path);
            String drop_lock_path = zookeeper_path + "/dropped/lock";
            Coordination::Error code = zookeeper->tryCreate(drop_lock_path, "", zkutil::CreateMode::Ephemeral);

            if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_WARNING(log, "The leftovers from table {} were removed by another replica", zookeeper_path);
            }
            else if (code != Coordination::Error::ZOK)
            {
                throw Coordination::Exception::fromPath(code, drop_lock_path);
            }
            else
            {
                auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *zookeeper);
                if (!removeTableNodesFromZooKeeper(zookeeper, zookeeper_info, metadata_drop_lock, log.load()))
                {
                    /// Someone is recursively removing table right now, we cannot create new table until old one is removed
                    continue;
                }
            }
        }

        LOG_DEBUG(log, "Creating table {}", zookeeper_path);

        /// We write metadata of table so that the replicas can check table parameters with them.
        String metadata_str = ReplicatedMergeTreeTableMetadata(*this, metadata_snapshot).toString();

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/metadata", metadata_str,
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/columns", metadata_snapshot->getColumns().toString(true),
            zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/parts", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/parts_stale", "",
            zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/blocks", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/async_blocks", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/deduplication_hashes", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/block_numbers", "",
            zkutil::CreateMode::Persistent));

        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name,
            zkutil::CreateMode::Persistent));

        /// And create first replica atomically. See also "createReplica" method that is used to create not the first replicas.

        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/epoch", "0",
            zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "It looks like the table {} was created by another server at the same moment, will retry", zookeeper_path);
            continue;
        }
        if (code != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(code, ops, responses);
        }

        return true;
    }

    /// Do not use LOGICAL_ERROR code, because it may happen if user has specified wrong zookeeper_path
    throw Exception(ErrorCodes::REPLICA_ALREADY_EXISTS,
                    "Cannot create table, because it is created concurrently every time or because "
                    "of wrong zookeeper_path or because of logical error");
}


void StorageAmateurMergeTree::createReplica(
    const StorageMetadataPtr & metadata_snapshot, const ZooKeeperRetriesInfo & zookeeper_retries_info)
{
    if (zookeeper_retries_info.max_retries > 0)
    {
        ZooKeeperRetriesControl retries_ctl{"StorageAmateurMergeTree::createReplica", log.load(), zookeeper_retries_info};
        retries_ctl.retryLoop([&]
        {
            /// Refresh current_zookeeper on retry since it's not auto-updated during creation (RestartingThread not yet running).
            if (retries_ctl.isRetry())
                setZooKeeper();
            createReplicaAttempt(metadata_snapshot, zookeeper_retries_info.query_status);
        });
    }
    else
    {
        createReplicaAttempt(metadata_snapshot, zookeeper_retries_info.query_status);
    }
}


void StorageAmateurMergeTree::createReplicaAttempt(
    const StorageMetadataPtr &, QueryStatusPtr process_list_element) const
{
    auto zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Creating replica {}", replica_path);

    /// It is possible for the replica to fail after creating ZK nodes without saving local metadata.
    /// Because of that we need to check whether the replica exists and is newly created.
    /// For this we check that all nodes exist, the metadata of the table is the same, and other nodes are not modified.

    std::vector<String> paths_exists = {
        replica_path + "/host"
    };

    auto response_exists = zookeeper->tryGet(paths_exists);
    bool all_nodes_exist = true;

    for (size_t i = 0; i < response_exists.size(); ++i)
    {
        if (response_exists[i].error != Coordination::Error::ZOK)
        {
            all_nodes_exist = false;
            break;
        }
    }

    if (all_nodes_exist)
    {
        size_t response_num = 0;

        const auto & zk_host = response_exists[response_num++].data;

        if (zk_host.empty())
        {
            LOG_DEBUG(log, "Empty replica {} exists, will use it", replica_path);
            return;
        }
    }

    Coordination::Error code{};

    do
    {
        /// Check if the query was cancelled.
        if (process_list_element)
            process_list_element->checkTimeLimit();

        Coordination::Stat replicas_stat;
        String replicas_value;

        if (!zookeeper->tryGet(zookeeper_path + "/replicas", replicas_value, &replicas_stat))
            throw Exception(ErrorCodes::ALL_REPLICAS_LOST,
                "Cannot create a replica of the table {}, because the last replica of the table was dropped right now",
                zookeeper_path);

        /// It is not the first replica, we will mark it as "lost", to immediately repair (clone) from existing replica.
        /// By the way, it's possible that the replica will be first, if all previous replicas were removed concurrently.
        // const String is_lost_value = replicas_stat.numChildren ? "1" : "0";

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(replica_path, "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "",
            zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/epoch", "0",
            zkutil::CreateMode::Persistent));

        /// Check version of /replicas to see if there are any replicas created at the same moment of time.
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/replicas", "last added replica: " + replica_name, replicas_stat.version));

        Coordination::Responses responses;
        code = zookeeper->tryMulti(ops, responses);

        switch (code)
        {
            case Coordination::Error::ZNODEEXISTS:
                throw Exception(ErrorCodes::REPLICA_ALREADY_EXISTS, "Replica {} already exists", replica_path);
            case Coordination::Error::ZBADVERSION:
                LOG_INFO(log, "Retrying createReplica(), because some other replicas were created at the same time");
                break;
            case Coordination::Error::ZNONODE:
                throw Exception(ErrorCodes::ALL_REPLICAS_LOST, "Table {} was suddenly removed", zookeeper_path);
            default:
                zkutil::KeeperMultiException::check(code, ops, responses);
        }
    } while (code == Coordination::Error::ZBADVERSION);
}


bool StorageAmateurMergeTree::checkTableStructure(
    const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot,
    int32_t * metadata_version, bool strict_check,
    const ZooKeeperRetriesInfo & zookeeper_retries_info)
{
    bool same_structure = false;
    if (zookeeper_retries_info.max_retries > 0)
    {
        ZooKeeperRetriesControl retries_ctl{"StorageAmateurMergeTree::checkTableStructure", log.load(), zookeeper_retries_info};
        retries_ctl.retryLoop([&]
        {
            /// Refresh current_zookeeper on retry since it's not auto-updated during creation (RestartingThread not yet running).
            if (retries_ctl.isRetry())
                setZooKeeper();
            same_structure = checkTableStructureAttempt(zookeeper_prefix, metadata_snapshot, metadata_version, strict_check);
        });
    }
    else
    {
        same_structure = checkTableStructureAttempt(zookeeper_prefix, metadata_snapshot, metadata_version, strict_check);
    }
    return same_structure;
}


bool StorageAmateurMergeTree::checkTableStructureAttempt(
    const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot,
    int32_t * metadata_version, bool strict_check) const
{
    auto zookeeper = getZooKeeper();

    ReplicatedMergeTreeTableMetadata old_metadata(*this, metadata_snapshot);

    Coordination::Stat metadata_stat;
    String metadata_str = zookeeper->get(fs::path(zookeeper_prefix) / "metadata", &metadata_stat);
    auto metadata_from_zk = ReplicatedMergeTreeTableMetadata::parseAndNormalize(
        metadata_str, metadata_snapshot->getColumns(),
        metadata_snapshot->add_minmax_index_for_numeric_columns,
        metadata_snapshot->add_minmax_index_for_string_columns,
        getContext());
    bool is_metadata_equal = old_metadata.checkEquals(metadata_from_zk, metadata_snapshot->columns, metadata_snapshot->virtuals, getStorageID().getNameForLogs(), getContext(), /*check_index_granularity*/ true, strict_check, log.load());

    if (metadata_version)
        *metadata_version = metadata_stat.version;

    Coordination::Stat columns_stat;
    auto columns_from_zk = ColumnsDescription::parse(zookeeper->get(fs::path(zookeeper_prefix) / "columns", &columns_stat));

    const ColumnsDescription & old_columns = metadata_snapshot->getColumns();
    if (columns_from_zk == old_columns && is_metadata_equal)
        return true;

    if (!strict_check && metadata_stat.version != 0)
    {
        LOG_WARNING(log, "Table columns structure in ZooKeeper is different from local table structure. "
                    "Assuming it's because the table was altered concurrently. Metadata version: {}. Local columns:\n"
                    "{}\nZookeeper columns:\n{}", metadata_stat.version, old_columns.toString(true), columns_from_zk.toString(true));
        return false;
    }

    throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS,
        "Table columns structure in ZooKeeper is different from local table structure. Local columns:\n"
        "{}\nZookeeper columns:\n{}", old_columns.toString(true), columns_from_zk.toString(true));
}


ZooKeeperRetriesInfo StorageAmateurMergeTree::getCreateQueryZooKeeperRetriesInfo() const
{
    std::lock_guard lock(create_query_zookeeper_retries_info_mutex);
    return create_query_zookeeper_retries_info;
}


void StorageAmateurMergeTree::clearCreateQueryZooKeeperRetriesInfo()
{
    std::lock_guard lock(create_query_zookeeper_retries_info_mutex);
    create_query_zookeeper_retries_info = {};
}


void StorageAmateurMergeTree::startBackgroundMovesIfNeeded()
{
}

String StorageAmateurMergeTree::getSharedDataPath() const
{
    auto zookeeper = getZooKeeper();

    String zookeeper_data_path_znode = fs::path(zookeeper_path) / "data_path";

    String data_path;
    // TODO: better error handling here
    if (zookeeper->tryGet(zookeeper_data_path_znode, data_path))
        return data_path;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get shared data path from Keeper");
}

void StorageAmateurMergeTree::createSharedDataPath(const String & data_path, const ZooKeeperRetriesInfo & zookeeper_retries_info)
{
    if (zookeeper_retries_info.max_retries > 0)
    {
        ZooKeeperRetriesControl retries_ctl{"StorageAmateurMergeTree::createTableDataPath", log.load(), zookeeper_retries_info};
        retries_ctl.retryLoop([&]
        {
            /// Refresh current_zookeeper on retry since it's not auto-updated during creation (RestartingThread not yet running).
            if (retries_ctl.isRetry())
                setZooKeeper();
            createSharedDataPathAttempt(data_path);
        });
    }
    else
    {
        createSharedDataPathAttempt(data_path);
    }
}

void StorageAmateurMergeTree::createSharedDataPathAttempt(const String & data_path) const
{
    LOG_DEBUG(log, "Creating shared data path for table {}", getStorageID().getNameForLogs());
    // can be set by the call to getTableSharedID
    if (!relative_data_path.empty())
    {
        LOG_INFO(log, "Shared data path already set to {}", relative_data_path);
        return;
    }

    /// We may call getTableSharedID when table is shut down. If exception happen, restarting thread will be already turned
    /// off and nobody will reconnect our zookeeper connection. In this case we use zookeeper connection from
    /// context.
    ZooKeeperPtr zookeeper;
    if (shutdown_called.load())
        zookeeper = getZooKeeperIfTableShutDown();
    else
        zookeeper = getZooKeeper();

    String zookeeper_table_id_path = fs::path(zookeeper_path) / "data_path";
    String data_path_candidate;
    if (!zookeeper->tryGet(zookeeper_table_id_path, data_path_candidate))
    {
        LOG_DEBUG(log, "Shared data path for table {} doesn't exist in ZooKeeper on path {}", getStorageID().getNameForLogs(), zookeeper_table_id_path);

        auto code = zookeeper->tryCreate(zookeeper_table_id_path, data_path, zkutil::CreateMode::Persistent);
        if (code == Coordination::Error::ZNODEEXISTS)
            LOG_DEBUG(log, "Shared data path on path {} concurrently created", zookeeper_table_id_path);
        else if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException::fromPath(code, zookeeper_table_id_path);
    }
}

std::vector<String> StorageAmateurMergeTree::getZookeeperZeroCopyLockPaths() const
{
    return {};
}

void StorageAmateurMergeTree::dropZookeeperZeroCopyLockPaths(
    zkutil::ZooKeeperPtr /*zookeeper*/, std::vector<String> /*zero_copy_locks_paths*/, LoggerPtr /*logger*/)
{
}


void StorageAmateurMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum,
    const size_t max_block_size,
    const size_t num_streams)
{
    const bool enable_parallel_reading = local_context->canUseParallelReplicasOnFollower();
    auto plan = MergeTreeDataSelectExecutor(*this).read(
        column_names,
        storage_snapshot,
        query_info,
        local_context,
        max_block_size,
        num_streams,
        local_context->getPartitionIdToMaxBlock(getStorageID().uuid),
        enable_parallel_reading);

    if (plan)
        query_plan = std::move(*plan);
}

template <class Func>
void StorageAmateurMergeTree::foreachActiveParts(Func && func) const
{
    std::optional<PartitionIdToMaxBlock> max_added_blocks = {};

    auto lock = readLockParts();

    for (const auto & part : getDataPartsStateRange(DataPartState::Active, MergeTreePartInfo::Kind::Regular))
    {
        if (part->isEmpty())
            continue;

        func(part);
    }
}

std::optional<UInt64> StorageAmateurMergeTree::totalRows(ContextPtr) const
{
    UInt64 res = 0;
    foreachActiveParts([&res](auto & part) { res += part->rows_count; });
    return res;
}

std::optional<UInt64> StorageAmateurMergeTree::totalRowsByPartitionPredicate(const ActionsDAG &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "totalRowsByPartitionPredicate is not implemented for StorageAmateurMergeTree");
}

std::optional<UInt64> StorageAmateurMergeTree::totalBytes(ContextPtr) const
{
    UInt64 res = 0;
    foreachActiveParts([&res](auto & part) { res += part->getBytesOnDisk(); });
    return res;
}

std::optional<UInt64> StorageAmateurMergeTree::totalBytesUncompressed(const Settings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "totalBytesUncompressed is not implemented for StorageAmateurMergeTree");
}

MutationCounters StorageAmateurMergeTree::getMutationCounters() const
{
    return MutationCounters{0, 0, 0};
}

EphemeralLockInZooKeeper StorageAmateurMergeTree::allocateBlockNumber(
    const String & partition_id,
    const zkutil::ZooKeeperPtr & zookeeper,
    const std::vector<std::string> & zookeeper_block_id_paths,
    const String & zookeeper_path_prefix,
    const std::optional<String> & znode_data) const
{
    return allocateBlockNumber(
        partition_id, std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), zookeeper_block_id_paths, zookeeper_path_prefix, znode_data);
}

EphemeralLockInZooKeeper StorageAmateurMergeTree::allocateBlockNumber(
    const String & partition_id,
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    const std::vector<std::string> & zookeeper_block_id_paths,
    const String & zookeeper_path_prefix,
    const std::optional<String> & znode_data) const
{
    String zookeeper_table_path;
    if (zookeeper_path_prefix.empty())
        zookeeper_table_path = zookeeper_path;
    else
        zookeeper_table_path = zookeeper_path_prefix;

    String block_numbers_path = fs::path(zookeeper_table_path) / "block_numbers";
    String partition_path = fs::path(block_numbers_path) / partition_id;

    if (!existsNodeCached(zookeeper, partition_path))
    {
        Coordination::Requests ops;
        /// Check that table is not being dropped ("host" is the first node that is removed on replica drop)
        ops.push_back(zkutil::makeCheckRequest(fs::path(replica_path) / "host", -1));
        ops.push_back(zkutil::makeCreateRequest(partition_path, "", zkutil::CreateMode::Persistent));
        /// We increment data version of the block_numbers node so that it becomes possible
        /// to check in a ZK transaction that the set of partitions didn't change
        /// (unfortunately there is no CheckChildren op).
        ops.push_back(zkutil::makeSetRequest(block_numbers_path, "", -1));

        Coordination::Responses responses;
        Coordination::Error code = zookeeper->tryMulti(ops, responses);
        if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
            zkutil::KeeperMultiException::check(code, ops, responses);
    }

    LOG_TRACE(log, "Allocating block number at {}", fs::path(partition_path) / "block-");

    auto lock = createEphemeralLockInZooKeeper(
        fs::path(partition_path) / "block-", fs::path(zookeeper_table_path) / "temp", zookeeper, zookeeper_block_id_paths, znode_data);

    if (lock.isLocked())
        LOG_TRACE(log, "Allocated block number {} in partition {}", lock.getNumber(), partition_id);

    return lock;
}

bool StorageAmateurMergeTree::existsNodeCached(const ZooKeeperWithFaultInjectionPtr & zookeeper, const std::string & path) const
{
    {
        std::lock_guard lock(existing_nodes_cache_mutex);
        if (existing_nodes_cache.contains(path))
            return true;
    }

    bool res = zookeeper->exists(path);

    if (res)
    {
        std::lock_guard lock(existing_nodes_cache_mutex);
        existing_nodes_cache.insert(path);
    }

    return res;
}

void StorageAmateurMergeTree::getCommitPartOps(
    Coordination::Requests & ops,
    const DataPartPtr & part,
    const String & block_id_path) const
{
    if (block_id_path.empty())
        getCommitPartOps(ops, part, std::vector<String>());
    else
        getCommitPartOps(ops, part, std::vector<String>({block_id_path}));
}

void StorageAmateurMergeTree::getCommitPartOps(
    Coordination::Requests & ops,
    const DataPartPtr & part,
    const std::vector<String> & block_id_paths) const
{
    const String & part_name = part->name;
    const auto storage_settings_ptr = getSettings();
    for (const String & block_id_path : block_id_paths)
    {
        /// Make final duplicate check and commit block_id
        ops.emplace_back(
            zkutil::makeCreateRequest(
                block_id_path,
                part_name,  /// We will be able to know original part number for duplicate blocks, if we want.
                zkutil::CreateMode::Persistent));
    }

    ops.emplace_back(zkutil::makeCreateRequest(
        fs::path(zookeeper_path) / "parts" / part->name,
        "",
        zkutil::CreateMode::Persistent));
}

SinkToStoragePtr StorageAmateurMergeTree::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool async_insert)
{
    /// We need to check it explicitly since someone may write to table explicitly (bypassing InterpreterInsertQuery, which has this check)
    if (local_context->getCurrentTransaction())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} (table {}) does not support transactions", getName(), getStorageID().getNameForLogs());

    if (!initialization_done)
        throw Exception(ErrorCodes::NOT_INITIALIZED, "Table is not initialized yet");

    if (isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode due to static storage");
    /// If table is read-only because it doesn't have metadata in zk yet, then it's not possible to insert into it
    /// Without this check, we'll write data parts on disk, and afterwards will remove them since we'll fail to commit them into zk
    /// In case of remote storage like s3, it'll generate unnecessary PUT requests
    if (is_readonly && (!has_metadata_in_zookeeper.has_value() || false == has_metadata_in_zookeeper.value()))
        throw Exception(
            ErrorCodes::TABLE_IS_READ_ONLY,
            "Table is in readonly mode since table metadata was not found in zookeeper: replica_path={}",
            replica_path);

    const auto storage_settings_ptr = getSettings();
    const auto & settings = local_context->getSettingsRef();

    return std::make_shared<AmateurMergeTreeSink>(async_insert, *this, metadata_snapshot, settings[Setting::max_partitions_per_insert_block], local_context);
}

bool StorageAmateurMergeTree::mergeParts(
    const StorageMetadataPtr & metadata_snapshot,
    const FutureMergedMutatedPartPtr & future_part,
    ReservationSharedPtr reserved_space,
    TableLockHolder & table_lock_holder)
{
    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::mergeParts");

    ProfileEvents::Counters profile_counters;
    ProfileEventsScope profile_events_scope(&profile_counters);

    auto query_id = getStorageID().getShortName() + "::" + future_part->name;

    auto stopwatch = Stopwatch();
    auto task_context = Context::createCopy(getContext()->getBackgroundContext());
    task_context->makeQueryContextForMerge(*getSettings());
    task_context->setCurrentQueryId(query_id);

    auto merge_list_entry = getContext()->getMergeList().insert(
        getStorageID(), future_part, task_context);

    std::optional<ThreadGroupSwitcher> switcher;
    if (merge_list_entry)
        switcher.emplace((*merge_list_entry)->thread_group, ThreadName::MERGE_MUTATE, true);

    writePartLog(
        PartLogElement::MERGE_PARTS_START, {}, 0,
        future_part->name, nullptr, future_part->parts, merge_list_entry.get(), {}, {}, {});

    auto log = getLogger("StorageAmateurMergeTree");

    MergeTaskPtr merge_task;
    MergeTreeData::MutableDataPartPtr new_part;

    try
    {
        merge_task = merger_mutator.mergePartsToTemporaryPart(
            future_part, metadata_snapshot, merge_list_entry.get(),
            {}, table_lock_holder, time(nullptr), task_context,
            reserved_space, false, {}, false,
            merging_params, nullptr);

        while (merge_task->execute()) {}

        new_part = merge_task->getFuture().get();

        auto zookeeper = std::make_shared<ZooKeeperWithFaultInjection>(getZooKeeper());
        Coordination::Requests ops;
        getCommitPartOps(ops, new_part);

        for (const auto & src : future_part->parts)
        {
            ops.emplace_back(zkutil::makeRemoveRequest(
                zookeeper_path + "/parts/" + src->name, -1));
            ops.emplace_back(zkutil::makeCreateRequest(
                zookeeper_path + "/parts_stale/" + src->name, "", zkutil::CreateMode::Persistent));
        }

        new_part->renameTo(new_part->name, false);

        fiu_do_on(FailPoints::amateur_merge_commit_zk_fail_before_op, { zookeeper->forceFailureBeforeOperation(); });
        fiu_do_on(FailPoints::amateur_merge_commit_zk_fail_after_op, { zookeeper->forceFailureAfterOperation(); });

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);
        if (code != Coordination::Error::ZOK)
        {
            LOG_DEBUG(log, "Failed to commit {}: {}", new_part->name, code);
            throw zkutil::KeeperMultiException(code, ops, responses);
        }

        auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(
            profile_counters.getPartiallyAtomicSnapshot());
        auto projections_duration_ms = merge_task->grabProjectionsMergeTime();
        writePartLog(
            PartLogElement::MERGE_PARTS, {}, stopwatch.elapsed(),
            future_part->name, new_part, future_part->parts, merge_list_entry.get(),
            counters_snapshot, {}, projections_duration_ms);

        ProfileEvents::increment(ProfileEvents::MergedIntoWideParts);

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Exception in merge.");

        auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(
            profile_counters.getPartiallyAtomicSnapshot());
        auto projections_duration_ms = merge_task ? merge_task->grabProjectionsMergeTime()
                                                   : std::map<String, UInt64>{};
        writePartLog(
            PartLogElement::MERGE_PARTS,
            ExecutionStatus::fromCurrentException("", true),
            stopwatch.elapsed(), future_part->name,
            new_part, future_part->parts, merge_list_entry.get(),
            counters_snapshot, {}, projections_duration_ms);
        throw;
    }
}

bool StorageAmateurMergeTree::optimize(
    const ASTPtr &, const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & partition, bool final, bool, const Names &, bool, ContextPtr)
{
    if (shutdown_called)
        return false;

    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::scheduleDataProcessingJob");

    auto settings_ptr = getSettings();

    auto shared_lock = lockForShare(
        RWLockImpl::NO_QUERY,
        (*settings_ptr)[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);

    std::unique_lock lock(currently_processing_in_background_mutex);

    auto zookeeper = getZooKeeperAndAssertNotReadonly();
    String partition_id;
    if (partition)
        partition_id = getPartitionIDFromQuery(partition, getContext());

    auto merge_pred = std::make_shared<AmateurMergePredicate>(*this, zookeeper, zookeeper_path);
    auto parts_collector = std::make_shared<AmateurMergeTreePartsCollector>(*this, merge_pred);

    FutureMergedMutatedPartPtr future_part;
    if (!partition_id.empty())
    {
        auto select_result = merger_mutator.selectAllPartsToMergeWithinPartition(
            metadata_snapshot, parts_collector, merge_pred,
            partition_id, final, false);

        if (!select_result.has_value())
            return false;

        chassert(select_result.value().size() == 1);
        future_part = constructFuturePart(*this, select_result.value()[0], {MergeTreeDataPartState::Active});
    }
    else
    {
        UInt64 max_source_parts_bytes = CompactionStatistics::getMaxSourcePartsBytesForMerge(*this);
        if (max_source_parts_bytes == 0)
            return false;

        UInt64 max_result_part_rows = CompactionStatistics::getMaxResultPartRowsCount(*this);

        auto select_result = merger_mutator.selectPartsToMerge(
            parts_collector, merge_pred,
            MergeSelectorApplier(
                {{max_source_parts_bytes, max_result_part_rows}},
                true,
                true,
                nullptr,
                getStorageID()),
            std::nullopt);

        if (!select_result.has_value())
            return false;

        chassert(select_result.value().size() == 1);
        future_part = constructFuturePart(*this, select_result.value()[0], {MergeTreeDataPartState::Active});
    }

    if (!future_part)
        return false;

    size_t total_size = CompactionStatistics::estimateNeededDiskSpace(future_part->parts, true);
    auto reserved_space = tryReserveSpacePreferringTTLRules(
        metadata_snapshot, total_size, {}, time(nullptr));
    if (!reserved_space)
        return false;

    future_part->updatePath(*this, reserved_space.get());

    currently_merging_mutating_parts.insert(future_part->parts.begin(), future_part->parts.end());

    SCOPE_EXIT({
        for (const auto & part : future_part->parts)
            currently_merging_mutating_parts.erase(part);
    });

    return mergeParts(metadata_snapshot, future_part, std::move(reserved_space), shared_lock);
}

void StorageAmateurMergeTree::alter(const AlterCommands &, ContextPtr, AlterLockHolder &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "alter is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::mutate(const MutationCommands &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "mutate is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::waitMutation(const String &, size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "waitMutation is not implemented for StorageAmateurMergeTree");
}

std::vector<MergeTreeMutationStatus> StorageAmateurMergeTree::getMutationsStatus() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getMutationsStatus is not implemented for StorageAmateurMergeTree");
}

CancellationCode StorageAmateurMergeTree::killMutation(const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "killMutation is not implemented for StorageAmateurMergeTree");
}

QueryPipeline StorageAmateurMergeTree::updateLightweight(const MutationCommands &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "updateLightweight is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::haveCommittingOps(const CommittingBlocks &, PartitionIdToMaxBlockPtr, std::set<CommittingBlock::Op>) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "haveCommittingOps is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::waitForCommittingOpsToFinish(zkutil::ZooKeeperPtr, PartitionIdToMaxBlockPtr, std::set<CommittingBlock::Op>, size_t, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "waitForCommittingOpsToFinish is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::drop()
{
    /// There is also the case when user has configured ClickHouse to wrong ZooKeeper cluster
    /// or metadata of staled replica were removed manually,
    /// in this case, has_metadata_in_zookeeper = false, and we also permit to drop the table.

    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::drop");
    bool maybe_has_metadata_in_zookeeper = !has_metadata_in_zookeeper.has_value() || *has_metadata_in_zookeeper;
    zkutil::ZooKeeperPtr zookeeper;
    if (maybe_has_metadata_in_zookeeper)
    {
        /// Table can be shut down, restarting thread is not active
        /// and calling StorageAmateurMergeTree::getZooKeeper()/getAuxiliaryZooKeeper() won't suffice.
        zookeeper = getZooKeeperIfTableShutDown();
        /// Update zookeeper client, since existing may be expired, while ZooKeeper is required inside dropAllData().
        {
            std::lock_guard lock(current_zookeeper_mutex);
            current_zookeeper = zookeeper;
        }

        /// If probably there is metadata in ZooKeeper, we don't allow to drop the table.
        if (!zookeeper)
            throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Can't drop readonly table (need to drop data in ZooKeeper as well)");
    }


    /// getZookeeperZeroCopyLockPaths has to be called before dropAllData
    /// otherwise table_shared_id is unknown
    auto zero_copy_locks_paths = getZookeeperZeroCopyLockPaths();

    if (maybe_has_metadata_in_zookeeper)
    {
        /// Session could expire, get it again
        zookeeper = getZooKeeperIfTableShutDown();

        auto lost_part_count_path = fs::path(zookeeper_path) / "lost_part_count";
        Coordination::Stat lost_part_count_stat;
        String lost_part_count_str;
        if (zookeeper->tryGet(lost_part_count_path, lost_part_count_str, &lost_part_count_stat))
        {
            UInt64 lost_part_count = lost_part_count_str.empty() ? 0 : parse<UInt64>(lost_part_count_str);
            if (lost_part_count > 0)
                LOG_INFO(log, "Dropping table with non-zero lost_part_count equal to {}", lost_part_count);
        }

        bool last_replica_dropped = dropReplica(zookeeper, zookeeper_info, log.load(), getSettings(), &has_metadata_in_zookeeper);
        if (last_replica_dropped)
            dropAllData();
    }
}

void StorageAmateurMergeTree::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "truncate is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::checkTableCanBeRenamed(const StorageID &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "checkTableCanBeRenamed is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::rename(const String &, const StorageID &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "rename is not implemented for StorageAmateurMergeTree");
}

ActionLock StorageAmateurMergeTree::getActionLock(StorageActionBlockType)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getActionLock is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::onActionLockRemove(StorageActionBlockType)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "onActionLockRemove is not implemented for StorageAmateurMergeTree");
}



IStorage::DataValidationTasksPtr StorageAmateurMergeTree::getCheckTaskList(const CheckTaskFilter &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getCheckTaskList is not implemented for StorageAmateurMergeTree");
}

std::optional<CheckResult> StorageAmateurMergeTree::checkDataNext(DataValidationTasksPtr &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "checkDataNext is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::applyMetadataChangesToCreateQueryForBackup(const ASTPtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "applyMetadataChangesToCreateQueryForBackup is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::backupData(BackupEntriesCollector &, const String &, const std::optional<ASTs> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "backupData is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::restoreDataFromBackup(RestorerFromBackup &, const String &, const std::optional<ASTs> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "restoreDataFromBackup is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::dropReplica(
    zkutil::ZooKeeperPtr zookeeper, const TableZnodeInfo & zookeeper_info, LoggerPtr logger,
    MergeTreeSettingsPtr, std::optional<bool> * has_metadata_out)
{
    if (zookeeper->expired())
        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Table was not dropped because ZooKeeper session has expired.");

    const String & zookeeper_path = zookeeper_info.path;
    auto remote_replica_path = zookeeper_path + "/replicas/" + zookeeper_info.replica_name;

    LOG_INFO(logger, "Removing replica {}, marking it as lost", remote_replica_path);
    /// Mark itself lost before removing, because the following recursive removal may fail
    /// and partially dropped replica may be considered as alive one (until someone will mark it lost)
    zookeeper->trySet(remote_replica_path + "/is_lost", "1");

    /// NOTE: we should check for remote_replica_path existence,
    /// since otherwise DROP REPLICA will fail if the replica had been already removed.
    if (!zookeeper->exists(remote_replica_path))
    {
        LOG_INFO(logger, "Removing replica {} does not exist", remote_replica_path);
        return false;
    }

    {
        /// Remove "host" node first to mark replica as dropped (the choice is arbitrary,
        /// it could be any node without children that exists since ancient server versions and not re-created on startup)
        [[maybe_unused]] auto code = zookeeper->tryRemove(fs::path(remote_replica_path) / "host");
        chassert(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE);

        zookeeper->tryRemoveChildrenRecursive(remote_replica_path);

        /// Update has_metadata_in_zookeeper to avoid retries. Otherwise we can accidentally remove metadata of a new table on retries
        if (has_metadata_out)
            *has_metadata_out = false;

        if (zookeeper->tryRemove(remote_replica_path) != Coordination::Error::ZOK)
            LOG_ERROR(logger, "Replica was not completely removed from ZooKeeper, {} still exists and may contain some garbage.", remote_replica_path);
    }

    /// Check that `zookeeper_path` exists: it could have been deleted by another replica after execution of previous line.
    Strings replicas;
    if (Coordination::Error::ZOK != zookeeper->tryGetChildren(zookeeper_path + "/replicas", replicas) || !replicas.empty())
        return false;

    LOG_INFO(logger, "{} is the last replica, will remove table", remote_replica_path);

    /** At this moment, another replica can be created and we cannot remove the table.
      * Try to remove /replicas node first. If we successfully removed it,
      * it guarantees that we are the only replica that proceed to remove the table
      * and no new replicas can be created after that moment (it requires the existence of /replicas node).
      * and table cannot be recreated with new /replicas node on another servers while we are removing data,
      * because table creation is executed in single transaction that will conflict with remaining nodes.
      */

    /// Node /dropped works like a lock that protects from concurrent removal of old table and creation of new table.
    /// But recursive removal may fail in the middle of operation leaving some garbage in zookeeper_path, so
    /// we remove it on table creation if there is /dropped node. Creating thread may remove /dropped node created by
    /// removing thread, and it causes race condition if removing thread is not finished yet.
    /// To avoid this we also create ephemeral child before starting recursive removal.
    /// (The existence of child node does not allow to remove parent node).
    Coordination::Requests ops;
    Coordination::Responses responses;
    String drop_lock_path = zookeeper_path + "/dropped/lock";
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/replicas", -1));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/dropped", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(drop_lock_path, "", zkutil::CreateMode::Ephemeral));
    Coordination::Error code = zookeeper->tryMulti(ops, responses);

    if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
    {
        LOG_WARNING(logger, "Table {} is already started to be removing by another replica right now", remote_replica_path);
        return false;
    }
    if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_WARNING(logger, "Another replica was suddenly created, will keep the table {}", remote_replica_path);
        return false;
    }
    if (code != Coordination::Error::ZOK)
    {
        zkutil::KeeperMultiException::check(code, ops, responses);
    }
    else
    {
        auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(drop_lock_path, *zookeeper);
        LOG_INFO(logger, "Removing table {} (this might take several minutes)", zookeeper_path);
        removeTableNodesFromZooKeeper(zookeeper, zookeeper_info, metadata_drop_lock, logger);
    }

    return true;
}

bool StorageAmateurMergeTree::dropReplica(const String & drop_replica, LoggerPtr logger)
{
    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::dropReplica");
    zkutil::ZooKeeperPtr zookeeper = getZooKeeperIfTableShutDown();

    /// NOTE it's not atomic: replica may become active after this check, but before dropReplica(...)
    /// However, the main use case is to drop dead replica, which cannot become active.
    /// This check prevents only from accidental drop of some other replica.
    if (zookeeper->exists(zookeeper_info.path + "/replicas/" + drop_replica))
        throw Exception(ErrorCodes::TABLE_WAS_NOT_DROPPED, "Can't drop replica: {}, because it's active", drop_replica);

    TableZnodeInfo info = zookeeper_info;
    info.replica_name = drop_replica;
    return dropReplica(zookeeper, info, logger);
}

bool StorageAmateurMergeTree::removeTableNodesFromZooKeeper(zkutil::ZooKeeperPtr zookeeper,
        const TableZnodeInfo & zookeeper_info2, const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock, LoggerPtr logger)
{
    const String & zookeeper_path = zookeeper_info2.path;
    bool completely_removed = false;

    /// NOTE /block_numbers/ actually is not flat, because /block_numbers/<partition_id>/ may have ephemeral children,
    /// but we assume that all ephemeral block locks are already removed when table is being dropped.
    static constexpr std::array flat_nodes = {"block_numbers", "blocks", "async_blocks", "deduplication_hashes"};

    /// First try to remove paths that are known to be flat
    for (const auto * node : flat_nodes)
    {
        bool removed_quickly = zookeeper->tryRemoveChildrenRecursive(fs::path(zookeeper_path) / node, /* probably flat */ true);
        if (!removed_quickly)
            LOG_WARNING(logger, "Failed to quickly remove node '{}' and its children, fell back to recursive removal (table: {})",
                        node, zookeeper_path);
    }

    /// Then try to remove nodes that are known to have no children (and should always exist)
    Coordination::Requests ops;
    for (const auto * node : flat_nodes)
        ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/" + node, -1));

    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/columns", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path + "/metadata", -1));
    Coordination::Responses res;
    auto code = zookeeper->tryMulti(ops, res);
    if (code != Coordination::Error::ZOK)
        LOG_WARNING(logger, "Cannot quickly remove nodes without children: {} (table: {}). Will remove recursively.",
                    code, zookeeper_path);

    // FailPointInjection::pauseFailPoint(FailPoints::replicated_table_remove_zk_before_get_children);

    Strings children;
    code = zookeeper->tryGetChildren(zookeeper_path, children);
    if (code == Coordination::Error::ZNONODE)
    {
        /// It is possible if ZooKeeper session expired and ephemeral drop lock was deleted,
        /// allowing another process to complete the removal. The table is completely gone.
        LOG_WARNING(logger, "Table {} was already removed from ZooKeeper, looks like a concurrent operation removed it", zookeeper_path);
        return true;
    }

    for (const auto & child : children)
    {
        if (child != "dropped")
            zookeeper->tryRemoveRecursive(fs::path(zookeeper_path) / child);
    }

    // FailPointInjection::pauseFailPoint(FailPoints::replicated_table_remove_zk_before_final_multi);

    ops.clear();
    Coordination::Responses responses;
    ops.emplace_back(zkutil::makeRemoveRequest(metadata_drop_lock->getPath(), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(fs::path(zookeeper_path) / "dropped", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zookeeper_path, -1));
    code = zookeeper->tryMulti(ops, responses, /* check_session_valid */ true);

    if (code == Coordination::Error::ZNONODE)
    {
        /// It is possible if ZooKeeper session expired and the ephemeral drop lock was deleted,
        /// allowing another process to complete the removal or create a new table.
        LOG_WARNING(logger, "Table {} was not completely removed from ZooKeeper, some nodes were already removed by a concurrent operation", zookeeper_path);
    }
    else if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_ERROR(
            logger,
            "Table was not completely removed from ZooKeeper, {} still exists and may contain some garbage,"
            "but someone is removing it right now.",
            zookeeper_path);
    }
    else if (code != Coordination::Error::ZOK)
    {
        /// It is still possible that ZooKeeper session is expired or server is killed in the middle of the delete operation.
        zkutil::KeeperMultiException::check(code, ops, responses);
    }
    else
    {
        metadata_drop_lock->setAlreadyRemoved();
        completely_removed = true;
        LOG_INFO(logger, "Table {} was successfully removed from ZooKeeper", zookeeper_path);

        try
        {
            zookeeper_info2.dropAncestorZnodesIfNeeded(zookeeper);
        }
        catch (...)
        {
            LOG_WARNING(logger, "Failed to drop ancestor znodes {} - {} after dropping table: {}", zookeeper_info2.path_prefix_for_drop, zookeeper_info2.path, getCurrentExceptionMessage(false));
        }
    }

    return completely_removed;
}

bool StorageAmateurMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    if (shutdown_called)
        return false;

    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::scheduleDataProcessingJob");

    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
    auto settings_ptr = getSettings();

    auto shared_lock = lockForShare(
        RWLockImpl::NO_QUERY,
        (*settings_ptr)[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);

    {
        std::unique_lock lock(currently_processing_in_background_mutex);

        if (merger_mutator.merges_blocker.isCancelled())
            return false;

        auto zookeeper = getZooKeeper();
        auto merge_pred = std::make_shared<AmateurMergePredicate>(*this, zookeeper, zookeeper_path);
        auto parts_collector = std::make_shared<AmateurMergeTreePartsCollector>(*this, merge_pred);

        UInt64 max_source_parts_bytes = CompactionStatistics::getMaxSourcePartsBytesForMerge(*this);
        if (max_source_parts_bytes == 0)
            return false;

        UInt64 max_result_part_rows = CompactionStatistics::getMaxResultPartRowsCount(*this);
        bool merge_with_ttl_allowed = getTotalMergesWithTTLInMergeList()
            < (*settings_ptr)[MergeTreeSetting::max_number_of_merges_with_ttl_in_pool];

        auto select_result = merger_mutator.selectPartsToMerge(
            parts_collector,
            merge_pred,
            MergeSelectorApplier(
                {{max_source_parts_bytes, max_result_part_rows}},
                merge_with_ttl_allowed,
                /*aggressive=*/false,
                /*range_filter_=*/nullptr,
                getStorageID()),
            /*partitions_hint=*/std::nullopt);

        if (!select_result.has_value())
            return false;

        chassert(select_result.value().size() == 1);
        auto future_part = constructFuturePart(*this, select_result.value()[0], {MergeTreeDataPartState::Active});
        if (!future_part)
            return false;

        size_t total_size = CompactionStatistics::estimateNeededDiskSpace(future_part->parts, true);
        auto reserved_space = tryReserveSpacePreferringTTLRules(
            metadata_snapshot, total_size, {}, time(nullptr));
        if (!reserved_space)
            return false;

        future_part->updatePath(*this, reserved_space.get());

        currently_merging_mutating_parts.insert(future_part->parts.begin(), future_part->parts.end());

        auto reserved_space_shared = std::make_shared<ReservationSharedPtr>(std::move(reserved_space));
        auto task = std::make_shared<ExecutableLambdaAdapter>(
            [this, metadata_snapshot, future_part, reserved_space_shared, shared_lock]() mutable
            {
                SCOPE_EXIT({
                    std::lock_guard background_processing_lock(currently_processing_in_background_mutex);
                    for (const auto & part : future_part->parts)
                        currently_merging_mutating_parts.erase(part);
                });
                return mergeParts(metadata_snapshot, future_part, std::move(*reserved_space_shared), shared_lock);
            },
            common_assignee_trigger,
            getStorageID());

        return assignee.scheduleCommonTask(task, false);
    }
}

void StorageAmateurMergeTree::restoreMetadataInZooKeeper(const ZooKeeperRetriesInfo &, bool)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "restoreMetadataInZooKeeper is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::createEmptyPartInsteadOfLost(zkutil::ZooKeeperPtr, const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "createEmptyPartInsteadOfLost is not implemented for StorageAmateurMergeTree");
}

std::map<std::string, MutationCommands> StorageAmateurMergeTree::getUnfinishedMutationCommands() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getUnfinishedMutationCommands is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::checkBrokenDisks()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "checkBrokenDisks is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::removeSharedDetachedPart(DiskPtr, const String &, const String &, const String &,
    const String &, const String &, const ContextPtr &, const zkutil::ZooKeeperPtr &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "removeSharedDetachedPart is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::canUseZeroCopyReplication() const
{
    return false;
}

PartitionIdToMaxBlock StorageAmateurMergeTree::getMaxAddedBlocks() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getMaxAddedBlocks is not implemented for StorageAmateurMergeTree");
}

size_t StorageAmateurMergeTree::clearOldPartsAndRemoveFromZK()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "clearOldPartsAndRemoveFromZK is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::clearOldPartsAndRemoveFromZKImpl(zkutil::ZooKeeperPtr, DataPartsVector &&)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "clearOldPartsAndRemoveFromZKImpl is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::checkPartChecksumsAndAddCommitOps(
    const ZooKeeperWithFaultInjectionPtr &, const DataPartPtr &, Coordination::Requests &, String, NameSet &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "checkPartChecksumsAndAddCommitOps is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr &) const
{
    return false;
}

void StorageAmateurMergeTree::forcefullyRemoveBrokenOutdatedPartFromZooKeeperBeforeDetaching(const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "forcefullyRemoveBrokenOutdatedPartFromZooKeeperBeforeDetaching is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::dropPartNoWaitNoThrow(const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "dropPartNoWaitNoThrow is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::dropPart(const String &, bool, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "dropPart is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context)
{
    auto component_guard = Coordination::setCurrentComponent("StorageAmateurMergeTree::dropPartition");
    assertNotReadonly();

    if (detach)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DETACH PARTITION queries are not implemented for StorageAMateurMergeTree.");

    zkutil::ZooKeeperPtr zookeeper = getZooKeeperAndAssertNotReadonly();
    const auto * partition_ast = partition->as<ASTPartition>();

    std::vector<std::string> partitions;
    if (partition_ast && partition_ast->all)
        partitions = zookeeper->getChildren(fs::path(zookeeper_path) / "block_numbers");
    else
        partitions = {getPartitionIDFromQuery(partition, query_context)};

    const auto partition_to_drop = partitions.front();

    const auto block_lock = allocateBlockNumber(partition_to_drop, getZooKeeper(), {});
    const auto right = block_lock.getNumber();

    const auto drop_range = MergeTreePartInfo(partition_to_drop, 0, right, MergeTreePartInfo::MAX_LEVEL, MergeTreePartInfo::MAX_BLOCK_NUMBER);

    DataPartsVector parts_to_outdate;
    {
        auto parts_lock = readLockParts();
        parts_to_outdate = grabActivePartsToRemoveForDropRange(NO_TRANSACTION_RAW, drop_range, parts_lock);
    }

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(fs::path(zookeeper_path) / "parts" / drop_range.getPartNameForLogs(), "", zkutil::CreateMode::Persistent));
    for (const auto & part : parts_to_outdate)
    {
        ops.emplace_back(zkutil::makeRemoveRequest(fs::path(zookeeper_path) / "parts" / part->name, -1));
        ops.emplace_back(zkutil::makeCreateRequest(fs::path(zookeeper_path) / "parts_stale" / part->name, "", zkutil::CreateMode::Persistent));
    }

    // TODO: better approach would be to read parts from /parts node, and commit
    // covered parts as stale, instead of using local state. On ZNONODE, retry
    // and commit if there are no parts crossing drop marker.
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    zkutil::KeeperMultiException::check(code, ops, responses);
}

PartitionCommandsResultInfo StorageAmateurMergeTree::attachPartition(const PartitionCommand &, const StorageMetadataPtr &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "attachPartition is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::replacePartitionFrom(const StoragePtr &, const ASTPtr &, bool, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "replacePartitionFrom is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::movePartitionToTable(const StoragePtr &, const ASTPtr &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "movePartitionToTable is not implemented for StorageAmateurMergeTree");
}

MutationCommands StorageAmateurMergeTree::MutationsSnapshot::getOnFlyMutationCommandsForPart(const MergeTreeData::DataPartPtr &) const
{
    return MutationCommands();
}

NameSet StorageAmateurMergeTree::MutationsSnapshot::getAllUpdatedColumns() const
{
    return NameSet();
}

MergeTreeData::MutationsSnapshotPtr StorageAmateurMergeTree::getMutationsSnapshot(const IMutationsSnapshot::Params &) const
{
    return std::make_shared<StorageAmateurMergeTree::MutationsSnapshot>();
}

void StorageAmateurMergeTree::attachRestoredParts(MutableDataPartsVector &&, const std::optional<ZooKeeperRetriesInfo> &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "attachRestoredParts is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::removeDetachedPart(DiskPtr, const String &, const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "removeDetachedPart is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::createAndStoreFreezeMetadata(DiskPtr, DataPartPtr, String) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "createAndStoreFreezeMetadata is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::checkZeroCopyLockExists(const String &, const DiskPtr &, String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "checkZeroCopyLockExists is not implemented for StorageAmateurMergeTree");
}

void StorageAmateurMergeTree::watchZeroCopyLock(const String &, const DiskPtr &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "watchZeroCopyLock is not implemented for StorageAmateurMergeTree");
}

std::optional<ZeroCopyLock> StorageAmateurMergeTree::tryCreateZeroCopyExclusiveLock(const String &, const DiskPtr &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "tryCreateZeroCopyExclusiveLock is not implemented for StorageAmateurMergeTree");
}

bool StorageAmateurMergeTree::waitZeroCopyLockToDisappear(const ZeroCopyLock &, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "waitZeroCopyLockToDisappear is not implemented for StorageAmateurMergeTree");
}

}
