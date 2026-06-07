#pragma once

#include <Interpreters/Cluster.h>
#include <Interpreters/PartLog.h>
#include <Parsers/SyncReplicaMode.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include <Storages/IStorageCluster.h>
#include <Storages/MergeTree/AsyncBlockIDsCache.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/RenamingRestrictions.h>
#include <Storages/TableZnodeInfo.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/escapeForFileName.h>
#include <Common/EventNotifier.h>
#include <Common/ProfileEventsScope.h>
#include <Common/Throttler.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/randomSeed.h>
#include <base/UUID.h>
#include <base/defines.h>

#include <atomic>
#include <set>
#include <pcg_random.hpp>


namespace DB
{

class ZooKeeperWithFaultInjection;
using ZooKeeperWithFaultInjectionPtr = std::shared_ptr<ZooKeeperWithFaultInjection>;

class StorageAmateurMergeTree final : public MergeTreeData
{
public:
    StorageAmateurMergeTree(
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
        const ZooKeeperRetriesInfo & create_query_zookeeper_retries_info_);

    void startup() override;

    void partialShutdown();
    void flushAndPrepareForShutdown() override;
    void shutdown(bool is_drop) override;

    ~StorageAmateurMergeTree() override;

    std::string getName() const override;

    bool supportsParallelInsert() const override { return true; }
    bool supportsReplication() const override { return true; }
    bool supportsDeduplication() const override { return true; }
    bool supportsStreaming() const override { return true; }

    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr query_context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context) override;
    PartitionCommandsResultInfo attachPartition(const PartitionCommand & command, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr query_context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr query_context) override;

    CursorPromotersMap buildPromoters() override;

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    template <class Func>
    void foreachActiveParts(Func && func) const;

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalRowsByPartitionPredicate(const ActionsDAG & filter_actions_dag, ContextPtr context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytesUncompressed(const Settings & settings) const override;
    MutationCounters getMutationCounters() const override;

    Int64 getCurrentMutationVersion(const String & /*partition_id*/, Int64 /*data_version*/) const { return 0; }
    Int64 getNextMutationVersion(const String & /*partition_id*/, Int64 /*data_version*/) const { return 0; }

    /// Creates new block number if block with such block_id does not exist
    /// If zookeeper_path_prefix specified then allocate block number on this path
    /// (can be used if we want to allocate blocks on other replicas)
    EphemeralLockInZooKeeper allocateBlockNumber(
        const String & partition_id,
        const zkutil::ZooKeeperPtr & zookeeper,
        const std::vector<std::string> & zookeeper_block_id_paths = {},
        const String & zookeeper_path_prefix = "",
        const std::optional<String> & znode_data = std::nullopt) const;

    EphemeralLockInZooKeeper allocateBlockNumber(
        const String & partition_id,
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        const std::vector<std::string> & zookeeper_block_id_paths = {},
        const String & zookeeper_path_prefix = "",
        const std::optional<String> & znode_data = std::nullopt) const;

    void getCommitPartOps(Coordination::Requests & ops, const DataPartPtr & part, const String & block_id_path = "") const;
    void getCommitPartOps(Coordination::Requests & ops, const DataPartPtr & part, const std::vector<String> & block_id_paths) const;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr query_context) override;

    void alter(const AlterCommands & commands, ContextPtr query_context, AlterLockHolder & table_lock_holder) override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;
    void waitMutation(const String & znode_name, size_t mutations_sync) const;
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;
    CancellationCode killMutation(const String & mutation_id) override;

    QueryPipeline updateLightweight(const MutationCommands & commands, ContextPtr query_context) override;
    bool haveCommittingOps(const CommittingBlocks & committing_blocks, PartitionIdToMaxBlockPtr partitions, std::set<CommittingBlock::Op> ops) const;
    void waitForCommittingOpsToFinish(zkutil::ZooKeeperPtr zookeeper, PartitionIdToMaxBlockPtr partitions, std::set<CommittingBlock::Op> ops, size_t backoff_ms, size_t sync_timeout_ms);

    void drop() override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr query_context, TableExclusiveLockHolder &) override;

    void checkTableCanBeRenamed(const StorageID & new_name) const override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    void onActionLockRemove(StorageActionBlockType action_type) override;

    DataValidationTasksPtr getCheckTaskList(const CheckTaskFilter & check_task_filter, ContextPtr context) override;
    std::optional<CheckResult> checkDataNext(DataValidationTasksPtr & check_task_list) override;

    bool canUseAdaptiveGranularity() const override;

    void applyMetadataChangesToCreateQueryForBackup(const ASTPtr & create_query) const override;

    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

    static bool dropReplica(zkutil::ZooKeeperPtr zookeeper, const TableZnodeInfo & zookeeper_info,
                            LoggerPtr logger, MergeTreeSettingsPtr table_settings = nullptr, std::optional<bool> * has_metadata_out = nullptr);

    bool dropReplica(const String & drop_replica, LoggerPtr logger);

    static bool removeTableNodesFromZooKeeper(
        zkutil::ZooKeeperPtr zookeeper, const TableZnodeInfo & zookeeper_info2,
        const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock, LoggerPtr logger);

    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    const String & getReplicaName() const { return replica_name; }
    const String & getReplicaPath() const { return replica_path; }

    std::string getPostfixForTempInsertName() const override
    {
        return escapeForFileName(replica_name);
    }

    void restoreMetadataInZooKeeper(const ZooKeeperRetriesInfo & zookeeper_retries_info, bool is_called_during_attach);

    bool createEmptyPartInsteadOfLost(zkutil::ZooKeeperPtr zookeeper, const String & lost_part_name);

    const String & getZooKeeperName() const { return zookeeper_info.zookeeper_name; }
    const String & getZooKeeperPath() const { return zookeeper_info.path; }
    const String & getFullZooKeeperPath() const { return zookeeper_info.full_path; }

    std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const override;

    void checkBrokenDisks();

    static bool removeSharedDetachedPart(DiskPtr disk, const String & path, const String & part_name, const String & table_uuid,
        const String & replica_name, const String & zookeeper_path, const ContextPtr & local_context, const zkutil::ZooKeeperPtr & zookeeper);

    bool canUseZeroCopyReplication() const;
    bool isSharedStorage() const override { return true; }

    bool isTableReadOnly () { return is_readonly || isStaticStorage(); }

    std::optional<bool> hasMetadataInZooKeeper () { return has_metadata_in_zookeeper; }

    PartitionIdToMaxBlock getMaxAddedBlocks() const;

private:
    friend class AmateurMergeTreeSink;
    friend class AmateurMergeTreePartsCollector;
    friend class AsyncBlockIDsCache<StorageAmateurMergeTree>;

    size_t clearOldPartsAndRemoveFromZK();
    void clearOldPartsAndRemoveFromZKImpl(zkutil::ZooKeeperPtr zookeeper, DataPartsVector && parts);

    using MergeTreeData::MutableDataPartPtr;

    zkutil::ZooKeeperPtr current_zookeeper;
    mutable std::mutex current_zookeeper_mutex;

    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeperIfTableShutDown() const;
    zkutil::ZooKeeperPtr getZooKeeperAndAssertNotReadonly() const;
    zkutil::ZooKeeperPtr getZooKeeperAndAssertNotStaticStorage() const;
    void setZooKeeper();
    String getEndpointName() const;

    std::atomic_bool is_readonly {true};

    std::optional<bool> has_metadata_in_zookeeper;

    const TableZnodeInfo zookeeper_info;
    const String zookeeper_path;
    const String replica_name;
    const String replica_path;

    ZooKeeperRetriesInfo create_query_zookeeper_retries_info TSA_GUARDED_BY(create_query_zookeeper_retries_info_mutex);
    mutable std::mutex create_query_zookeeper_retries_info_mutex;

    MergeTreeDataWriter writer;
    MergeTreeDataMergerMutator merger_mutator;

    Poco::Event partial_shutdown_event {false};

    std::atomic<bool> shutdown_called {false};
    std::atomic<bool> shutdown_prepared_called {false};

    std::mutex currently_processing_in_background_mutex;

    /// Parts that currently participate in merge or mutation.
    /// This set have to be used with `currently_processing_in_background_mutex`.
    DataParts currently_merging_mutating_parts;

    std::atomic<bool> initialization_done{false};

    AsyncBlockIDsCache<StorageAmateurMergeTree> deduplication_hashes_cache;
    AsyncBlockIDsCache<StorageAmateurMergeTree> async_block_ids_cache;

    BackgroundSchedulePoolTaskHolder parts_syncing_task;
    void partsSyncingTask();

    BackgroundSchedulePoolTaskHolder cleanup_task;
    void cleanupTask();

    /// Last observed pZxid of the /parts znode, updated by partsSyncingTask.
    /// Used by getStorageSnapshot to capture the current epoch for active reads.
    std::atomic<Int64> current_epoch{0};

    /// Last epoch value written to ZK. Guards against redundant writes.
    std::atomic<Int64> last_published_epoch{0};

    bool createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot, const ZooKeeperRetriesInfo & zookeeper_retries_info);
    bool createTableIfNotExistsAttempt(const StorageMetadataPtr & metadata_snapshot, QueryStatusPtr process_list_element) const;

    void createReplica(const StorageMetadataPtr & metadata_snapshot, const ZooKeeperRetriesInfo & zookeeper_retries_info);
    void createReplicaAttempt(const StorageMetadataPtr & metadata_snapshot, QueryStatusPtr process_list_element) const;

    ZooKeeperRetriesInfo getCreateQueryZooKeeperRetriesInfo() const;
    void clearCreateQueryZooKeeperRetriesInfo();

    bool checkTableStructure(const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot, int32_t * metadata_version, bool strict_check,
                             const ZooKeeperRetriesInfo & zookeeper_retries_info);
    bool checkTableStructureAttempt(const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot, int32_t * metadata_version, bool strict_check) const;

    bool checkPartChecksumsAndAddCommitOps(
        const ZooKeeperWithFaultInjectionPtr & zookeeper,
        const DataPartPtr & part,
        Coordination::Requests & ops,
        String part_name,
        NameSet & absent_replicas_paths);

    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;

    bool mergeParts(
        const StorageMetadataPtr & metadata_snapshot,
        const FutureMergedMutatedPartPtr & future_part,
        ReservationSharedPtr reserved_space,
        TableLockHolder & table_lock_holder);

    void forcefullyRemoveBrokenOutdatedPartFromZooKeeperBeforeDetaching(const String & part_name) override;

    void assertNotReadonly() const;
    void assertNotStaticStorage() const;

    mutable std::unordered_set<std::string> existing_nodes_cache;
    mutable std::mutex existing_nodes_cache_mutex;
    bool existsNodeCached(const ZooKeeperWithFaultInjectionPtr & zookeeper, const std::string & path) const;

    /// Epochs of all currently-active storage snapshots (reads in flight).
    /// getStorageSnapshot registers, snapshot destruction unregisters.
    mutable std::multiset<Int64> active_snapshot_epochs;
    mutable std::mutex active_snapshot_epochs_mutex;

    /// Guards that multiple getStorageSnapshot calls for the same parent
    /// StorageSnapshot share one epoch registration.  The guard is released
    /// when the parent snapshot is destroyed (the weak_ptr expires).
    mutable std::mutex snapshot_guard_cache_mutex;
    mutable std::vector<std::pair<std::weak_ptr<StorageSnapshot>, std::shared_ptr<void>>> snapshot_guard_cache;
    void cleanupStaleSnapshotGuards() const;

    struct MutationsSnapshot : public MergeTreeData::MutationsSnapshotBase
    {
    public:
        using Params = MergeTreeData::IMutationsSnapshot::Params;

        MutationsSnapshot() = default;

        MutationCommands getOnFlyMutationCommandsForPart(const MergeTreeData::DataPartPtr & part) const override;
        std::shared_ptr<MergeTreeData::IMutationsSnapshot> cloneEmpty() const override { return std::make_shared<MutationsSnapshot>(); }
        NameSet getAllUpdatedColumns() const override;
    };

    MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & params) const override;

    void startBackgroundMovesIfNeeded() override;

    void attachRestoredParts(MutableDataPartsVector && parts, const std::optional<ZooKeeperRetriesInfo> & zookeeper_retries_info) override;

    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

    bool removeDetachedPart(DiskPtr disk, const String & path, const String & part_name) override;

    void createAndStoreFreezeMetadata(DiskPtr disk, DataPartPtr part, String backup_part_path) const override;

    String getSharedDataPath() const;
    void createSharedDataPath(const String & data_path, const ZooKeeperRetriesInfo & zookeeper_retries_info);
    void createSharedDataPathAttempt(const String & data_path) const;

    bool checkZeroCopyLockExists(const String & part_name, const DiskPtr & disk, String & lock_replica);
    void watchZeroCopyLock(const String & part_name, const DiskPtr & disk);

    std::optional<String> getZeroCopyPartPath(const String & part_name, const DiskPtr & disk);

    std::optional<ZeroCopyLock> tryCreateZeroCopyExclusiveLock(const String & part_name, const DiskPtr & disk) override;

    bool waitZeroCopyLockToDisappear(const ZeroCopyLock & lock, size_t milliseconds_to_wait) override;

    std::vector<String> getZookeeperZeroCopyLockPaths() const;
    static void dropZookeeperZeroCopyLockPaths(zkutil::ZooKeeperPtr zookeeper,
                                               std::vector<String> zero_copy_locks_paths, LoggerPtr logger);
};

}
