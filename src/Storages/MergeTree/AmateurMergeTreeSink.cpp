#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/AmateurMergeTreeSink.h>
#include <Storages/MergeTree/InsertBlockInfo.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/AsyncBlockIDsCache.h>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <IO/Operators.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ThreadFuzzer.h>
#include "Storages/StorageAmateurMergeTree.h"
#include <base/scope_guard.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <algorithm>
#include <vector>

namespace ProfileEvents
{
    extern const Event DuplicatedInsertedBlocks;
    extern const Event SelfDuplicatedAsyncInserts;
    extern const Event DuplicatedAsyncInserts;
    extern const Event DuplicationElapsedMicroseconds;

    extern const Event QuorumParts;
    extern const Event QuorumWaitMicroseconds;
    extern const Event QuorumFailedInserts;
}

namespace DB
{
namespace Setting
{
    extern const SettingsFloat insert_keeper_fault_injection_probability;
    extern const SettingsUInt64 insert_keeper_fault_injection_seed;
    extern const SettingsUInt64 insert_keeper_max_retries;
    extern const SettingsUInt64 insert_keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 insert_keeper_retry_max_backoff_ms;
    extern const SettingsUInt64 input_format_max_block_wait_ms;
    extern const SettingsUInt64 max_insert_delayed_streams_for_parallel_write;
    extern const SettingsBool optimize_on_insert;
}

namespace ServerSetting
{
    extern const ServerSettingsInsertDeduplicationVersions insert_deduplication_version;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMilliseconds sleep_before_commit_local_part_in_replicated_table_ms;
    extern const MergeTreeSettingsUInt64 replicated_deduplication_window;
    extern const MergeTreeSettingsUInt64 replicated_deduplication_window_for_async_inserts;
}

namespace FailPoints
{
    extern const char replicated_merge_tree_commit_zk_fail_after_op[];
    extern const char replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault[];
    extern const char replicated_merge_tree_insert_retry_pause[];
    extern const char replicated_merge_tree_restore_attach_retry[];
    extern const char rmt_delay_commit_part[];
}

namespace ErrorCodes
{
    extern const int TOO_FEW_LIVE_REPLICAS;
    extern const int UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
    extern const int READONLY;
    extern const int UNKNOWN_STATUS_OF_INSERT;
    extern const int INSERT_WAS_DEDUPLICATED;
    extern const int DUPLICATE_DATA_PART;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int LOGICAL_ERROR;
    extern const int TABLE_IS_READ_ONLY;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{
    /// Convert block id vector to string. Output at most 50 ids.
    template<typename T>
    inline String toString(const std::vector<T> & vec)
    {
        size_t size = vec.size();
        size = std::min<size_t>(size, 50);
        return fmt::format("({})", fmt::join(vec.begin(), vec.begin() + size, ","));
    }
}

AmateurMergeTreeSink::AmateurMergeTreeSink(
    bool async_insert_,
    StorageAmateurMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    size_t max_parts_per_block_,
    ContextPtr context_,
    bool is_attach_,
    bool allow_attach_while_readonly_,
    std::optional<ZooKeeperRetriesInfo> keeper_retries_info_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , max_parts_per_block(max_parts_per_block_)
    , is_attach(is_attach_)
    , allow_attach_while_readonly(allow_attach_while_readonly_)
    , deduplicate(
        async_insert_
        ? (*storage.getSettings())[MergeTreeSetting::replicated_deduplication_window_for_async_inserts] != 0
        : (*storage.getSettings())[MergeTreeSetting::replicated_deduplication_window] != 0)
    , log(getLogger(storage.getLogName() + " (Replicated OutputStream)"))
    , context(context_)
    , storage_snapshot(storage.getStorageSnapshotWithoutData(metadata_snapshot, context_))
    , keeper_retries_info(std::move(keeper_retries_info_))
    , is_async_insert(async_insert_)
    , insert_deduplication_version(context->getServerSettings()[ServerSetting::insert_deduplication_version].value)
{
    LOG_DEBUG(log, "Create AmateurMergeTreeSink {} async_insert={}, deduplicate={}, max_parts_per_block={}, is_attach={}",
        storage.getStorageID().getNameForLogs(),
        is_async_insert,
        deduplicate,
        max_parts_per_block_,
        is_attach_);
}

AmateurMergeTreeSink::~AmateurMergeTreeSink()
{
    if (delayed_parts.empty())
        return;

    chassert(isCancelled() || std::uncaught_exceptions());

    for (auto & partition : delayed_parts)
    {
        partition.temp_part->cancel();
    }
    delayed_parts.clear();
}

void AmateurMergeTreeSink::consume(Chunk & chunk)
{
    auto component_guard = Coordination::setCurrentComponent("AmateurMergeTreeSink::consume");
    if (num_blocks_processed > 0)
        storage.delayInsertOrThrowIfNeeded(&storage.partial_shutdown_event, context, false);

    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    const auto & settings = context->getSettingsRef();

    ZooKeeperWithFaultInjectionPtr zookeeper = createKeeper("AmateurMergeTreeSink::consume");

    auto deduplication_info = chunk.getChunkInfos().getSafe<DeduplicationInfo>();

    BlocksWithPartition part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts_per_block, metadata_snapshot, context);

    decltype(delayed_parts) current_parts;

    size_t total_streams = 0;
    bool support_parallel_write = false;

    std::vector<UInt128> all_partitions_block_ids;

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        ProfileEvents::Counters part_counters;
        auto profile_events_scope = std::make_unique<ProfileEventsScope>(&part_counters);

        auto current_deduplication_info = deduplication_info->cloneSelf();

        {
            ProfileEventTimeIncrement<Microseconds> duplication_elapsed(ProfileEvents::DuplicationElapsedMicroseconds);

            auto result = current_deduplication_info->deduplicateSelf(deduplicate, current_block.partition_id, context);

            if (result.removed_rows > 0)
            {
                ProfileEvents::increment(ProfileEvents::SelfDuplicatedAsyncInserts, result.removed_tokens);
                LOG_DEBUG(
                    log,
                    "In partition {} self deduplication removed tokens {} out of {}, left rows {} in tokens {}, debug: {}",
                    current_block.partition_id,
                    result.removed_tokens,
                    current_deduplication_info->getCount(),
                    result.filtered_block->rows(),
                    result.deduplication_info->getCount(),
                    result.deduplication_info->debug());

                current_block.block = result.filtered_block;
                current_deduplication_info = result.deduplication_info;
            }
        }

        /// Write part to the filesystem under temporary name. Calculate a checksum.
        auto temp_part = writeNewTempPart(current_block);

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part->part)
            continue;

        if (!support_parallel_write && temp_part->part->getDataPartStorage().supportParallelWrite())
            support_parallel_write = true;

        auto hash = temp_part->part->getPartBlockIDHash();
        current_deduplication_info->setPartWriterHashForPartition(hash, current_block.block->rows());

        LOG_DEBUG(
            log,
            "Wrote block with {} rows and deduplication blocks: {}, deduplication info: {}",
            current_block.block->rows(),
            fmt::join(getDeduplicationBlockIds(current_deduplication_info->getDeduplicationHashes(current_block.partition_id, deduplicate)), ", "),
            current_deduplication_info->debug());

        all_partitions_block_ids.push_back(hash);

        profile_events_scope.reset();
        UInt64 elapsed_ns = watch.elapsed();

        size_t max_insert_delayed_streams_for_parallel_write = 0;
        if (settings[Setting::max_insert_delayed_streams_for_parallel_write].changed)
            max_insert_delayed_streams_for_parallel_write = settings[Setting::max_insert_delayed_streams_for_parallel_write];
        else if (support_parallel_write)
            max_insert_delayed_streams_for_parallel_write = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
        else
            max_insert_delayed_streams_for_parallel_write = 0;

        /// In case of too much columns/parts in block, flush explicitly.
        size_t current_streams = 0;
        for (const auto & stream : temp_part->streams)
            current_streams += stream.stream->getNumberOfOpenStreams();

        if (total_streams + current_streams > max_insert_delayed_streams_for_parallel_write)
        {
            finishDelayed(zookeeper);
            delayed_parts = std::move(current_parts);
            finishDelayed(zookeeper);

            total_streams = 0;
            support_parallel_write = false;
            current_parts.clear();
        }

        current_parts.push_back(
            {
                .log = log,
                .block_with_partition = std::move(current_block),
                .deduplication_info = std::move(current_deduplication_info),
                .temp_part = std::move(temp_part),
                .elapsed_ns = elapsed_ns,
                .part_counters = std::move(part_counters),
            });

        total_streams += current_streams;
    }

    deduplication_info->setPartWriterHashes(all_partitions_block_ids, chunk.getNumRows());

    finishDelayed(zookeeper);
    delayed_parts = std::move(current_parts);
    /// Streaming `INSERT` flushes partial blocks on a timeout, so commit the just-written
    /// part immediately to make its rows visible without waiting for the next consume()
    /// or onFinish(); the normal write/commit pipelining is preferred otherwise.
    if (settings[Setting::input_format_max_block_wait_ms] != 0)
        finishDelayed(zookeeper);

    ++num_blocks_processed;
}

MergeTreeTemporaryPartPtr AmateurMergeTreeSink::writeNewTempPart(BlockWithPartition & block)
{
    return storage.writer.writeTempPart(block, metadata_snapshot, context);
}

void AmateurMergeTreeSink::finishDelayed(const ZooKeeperWithFaultInjectionPtr & zookeeper)
{
    if (delayed_parts.empty())
        return;

    for (auto & partition : delayed_parts)
    {
        ExecutionStatus status;
        std::vector<std::string> block_ids_for_log;

        {
            Stopwatch watch;
            SCOPE_EXIT({
                partition.elapsed_ns += watch.elapsed();
            });
            auto profile_events_scope = std::make_unique<ProfileEventsScope>(&partition.part_counters);

            std::set<std::string> parts_to_wait_for_quorum;

            /// reset the cache version to zero for every partition write.
            /// Version zero allows to avoid wait on first iteration
            deduplication_async_inserts_cache_version = 0;
            size_t retry_times = 0;
            while (true)
            {
                partition.temp_part->finalize();
                auto deduplication_hashes = partition.deduplication_info->getDeduplicationHashes(partition.block_with_partition.partition_id, deduplicate);
                auto deduplication_blocks_ids = getDeduplicationBlockIds(deduplication_hashes);

                auto conflicts = commitPart(zookeeper, partition.temp_part->part, deduplication_hashes, deduplication_blocks_ids);

                if (conflicts.empty())
                {
                    // Successfully committed
                    block_ids_for_log = deduplication_blocks_ids;
                    partition.temp_part->prewarmCaches();
                    break;
                }

                ++retry_times;
                // TODO: sync debuplication could use cache too

                if (insert_deduplication_version != InsertDeduplicationVersions::OLD_SEPARATE_HASHES)
                    storage.deduplication_hashes_cache.triggerCacheUpdate();

                if (is_async_insert)
                    storage.async_block_ids_cache.triggerCacheUpdate();

                {
                    ProfileEventTimeIncrement<Microseconds> duplication_elapsed(ProfileEvents::DuplicationElapsedMicroseconds);

                    LOG_DEBUG(log, "Found duplicate block IDs: {}, retry times {}", fmt::join(getDeduplicationBlockIds(conflicts), ", "), retry_times);

                    auto result = partition.deduplication_info->deduplicateBlock(
                        getDeduplicationBlockIds(conflicts),
                        partition.block_with_partition.partition_id,
                        context);

                    if (is_async_insert)
                        ProfileEvents::increment(ProfileEvents::DuplicatedAsyncInserts, result.removed_tokens);
                    else
                        ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks, result.removed_tokens);

                    LOG_DEBUG(
                        log,
                        "After filtering by collision, removed rows {}/{}, removed tokets {}/{} from origin block, after retry remaining rows: {}, remaining tokens: {}, elapsed {} ms, new deduplication info debug: {}",
                        result.removed_rows,
                        partition.deduplication_info->getRows(),
                        result.removed_tokens,
                        partition.deduplication_info->getCount(),
                        result.filtered_block->rows(),
                        result.deduplication_info->getCount(),
                        duplication_elapsed.elapsed() / 1000,
                        result.deduplication_info->debug());

                    partition.block_with_partition.block = result.filtered_block;
                    partition.deduplication_info = std::move(result.deduplication_info);
                }

                if (partition.block_with_partition.block->rows() == 0)
                {
                    // Whole block was deduplicated

                    if (!is_async_insert)
                    {
                        chassert(conflicts.size() == 1);
                        auto block_id = conflicts.front().getBlockId();
                        auto actual_part_name = conflicts.front().getConflictPartName();
                        bool exists_locally = bool(storage.getActiveContainingPart(actual_part_name));
                        LOG_INFO(
                            log,
                            "Block with ID {} {} as part {}; ignoring it.",
                            block_id,
                            exists_locally ? "already exists locally" : "already exists on other replicas",
                            actual_part_name);
                    }

                    block_ids_for_log = getDeduplicationBlockIds(conflicts);
                    status = ExecutionStatus(ErrorCodes::INSERT_WAS_DEDUPLICATED, "The part was deduplicated");
                    break;
                }

                partition.block_with_partition.partition = MergeTreePartition(partition.temp_part->part->partition.value);
                /// partition.temp_part is already finalized, no need to call cancel
                partition.temp_part = writeNewTempPart(partition.block_with_partition);
            }
        }

        // profile_events_scope has to be destroyed in the scope above
        auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
        PartLog::addNewPart(
            storage.getContext(),
            PartLog::PartLogEntry(partition.temp_part->part, partition.elapsed_ns, counters_snapshot),
            block_ids_for_log, // it is either blocks for committed part, or conflicting blocks for deduplicated part
            status);
    }

    delayed_parts.clear();
}

bool AmateurMergeTreeSink::writeExistingPart(MergeTreeData::MutableDataPartPtr & part)
{
    /// NOTE: No delay in this case. That's Ok.
    auto origin_zookeeper = storage.getZooKeeper();
    auto zookeeper = std::make_shared<ZooKeeperWithFaultInjection>(origin_zookeeper);

    Stopwatch watch;
    ProfileEventsScope profile_events_scope;

    String original_part_dir = part->getDataPartStorage().getPartDirectory();
    auto try_rollback_part_rename = [this, &part, &original_part_dir] ()
    {
        if (original_part_dir == part->getDataPartStorage().getPartDirectory())
            return;

        if (part->new_part_was_committed_to_zookeeper_after_rename_on_disk)
            return;

        /// Probably we have renamed the part on disk, but then failed to commit it to ZK.
        /// We should rename it back, otherwise it will be lost (e.g. if it was a part from detached/ and we failed to attach it).
        try
        {
            part->renameTo(original_part_dir, /*remove_new_dir_if_exists*/ false);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    };

    bool keep_non_zero_level = storage.merging_params.mode != MergeTreeData::MergingParams::Ordinary;
    part->info.level = (keep_non_zero_level && part->info.level > 0) ? 1 : 0;
    part->info.mutation = 0;
    part->version->setAndStoreCreationTID(Tx::NonTransactionalTID, nullptr);
    std::vector<DeduplicationHash> deduplication_hashes;
    if (deduplicate)
    {
        switch (insert_deduplication_version)
        {
            case InsertDeduplicationVersions::OLD_SEPARATE_HASHES:
                deduplication_hashes.emplace_back(DeduplicationHash::createSyncHash(part->checksums.getTotalChecksumUInt128(), part->info.getPartitionId()));
                break;
            case InsertDeduplicationVersions::COMPATIBLE_DOUBLE_HASHES:
                deduplication_hashes.emplace_back(DeduplicationHash::createSyncHash(part->checksums.getTotalChecksumUInt128(), part->info.getPartitionId()));
                deduplication_hashes.emplace_back(DeduplicationHash::createUnifiedHash(part->checksums.getTotalChecksumUInt128(), part->info.getPartitionId()));
                break;
            case InsertDeduplicationVersions::NEW_UNIFIED_HASHES:
                deduplication_hashes.emplace_back(DeduplicationHash::createUnifiedHash(part->checksums.getTotalChecksumUInt128(), part->info.getPartitionId()));
                break;
        }
    }

    auto deduplication_ids = getDeduplicationBlockIds(deduplication_hashes);

    try
    {
        auto conflicts = commitPart(zookeeper, part, deduplication_hashes, deduplication_ids);
        bool deduplicated = !conflicts.empty();

        int error = 0;
        String error_message;
        /// Set a special error code if the block is duplicate
        if (deduplicate && deduplicated)
        {
            error = ErrorCodes::INSERT_WAS_DEDUPLICATED;
            error_message = "The part was deduplicated";

            const auto & relative_path = part->getDataPartStorage().getRelativePath();
            const auto part_dir = fs::path(relative_path).parent_path().filename().string();

            if (relative_path.ends_with("detached/attaching_" + part->name + "/"))
            {
                /// Part came from ATTACH PART - rename back to detached/ (remove attaching_ prefix)
                fs::path new_relative_path = fs::path("detached") / part->getNewName(part->info);
                part->renameTo(new_relative_path, false);
            }
            else if (part_dir.starts_with("tmp_restore_" + part->name))
            {
                /// Part came from RESTORE with a temporary directory.
                /// Just remove the temporary part since it's a duplicate.
                LOG_DEBUG(log, "Removing deduplicated part {} from temporary path {}", part->name, relative_path);
                part->removeIfNeeded();
            }
            else
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected deduplicated part with relative path '{}' and part directory '{}'. "
                    "Expected relative path to end with 'detached/attaching_{}/' or part directory to start with 'tmp_restore_{}'.",
                    relative_path, part_dir, part->name, part->name);
            }
        }
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, watch.elapsed(), profile_events_scope.getSnapshot()), deduplication_ids, ExecutionStatus(error, error_message));
        return deduplicated;
    }
    catch (...)
    {
        try_rollback_part_rename();
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, watch.elapsed(), profile_events_scope.getSnapshot()), deduplication_ids, ExecutionStatus::fromCurrentException("", true));
        throw;
    }
}

std::vector<DeduplicationHash> AmateurMergeTreeSink::detectConflictsInAsyncBlockIDs(const std::vector<DeduplicationHash> & deduplication_hashes)
{
    if (insert_deduplication_version != InsertDeduplicationVersions::NEW_UNIFIED_HASHES)
    {
        auto conflict_block_ids = storage.async_block_ids_cache.detectConflicts(deduplication_hashes, deduplication_async_inserts_cache_version);
        if (!conflict_block_ids.empty())
        {
            deduplication_async_inserts_cache_version = 0;
            return conflict_block_ids;
        }
    }

    if (insert_deduplication_version != InsertDeduplicationVersions::OLD_SEPARATE_HASHES)
    {
        auto conflict_block_ids = storage.deduplication_hashes_cache.detectConflicts(deduplication_hashes, deduplication_cache_version);
        if (!conflict_block_ids.empty())
        {
            deduplication_cache_version = 0;
            return conflict_block_ids;
        }
    }

    return {};
}

struct CommitRetryContext
{
    enum Stages
    {
        LOCK_AND_COMMIT,
        RESOLVE_CONFLICTS,
        FILTER_CONFLICTS_AND_RETRY,
        SUCCESS,
        ERROR
    };

    /// Possible ways:

    /// LOCK_AND_COMMIT -> RESOLVE_CONFLICTS
    /// LOCK_AND_COMMIT -> FILTER_CONFLICTS_AND_RETRY
    /// LOCK_AND_COMMIT -> SUCCESS
    /// LOCK_AND_COMMIT -> ERROR

    /// RESOLVE_CONFLICTS -> FILTER_CONFLICTS_AND_RETRY
    /// RESOLVE_CONFLICTS -> ERROR

    Stages stage = LOCK_AND_COMMIT;

    String actual_part_name;
    std::vector<DeduplicationHash> conflict_deduplication_hashes;
};


std::vector<DeduplicationHash> AmateurMergeTreeSink::commitPart(
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    MergeTreeData::MutableDataPartPtr & part,
    const std::vector<DeduplicationHash> & deduplication_hashes,
    const std::vector<String> & deduplication_block_ids)
{
    /// It is possible that we alter a part with different types of source columns.
    /// In this case, if column was not altered, the result type will be different with what we have in metadata.
    /// For now, consider it is ok. See 02461_alter_update_respect_part_column_type_bug for an example.
    ///
    /// metadata_snapshot->check(part->getColumns());
#if CLICKHOUSE_CLOUD
    part->is_prewarmed = true;
#endif

    CommitRetryContext retry_context;

    const auto & settings = context->getSettingsRef();
    ZooKeeperRetriesInfo retries_info = keeper_retries_info.value_or(ZooKeeperRetriesInfo{
        settings[Setting::insert_keeper_max_retries],
        settings[Setting::insert_keeper_retry_initial_backoff_ms],
        settings[Setting::insert_keeper_retry_max_backoff_ms],
        context->getProcessListElement()});
    ZooKeeperRetriesControl retries_ctl(
        "commitPart",
        log,
        retries_info);

    auto resolve_duplicate_stage = [&] () -> CommitRetryContext::Stages
    {
        chassert(!retry_context.conflict_deduplication_hashes.empty());

        /// This block was already written to some replica. Get the part name for it.
        /// Note: race condition with DROP PARTITION operation is possible. User will get "No node" exception and it is Ok.
        auto response = zookeeper->tryGet(getDeduplicationPaths(storage.zookeeper_path, retry_context.conflict_deduplication_hashes));
        for (size_t i = 0; i < retry_context.conflict_deduplication_hashes.size(); ++i)
        {
            auto & deduplication_hash = retry_context.conflict_deduplication_hashes[i];
            const auto & resp = response[i];

            /// If we cannot get the node, then probably it was removed in the meantime. Just skip it then.
            if (resp.error == Coordination::Error::ZNONODE)
                continue;

            const String & part_name = resp.data;
            deduplication_hash.setConflictPartName(part_name);
        }

        return CommitRetryContext::FILTER_CONFLICTS_AND_RETRY;
    };

    auto sleep_before_commit_for_tests = [&] ()
    {
        auto sleep_before_commit_local_part_in_replicated_table_ms = (*storage.getSettings())[MergeTreeSetting::sleep_before_commit_local_part_in_replicated_table_ms];
        if (sleep_before_commit_local_part_in_replicated_table_ms.totalMilliseconds())
        {
            LOG_INFO(log, "committing part {}, triggered sleep_before_commit_local_part_in_replicated_table_ms {}",
                     part->name, sleep_before_commit_local_part_in_replicated_table_ms.totalMilliseconds());
            sleepForMilliseconds(sleep_before_commit_local_part_in_replicated_table_ms.totalMilliseconds());
        }
    };

    auto commit_new_part_stage = [&]() -> CommitRetryContext::Stages
    {
        if (is_attach)
        {
            fiu_do_on(FailPoints::replicated_merge_tree_restore_attach_retry,
            {
                retries_ctl.setUserError(
                    Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Injected read-only error while attaching restored part"));
                return CommitRetryContext::LOCK_AND_COMMIT;
            });
        }

        if (storage.is_readonly)
        {
            /// stop retries if in shutdown
            if (storage.shutdown_prepared_called)
                throw Exception(
                    ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode due to shutdown: replica_path={}", storage.replica_path);

            /// Usually parts should not be attached in read-only mode. So we retry until the table is not read-only.
            /// However there is one case when it's necessary to attach in read-only mode - during execution of the RESTORE REPLICA command.
            if (!allow_attach_while_readonly)
            {
                retries_ctl.setUserError(
                    Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode: replica_path={}", storage.replica_path));
                return CommitRetryContext::LOCK_AND_COMMIT;
            }
        }

        if (is_async_insert)
        {
            /// prefilter by cache
            auto conflicts = detectConflictsInAsyncBlockIDs(deduplication_hashes);
            std::move(conflicts.begin(), conflicts.end(), std::back_inserter(retry_context.conflict_deduplication_hashes));

            if (!retry_context.conflict_deduplication_hashes.empty())
            {
                return CommitRetryContext::RESOLVE_CONFLICTS;
            }
        }

        /// Save the current temporary path and name in case we need to revert the change to retry (ZK connection loss) or in case part is deduplicated.
        const String temporary_part_relative_path = part->getDataPartStorage().getPartDirectory();
        const String initial_part_name = part->name;

        /// Obtain incremental block number and lock it. The lock holds our intention to add the block to the filesystem.
        /// We remove the lock just after renaming the part. In case of exception, block number will be marked as abandoned.
        /// Also, make deduplication check. If a duplicate is detected, no nodes are created.

        /// Allocate new block number and check for duplicates
        auto block_data = serializeCommittingBlockOpToString(CommittingBlock::Op::NewPart);
        auto block_id_pathes = getDeduplicationPaths(storage.zookeeper_path, deduplication_hashes);
        auto block_number_lock = storage.allocateBlockNumber(part->info.getPartitionId(), zookeeper, block_id_pathes, "", block_data); /// 1 RTT

        ThreadFuzzer::maybeInjectSleep();

        {
            std::filesystem::path conflict_path = block_number_lock.getConflictPath();
            if (!conflict_path.empty())
            {
                LOG_DEBUG(log, "Cannot get lock, the conflict path is {}", conflict_path);
                auto conflicted_hash_it = std::find_if(
                    deduplication_hashes.begin(),
                    deduplication_hashes.end(),
                    [&](const DeduplicationHash & hash)
                    {
                        return hash.getBlockId() == conflict_path.filename().string();
                    });
                chassert(conflicted_hash_it != deduplication_hashes.end());
                retry_context.conflict_deduplication_hashes.push_back(*conflicted_hash_it);

                return CommitRetryContext::RESOLVE_CONFLICTS;
            }
        }

        auto block_number = block_number_lock.getNumber();

        /// Set part attributes according to part_number.
        part->info.min_block = block_number;
        part->info.max_block = block_number;

        part->setName(part->getNewName(part->info));
        retry_context.actual_part_name = part->name;

        /// Prepare transaction to ZooKeeper
        /// It will simultaneously add information about the part to all the necessary places in ZooKeeper and remove block_number_lock.
        Coordination::Requests ops;

        /// Deletes the information that the block number is used for writing.
        size_t block_unlock_op_idx = ops.size();
        block_number_lock.getUnlockOp(ops);

        storage.getCommitPartOps(ops, part, block_id_pathes);

        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        MergeTreeData::Transaction transaction(storage, NO_TRANSACTION_RAW); /// If you can not add a part to ZK, we'll remove it back from the working set.
        try
        {
            auto lock = storage.lockParts();
            storage.renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ true);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::DUPLICATE_DATA_PART || e.code() == ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Part with name {} is already written by concurrent request."
                                " It should not happen for non-duplicate data parts because unique names are assigned for them. It's a bug",
                                part->name);
            }

            throw;
        }

        /// Rename parts before committing to ZooKeeper without holding DataPartsLock.
        transaction.renameParts();

        ThreadFuzzer::maybeInjectSleep();

        fiu_do_on(FailPoints::replicated_merge_tree_commit_zk_fail_after_op, { zookeeper->forceFailureAfterOperation(); });

        fiu_do_on(FailPoints::rmt_delay_commit_part, { sleepForSeconds(5); });

        Coordination::Responses responses;
        Coordination::Error multi_code = zookeeper->tryMultiNoThrow(ops, responses, /* check_session_valid */ true); /// 1 RTT
        if (multi_code == Coordination::Error::ZOK)
        {
            part->new_part_was_committed_to_zookeeper_after_rename_on_disk = true;
            sleep_before_commit_for_tests();
            transaction.commit();

            /// Lock nodes have been already deleted, do not delete them in destructor
            block_number_lock.assumeUnlocked();
            return CommitRetryContext::SUCCESS;
        }

        if (Coordination::isHardwareError(multi_code))
        {
            LOG_DEBUG(
                log, "Insert of part {} failed when committing to keeper (Reason: {}). Attempting to recover it", part->name, multi_code);
            ZooKeeperRetriesControl new_retry_controller = retries_ctl;

            /// We are going to try to verify if the transaction was written into keeper
            /// If we fail to do so (keeper unavailable) then we don't know if the changes were applied or not so
            /// we can't delete the local part, as if the changes were applied then inserted block appeared in
            /// `/blocks/`, and it can not be inserted again.
            new_retry_controller.actionAfterLastFailedRetry([&]
            {
                transaction.commit();
                throw Exception(ErrorCodes::UNKNOWN_STATUS_OF_INSERT,
                        "Unknown status of part {} (Reason: {}). Data was written locally but we don't know the status in keeper.",
                        part->name, multi_code);
            });

            bool node_exists = false;
            /// The loop will be executed at least once
            new_retry_controller.retryLoop([&]
            {
                fiu_do_on(FailPoints::replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault, { zookeeper->forceFailureBeforeOperation(); });
                FailPointInjection::pauseFailPoint(FailPoints::replicated_merge_tree_insert_retry_pause);
                zookeeper->setKeeper(storage.getZooKeeper());
                node_exists = zookeeper->exists(fs::path(storage.zookeeper_path) / "parts" / part->name);
            });

            if (node_exists)
            {
                LOG_DEBUG(log, "Insert of part {} recovered from keeper successfully. It will be committed", part->name);
                part->new_part_was_committed_to_zookeeper_after_rename_on_disk = true;
                sleep_before_commit_for_tests();
                transaction.commit();
                block_number_lock.assumeUnlocked();
                return CommitRetryContext::SUCCESS;
            }

            LOG_DEBUG(log, "Insert of part {} was not committed to keeper. Will try again with a new block", part->name);
            /// We checked in keeper and the the data in ops being written so we can retry the process again, but
            /// there is a caveat: as we lost the connection the block number that we got (EphemeralSequential)
            /// might or might not be there (and it belongs to a different session anyway) so we need to assume
            /// it's not there and will be removed automatically, and start from scratch
            /// In order to start from scratch we need to undo the changes that we've done as part of the
            /// transaction: renameTempPartAndAdd
            transaction.rollbackPartsToTemporaryState();
            part->is_temp = true;
            part->setName(initial_part_name);
            part->renameTo(temporary_part_relative_path, false);
            /// Throw an exception to set the proper keeper error and force a retry (if possible)
            zkutil::KeeperMultiException::check(multi_code, ops, responses);
        }

        auto failed_op_idx = zkutil::getFailedOpIndex(multi_code, responses);
        std::filesystem::path failed_op_path = ops[failed_op_idx]->getPath();

        if (multi_code == Coordination::Error::ZNODEEXISTS && !block_id_pathes.empty() && std::ranges::contains(block_id_pathes, failed_op_path.string()))
        {
            /// Block with the same id have just appeared in table (or other replica), rollback the insertion.
            LOG_INFO(log, "Block with ID {} already exists (it was just appeared) for part {}. Ignore it.",
                     failed_op_path, part->name);

            transaction.rollbackPartsToTemporaryState();
            part->is_temp = true;
            part->setName(initial_part_name);
            part->renameTo(temporary_part_relative_path, false);

            auto conflicted_hash_it = std::find_if(
                deduplication_hashes.begin(),
                deduplication_hashes.end(),
                [&](const DeduplicationHash & hash)
                {
                    return hash.getBlockId() == failed_op_path.filename().string();
                });
            chassert(conflicted_hash_it != deduplication_hashes.end());
            retry_context.conflict_deduplication_hashes.push_back(*conflicted_hash_it);

            LOG_TRACE(log, "conflict when committing, the conflict block ids are {}",
                fmt::join(getDeduplicationBlockIds(retry_context.conflict_deduplication_hashes), ", "));
            return CommitRetryContext::RESOLVE_CONFLICTS;
        }

        transaction.rollback();

        if (!Coordination::isUserError(multi_code))
            throw Exception(
                    ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR,
                    "Unexpected ZooKeeper error while adding block {} with ID '{}': {}",
                    block_number,
                    fmt::join(deduplication_block_ids, ", "),
                    multi_code);

        if (multi_code == Coordination::Error::ZNONODE && failed_op_idx == block_unlock_op_idx)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                            "Insert query (for block {}) was canceled by concurrent ALTER PARTITION or TRUNCATE",
                            block_number_lock.getPath());

        if (multi_code == Coordination::Error::ZNODEEXISTS && failed_op_path == quorum_info.status_path)
            throw Exception(ErrorCodes::UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE,
                            "Another quorum insert has been already started");

        throw Exception(
                ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR,
                "Unexpected logical error while adding block {} with ID '{}': {}, path {}",
                block_number,
                fmt::join(deduplication_block_ids, ", "),
                multi_code,
                failed_op_path);
    };

    auto stage_switcher = [&] ()
    {
        try
        {
            switch (retry_context.stage)
            {
                case CommitRetryContext::LOCK_AND_COMMIT:
                    retry_context.stage = commit_new_part_stage();
                    break;
                case CommitRetryContext::RESOLVE_CONFLICTS:
                    retry_context.stage = resolve_duplicate_stage();
                    break;
                case CommitRetryContext::FILTER_CONFLICTS_AND_RETRY:
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Operation is already has a result.");
                case CommitRetryContext::SUCCESS:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Operation is already succeed.");
                case CommitRetryContext::ERROR:
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Operation is already in error state.");
            }
        }
        catch (const zkutil::KeeperException &)
        {
            throw;
        }
        catch (DB::Exception &)
        {
            retry_context.stage = CommitRetryContext::ERROR;
            throw;
        }

        return retry_context.stage;
    };

    retries_ctl.retryLoop([&]()
    {
        zookeeper->setKeeper(storage.getZooKeeper());

        while (true)
        {
            const auto prev_stage = retry_context.stage;

            stage_switcher();

            if (prev_stage == retry_context.stage)
            {
                /// trigger next retry in retries_ctl.retryLoop when stage has not changed
                return;
            }

            if (retry_context.stage == CommitRetryContext::SUCCESS
                || retry_context.stage == CommitRetryContext::FILTER_CONFLICTS_AND_RETRY)
            {
                /// operation is done
                return;
            }
        }
    });

    if (retry_context.stage == CommitRetryContext::FILTER_CONFLICTS_AND_RETRY)
        chassert(!retry_context.conflict_deduplication_hashes.empty());

    if (retry_context.stage == CommitRetryContext::SUCCESS)
        chassert(retry_context.conflict_deduplication_hashes.empty());

    return retry_context.conflict_deduplication_hashes;
}

void AmateurMergeTreeSink::onStart()
{
    /// It's only allowed to throw "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded(&storage.partial_shutdown_event, context, true);

    auto component_guard = Coordination::setCurrentComponent("AmateurMergeTreeSink::onStart");
    ZooKeeperWithFaultInjectionPtr zookeeper = createKeeper("AmateurMergeTreeSink::onStart");
}

void AmateurMergeTreeSink::onFinish()
{
    if (isCancelled())
        return;

    ZooKeeperWithFaultInjectionPtr zookeeper = createKeeper("AmateurMergeTreeSink::onFinish");
    auto component_guard = Coordination::setCurrentComponent("AmateurMergeTreeSink::onFinish");
    finishDelayed(zookeeper);
}

ZooKeeperWithFaultInjectionPtr AmateurMergeTreeSink::createKeeper(String name)
{
    const auto & settings = context->getSettingsRef();
    return ZooKeeperWithFaultInjection::createInstance(
        static_cast<double>(settings[Setting::insert_keeper_fault_injection_probability]),
        settings[Setting::insert_keeper_fault_injection_seed],
        storage.getZooKeeper(),
        name,
        log);
}
}
