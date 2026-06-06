#include <Disks/DiskObjectStorage/MetadataStorages/Amateur/MetadataStorageFromAmateurObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/ZooKeeper/Types.h>
#include <Common/escapeForFileName.h>

#include <Common/Exception.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int DIRECTORY_DOESNT_EXIST;
}

namespace fs = std::filesystem;

/// Sentinel used as the encoded path for the root directory.
static constexpr auto ROOT_ENCODED = "__root__";

/// Normalize a directory path to the form stored in Keeper:
/// always starts with '/' and ends with '/', except for root which is "".
static std::string normalizeDirPath(const std::string & path)
{
    auto normalized = normalizePath(path);
    std::string result = normalized.string();
    if (result.empty())
        return result;
    if (!result.starts_with('/'))
        result.insert(result.begin(), '/');
    if (!result.ends_with('/'))
        result.push_back('/');
    return result;
}

static std::string encodePathForKeeper(const std::string & path)
{
    if (path.empty())
        return ROOT_ENCODED;
    return escapeForFileName(path);
}

static std::string decodePathFromKeeper(const std::string & encoded)
{
    if (encoded == ROOT_ENCODED)
        return "";
    return unescapeForFileName(encoded);
}

MetadataStorageFromAmateurObjectStorage::MetadataStorageFromAmateurObjectStorage(
    ObjectStoragePtr object_storage_,
    std::string storage_path_prefix_,
    zkutil::ZooKeeperPtr zookeeper_,
    std::string keeper_prefix_)
    : object_storage(std::move(object_storage_))
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
    , zookeeper(std::move(zookeeper_))
    , keeper_prefix(std::move(keeper_prefix_))
    , common_key_prefix(object_storage->getCommonKeyPrefix())
{
    auto metadata_zk_path = keeper_prefix + "/metadata";
    zookeeper->createAncestors(metadata_zk_path);
    zookeeper->createIfNotExists(metadata_zk_path, "");

    auto root_zk_path = metadata_zk_path + "/" + ROOT_ENCODED;
    auto remote = getRandomASCIIString(32);
    zookeeper->createIfNotExists(root_zk_path, remote);
}

MetadataTransactionPtr MetadataStorageFromAmateurObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromAmateurObjectStorageTransaction>(*this);
}

std::optional<std::string> MetadataStorageFromAmateurObjectStorage::getDirectoryRemotePath(
    const std::string & normalized_directory_path) const
{
    auto encoded = encodePathForKeeper(normalized_directory_path);
    auto zk_path = keeper_prefix + "/metadata/" + encoded;
    String data;
    if (zookeeper->tryGet(zk_path, data))
        return data;
    return std::nullopt;
}

std::optional<std::pair<std::string, std::string>> MetadataStorageFromAmateurObjectStorage::resolveFileMapping(
    const std::string & path) const
{
    auto normalized = normalizePath(path);
    auto parent = normalizeDirPath(normalized.parent_path().string());

    auto remote = getDirectoryRemotePath(parent);
    if (!remote)
        return std::nullopt;

    return std::make_pair(std::move(*remote), normalized.filename().string());
}

std::string MetadataStorageFromAmateurObjectStorage::constructFileObjectKey(
    const std::string & directory_remote_path,
    const std::string & file_name) const
{
    return fs::path(common_key_prefix) / directory_remote_path / file_name;
}

bool MetadataStorageFromAmateurObjectStorage::existsFile(const std::string & path) const
{
    auto mapping = resolveFileMapping(path);
    if (!mapping)
        return false;

    auto object_key = constructFileObjectKey(mapping->first, mapping->second);
    try
    {
        return object_storage->exists(StoredObject(object_key));
    }
    catch (...)
    {
        return false;
    }
}

bool MetadataStorageFromAmateurObjectStorage::existsDirectory(const std::string & path) const
{
    auto dir_path = normalizeDirPath(path);
    if (dir_path.empty())
        return true;

    auto encoded = encodePathForKeeper(dir_path);
    auto zk_path = keeper_prefix + "/metadata/" + encoded;
    return zookeeper->exists(zk_path);
}

bool MetadataStorageFromAmateurObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    return existsFile(path) || existsDirectory(path);
}

uint64_t MetadataStorageFromAmateurObjectStorage::getFileSize(const std::string & path) const
{
    auto size = getFileSizeIfExists(path);
    if (size)
        return *size;

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
}

std::optional<uint64_t> MetadataStorageFromAmateurObjectStorage::getFileSizeIfExists(const std::string & path) const
{
    auto mapping = resolveFileMapping(path);
    if (!mapping)
        return std::nullopt;

    auto object_key = constructFileObjectKey(mapping->first, mapping->second);
    try
    {
        auto metadata = object_storage->getObjectMetadata(object_key, false);
        return metadata.size_bytes;
    }
    catch (...)
    {
        return std::nullopt;
    }
}

std::vector<std::string> MetadataStorageFromAmateurObjectStorage::listDirectory(const std::string & path) const
{
    auto target = normalizeDirPath(path);

    std::vector<std::string> result;

    auto metadata_zk_path = keeper_prefix + "/metadata";
    Strings children;
    try
    {
        children = zookeeper->getChildren(metadata_zk_path);
    }
    catch (...)
    {
        return result;
    }

    for (const auto & encoded : children)
    {
        try
        {
            auto decoded = decodePathFromKeeper(encoded);
            if (!decoded.starts_with(target) || decoded == target)
                continue;

            auto rest = decoded.substr(target.size());
            auto slash_pos = rest.find('/');
            auto subdir = (slash_pos == std::string::npos) ? rest : rest.substr(0, slash_pos);
            if (!subdir.empty() && (result.empty() || result.back() != subdir))
                result.push_back(std::move(subdir));
        }
        catch (...)
        {
        }
    }

    auto remote = getDirectoryRemotePath(target);
    if (remote)
    {
        auto dir_key = fs::path(common_key_prefix) / *remote / "";
        try
        {
            for (auto it = object_storage->iterate(dir_key, 0, false, std::nullopt); it->isValid(); it->next())
            {
                auto file_name = fs::path(it->current()->getPath()).filename().string();
                if (!file_name.empty())
                    result.push_back(std::move(file_name));
            }
        }
        catch (...)
        {
        }
    }

    return result;
}

DirectoryIteratorPtr MetadataStorageFromAmateurObjectStorage::iterateDirectory(const std::string & path) const
{
    auto paths = listDirectory(path);
    std::for_each(paths.begin(), paths.end(), [&](auto & child) { child = fs::path(path) / child; });
    std::vector<fs::path> fs_paths(paths.begin(), paths.end());
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

StoredObjects MetadataStorageFromAmateurObjectStorage::getStorageObjects(const std::string & path) const
{
    auto objects = getStorageObjectsIfExist(path);
    if (objects)
        return std::move(*objects);

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
}

std::optional<StoredObjects> MetadataStorageFromAmateurObjectStorage::getStorageObjectsIfExist(const std::string & path) const
{
    auto mapping = resolveFileMapping(path);
    if (!mapping)
        return std::nullopt;

    auto object_key = constructFileObjectKey(mapping->first, mapping->second);
    try
    {
        auto metadata = object_storage->getObjectMetadata(object_key, false);
        return StoredObjects{StoredObject(object_key, path, metadata.size_bytes)};
    }
    catch (...)
    {
        return std::nullopt;
    }
}

Poco::Timestamp MetadataStorageFromAmateurObjectStorage::getLastModified(const std::string & path) const
{
    auto ts = getLastModifiedIfExists(path);
    if (ts)
        return *ts;

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File or directory {} does not exist", path);
}

std::optional<Poco::Timestamp> MetadataStorageFromAmateurObjectStorage::getLastModifiedIfExists(const String & path) const
{
    if (existsDirectory(path))
        return Poco::Timestamp::fromEpochTime(0);

    auto mapping = resolveFileMapping(path);
    if (!mapping)
        return std::nullopt;

    auto object_key = constructFileObjectKey(mapping->first, mapping->second);
    try
    {
        auto metadata = object_storage->getObjectMetadata(object_key, false);
        return metadata.last_modified;
    }
    catch (...)
    {
        return std::nullopt;
    }
}

// ---- Transaction ----

MetadataStorageFromAmateurObjectStorageTransaction::MetadataStorageFromAmateurObjectStorageTransaction(
    MetadataStorageFromAmateurObjectStorage & metadata_storage_)
    : metadata_storage(metadata_storage_)
{
}

void MetadataStorageFromAmateurObjectStorageTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();
}

TransactionCommitOutcomeVariant MetadataStorageFromAmateurObjectStorageTransaction::tryCommit(
    const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Metadata storage from amateur supports only tryCommit without options");

    commit(NoCommitOptions{});
    return true;
}

void MetadataStorageFromAmateurObjectStorageTransaction::createMetadataFile(
    const std::string & /*path*/, const StoredObjects & /*objects*/)
{
}

void MetadataStorageFromAmateurObjectStorageTransaction::createDirectory(const std::string & path)
{
    auto dir_path = normalizeDirPath(path);
    auto encoded = encodePathForKeeper(dir_path);
    auto zk_path = metadata_storage.keeper_prefix + "/metadata/" + encoded;

    if (metadata_storage.zookeeper->exists(zk_path))
        return;

    auto remote = getRandomASCIIString(32);
    metadata_storage.zookeeper->create(zk_path, remote, zkutil::CreateMode::Persistent);
}

void MetadataStorageFromAmateurObjectStorageTransaction::createDirectoryRecursive(const std::string & path)
{
    auto dir_path = normalizeDirPath(path);

    if (!dir_path.starts_with('/'))
        dir_path.insert(dir_path.begin(), '/');

    /// Walk down the path creating each parent as needed.
    /// The root directory ("/") corresponds to an empty normalized path,
    /// which is encoded as ROOT_ENCODED. Start at pos=0 to catch it.
    size_t pos = 0;
    while (true)
    {
        pos = dir_path.find('/', pos);
        if (pos == std::string::npos)
            break;
        auto parent = normalizeDirPath(dir_path.substr(0, pos));
        auto encoded = encodePathForKeeper(parent);
        auto zk_path = metadata_storage.keeper_prefix + "/metadata/" + encoded;
        if (!metadata_storage.zookeeper->exists(zk_path))
        {
            auto remote = getRandomASCIIString(32);
            metadata_storage.zookeeper->create(zk_path, remote, zkutil::CreateMode::Persistent);
        }
        ++pos;
    }
}

void MetadataStorageFromAmateurObjectStorageTransaction::moveDirectory(
    const std::string & path_from, const std::string & path_to)
{
    auto from_str = normalizeDirPath(path_from);
    auto to_str = normalizeDirPath(path_to);

    if (from_str.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't move root folder");

    auto metadata_zk_path = metadata_storage.keeper_prefix + "/metadata";
    Strings children;
    try
    {
        children = metadata_storage.zookeeper->getChildren(metadata_zk_path);
    }
    catch (...)
    {
        return;
    }

    Coordination::Requests ops;
    for (const auto & encoded : children)
    {
        try
        {
            auto decoded = decodePathFromKeeper(encoded);

            if (decoded != from_str && !decoded.starts_with(from_str))
                continue;

            auto new_path = to_str + decoded.substr(from_str.size());
            auto new_encoded = encodePathForKeeper(new_path);
            auto old_zk_path = metadata_zk_path + "/" + encoded;
            auto new_zk_path = metadata_zk_path + "/" + new_encoded;

            /// Read the remote (blob prefix) from the old node.
            String remote = metadata_storage.zookeeper->get(old_zk_path);

            ops.push_back(zkutil::makeCreateRequest(new_zk_path, remote, zkutil::CreateMode::Persistent));
            ops.push_back(zkutil::makeRemoveRequest(old_zk_path, -1));
        }
        catch (...)
        {
        }
    }

    if (!ops.empty())
        metadata_storage.zookeeper->multi(ops);
}

void MetadataStorageFromAmateurObjectStorageTransaction::unlinkFile(
    const std::string & path, bool if_exists, bool should_remove_objects)
{
    auto mapping = metadata_storage.resolveFileMapping(path);
    if (!mapping)
    {
        if (if_exists)
            return;
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
    }

    auto object_key = metadata_storage.constructFileObjectKey(mapping->first, mapping->second);

    if (should_remove_objects)
    {
        metadata_storage.object_storage->removeObjectIfExists(StoredObject(object_key));
        removed_objects.emplace_back(object_key);
    }
}

void MetadataStorageFromAmateurObjectStorageTransaction::removeDirectory(const std::string & path)
{
    auto dir_path = normalizeDirPath(path);
    if (dir_path.empty())
        return;

    auto encoded = encodePathForKeeper(dir_path);
    auto zk_path = metadata_storage.keeper_prefix + "/metadata/" + encoded;

    if (!metadata_storage.zookeeper->exists(zk_path))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path);

    metadata_storage.zookeeper->tryRemove(zk_path);
}

void MetadataStorageFromAmateurObjectStorageTransaction::removeRecursive(
    const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    auto dir_path = normalizeDirPath(path);
    if (dir_path.empty())
        return;

    auto metadata_zk_path = metadata_storage.keeper_prefix + "/metadata";
    Strings children;
    try
    {
        children = metadata_storage.zookeeper->getChildren(metadata_zk_path);
    }
    catch (...)
    {
        return;
    }

    for (const auto & encoded : children)
    {
        try
        {
            auto decoded = decodePathFromKeeper(encoded);
            if (decoded != dir_path && !decoded.starts_with(dir_path))
                continue;

            if (!should_remove_objects || should_remove_objects(decoded))
            {
                String remote = metadata_storage.zookeeper->get(metadata_zk_path + "/" + encoded);

                auto dir_key = fs::path(metadata_storage.common_key_prefix) / remote / "";
                StoredObjects objects_to_remove;
                for (auto it = metadata_storage.object_storage->iterate(dir_key, 0, false, std::nullopt); it->isValid(); it->next())
                {
                    auto file_key = it->current()->getPath();
                    objects_to_remove.emplace_back(file_key);
                }
                metadata_storage.object_storage->removeObjectsIfExist(objects_to_remove);
                removed_objects.append_range(objects_to_remove);

                metadata_storage.zookeeper->tryRemove(metadata_zk_path + "/" + encoded);
            }
        }
        catch (...)
        {
        }
    }
}

void MetadataStorageFromAmateurObjectStorageTransaction::createHardLink(
    const std::string & path_from, const std::string & path_to)
{
    auto mapping_from = metadata_storage.resolveFileMapping(path_from);
    if (!mapping_from)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File '{}' does not exist", path_from);

    auto mapping_to = metadata_storage.resolveFileMapping(path_to);
    if (!mapping_to)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory for '{}' does not exist", path_to);

    auto src_key = metadata_storage.constructFileObjectKey(mapping_from->first, mapping_from->second);
    auto dst_key = metadata_storage.constructFileObjectKey(mapping_to->first, mapping_to->second);

    metadata_storage.object_storage->copyObject(
        StoredObject(src_key),
        StoredObject(dst_key),
        getReadSettings(),
        getWriteSettings());
}

void MetadataStorageFromAmateurObjectStorageTransaction::moveFile(
    const std::string & path_from, const std::string & path_to)
{
    auto mapping_from = metadata_storage.resolveFileMapping(path_from);
    if (!mapping_from)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File '{}' does not exist", path_from);

    auto mapping_to = metadata_storage.resolveFileMapping(path_to);
    if (!mapping_to)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory for '{}' does not exist", path_to);

    auto src_key = metadata_storage.constructFileObjectKey(mapping_from->first, mapping_from->second);
    auto dst_key = metadata_storage.constructFileObjectKey(mapping_to->first, mapping_to->second);

    metadata_storage.object_storage->copyObject(
        StoredObject(src_key),
        StoredObject(dst_key),
        getReadSettings(),
        getWriteSettings());

    metadata_storage.object_storage->removeObjectIfExists(StoredObject(src_key));
    removed_objects.emplace_back(src_key);
}

void MetadataStorageFromAmateurObjectStorageTransaction::replaceFile(
    const std::string & path_from, const std::string & path_to)
{
    auto mapping_from = metadata_storage.resolveFileMapping(path_from);
    if (!mapping_from)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File '{}' does not exist", path_from);

    auto mapping_to = metadata_storage.resolveFileMapping(path_to);
    if (!mapping_to)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory for '{}' does not exist", path_to);

    auto src_key = metadata_storage.constructFileObjectKey(mapping_from->first, mapping_from->second);
    auto dst_key = metadata_storage.constructFileObjectKey(mapping_to->first, mapping_to->second);

    metadata_storage.object_storage->copyObject(
        StoredObject(src_key),
        StoredObject(dst_key),
        getReadSettings(),
        getWriteSettings());

    metadata_storage.object_storage->removeObjectIfExists(StoredObject(src_key));
    removed_objects.emplace_back(src_key);
}

ObjectStorageKey MetadataStorageFromAmateurObjectStorageTransaction::generateObjectKeyForPath(const std::string & path)
{
    auto normalized = normalizePath(path);
    auto parent = normalizeDirPath(normalized.parent_path().string());

    auto remote = metadata_storage.getDirectoryRemotePath(parent);
    if (!remote)
    {
        createDirectoryRecursive(parent);
        remote = metadata_storage.getDirectoryRemotePath(parent);
        if (!remote)
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", parent);
    }

    auto object_key = metadata_storage.constructFileObjectKey(*remote, normalized.filename().string());
    return ObjectStorageKey::createAsAbsolute(object_key);
}

StoredObjects MetadataStorageFromAmateurObjectStorageTransaction::getSubmittedForRemovalBlobs()
{
    return removed_objects;
}

}
