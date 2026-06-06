#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/ZooKeeper/ZooKeeper.h>

#include <memory>

namespace DB
{

/** Stores a flat list of directory-to-blob-name mappings in Keeper.
  * File operations are proxied directly to object storage.
  *
  * Keeper layout:
  *   <keeper_prefix>/metadata/<random_name>  data: <normalized_logical_path>  (e.g., "/foo/bar/")
  *
  * Object storage layout:
  *   <common_key_prefix>/<random_name>/<file_name>
  */
class MetadataStorageFromAmateurObjectStorage final : public IMetadataStorage
{
    friend class MetadataStorageFromAmateurObjectStorageTransaction;

public:
    MetadataStorageFromAmateurObjectStorage(
        ObjectStoragePtr object_storage_,
        std::string storage_path_prefix_,
        zkutil::ZooKeeperPtr zookeeper_,
        std::string keeper_prefix_);

    MetadataStorageType getType() const override { return MetadataStorageType::Amateur; }
    const std::string & getPath() const override { return storage_path_full; }
    uint32_t getHardlinkCount(const std::string &) const override { return 0; }
    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
    bool isReadOnly() const override { return false; }
    bool areBlobPathsRandom() const override { return false; }
    bool isPlain() const override { return true; }
    bool isWriteOnce() const override { return false; }

    MetadataTransactionPtr createTransaction() override;

    void dropCache() override { }
    void refresh(UInt64) override { }

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const std::string & path) const override;
    std::optional<uint64_t> getFileSizeIfExists(const std::string & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;
    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;
    std::optional<Poco::Timestamp> getLastModifiedIfExists(const std::string & path) const override;

private:
    std::optional<std::string> getDirectoryRemotePath(const std::string & normalized_directory_path) const;
    std::optional<std::pair<std::string, std::string>> resolveFileMapping(const std::string & path) const;
    std::string constructFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const;

    const std::shared_ptr<IObjectStorage> object_storage;
    const std::string storage_path_prefix;
    const std::string storage_path_full;
    const zkutil::ZooKeeperPtr zookeeper;
    const std::string keeper_prefix;
    const std::string common_key_prefix;

    mutable std::mutex zk_mutex;
};

class MetadataStorageFromAmateurObjectStorageTransaction : public IMetadataTransaction
{
public:
    explicit MetadataStorageFromAmateurObjectStorageTransaction(MetadataStorageFromAmateurObjectStorage & metadata_storage_);

    bool supportsChmod() const override { return false; }
    void setLastModified(const String &, const Poco::Timestamp &) override { }
    void setReadOnly(const std::string &) override { }

    void commit(const TransactionCommitOptionsVariant & options) override;
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override;

    void createMetadataFile(const std::string & path, const StoredObjects & objects) override;
    void createDirectory(const std::string & path) override;
    void createDirectoryRecursive(const std::string & path) override;
    void moveDirectory(const std::string & path_from, const std::string & path_to) override;

    void unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects) override;
    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects) override;

    void createHardLink(const std::string & path_from, const std::string & path_to) override;
    void moveFile(const std::string & path_from, const std::string & path_to) override;
    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    StoredObjects getSubmittedForRemovalBlobs() override;

protected:
    MetadataStorageFromAmateurObjectStorage & metadata_storage;
    StoredObjects removed_objects;
};

}
