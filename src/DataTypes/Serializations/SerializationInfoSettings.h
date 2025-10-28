#pragma once

#include <string>
#include <Core/SettingsEnums.h>

namespace DB
{

struct SerializationInfoSettings
{
    double ratio_of_defaults_for_sparse = 1.0;
    bool choose_kind = false;

    MergeTreeSerializationInfoVersion version = MergeTreeSerializationInfoVersion::BASIC;
    MergeTreeStringSerializationVersion string_serialization_version = MergeTreeStringSerializationVersion::SINGLE_STREAM;

    std::set<std::string> rle_columns;
    bool rle_columns_all = false;

    SerializationInfoSettings() = default;

    SerializationInfoSettings(
        double ratio_of_defaults_for_sparse_,
        bool choose_kind_,
        MergeTreeSerializationInfoVersion version_,
        MergeTreeStringSerializationVersion string_serialization_version_,
        std::string rle_columns_);

    bool isAlwaysDefault() const { return ratio_of_defaults_for_sparse >= 1.0 && !rle_columns_all && rle_columns.empty(); }
};

}
