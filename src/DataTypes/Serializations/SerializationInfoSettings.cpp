#include <DataTypes/Serializations/SerializationInfoSettings.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB
{

SerializationInfoSettings::SerializationInfoSettings(
        double ratio_of_defaults_for_sparse_,
        bool choose_kind_,
        MergeTreeSerializationInfoVersion version_,
        MergeTreeStringSerializationVersion string_serialization_version_,
        std::string rle_columns_)
        : ratio_of_defaults_for_sparse(ratio_of_defaults_for_sparse_)
        , choose_kind(choose_kind_)
        , version(version_)
        , string_serialization_version(string_serialization_version_)
    {
        /// New string_serialization_version is valid only when using MergeTreeSerializationInfoVersion::WITH_TYPES.
        /// For older versions, it is automatically defaulted to preserve compatibility.
        if (version < MergeTreeSerializationInfoVersion::WITH_TYPES)
            string_serialization_version = MergeTreeStringSerializationVersion::SINGLE_STREAM;

        if (rle_columns_ == "ALL")
        {
            rle_columns_all = true;
        }
        else
        {
            std::vector<std::string> splitted_rle_columns;
            boost::split(splitted_rle_columns, rle_columns_, [](char c) { return c == ','; });
            for (auto & column : splitted_rle_columns)
            {
                boost::trim(column);
                rle_columns.insert(column);
            }
        }
    }

}
