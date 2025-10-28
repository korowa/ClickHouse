#include <cstddef>
#include <Columns/ColumnRLE.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationRLE.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

struct DeserializeStateRLE : public ISerialization::DeserializeBinaryBulkState
{
    /// Flag indicating that there is incomplete run to deserialize
    bool run_in_progress = false;
    /// The number of remaining rows to emit
    size_t run_length_remaining = 0;
    /// If the run value should be emitted to `values` columns
    bool serialize_run_value = false;
    /// The value for in-progress run
    Field run_value;

    ISerialization::DeserializeBinaryBulkStatePtr nested;

    void reset()
    {
        run_length_remaining = 0;
        serialize_run_value = false;
        run_in_progress = false;
    }

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeStateRLE>(*this);
        new_state->nested = nested ? nested->clone() : nullptr;
        return new_state;
    }
};

void serializeOffsets(const IColumn::Offsets & offsets, WriteBuffer & ostr, size_t start)
{
    for (const auto offset : offsets)
    {
        size_t group_size = offset + 1 - start;
        writeVarUInt(group_size, ostr);
        start = offset + 1;
    }
}


/// Returns number of read rows.
size_t deserializeOffsets(
    IColumn::Offsets & offsets,
    ReadBuffer & istr,
    size_t start,
    size_t offset,
    size_t limit,
    size_t & skipped_runs,
    DeserializeStateRLE & state)
{
    size_t rows_read = 0;

    if (state.run_in_progress)
    {
        // Skip remaining run if it's less than offset, and proceed to reading stream
        if (offset >= state.run_length_remaining)
        {
            offset -= state.run_length_remaining;
            state.run_in_progress = false;
            state.run_length_remaining = 0;
        }
        // Otherwise, consume at most `limit` from remaining run
        else
        {
            state.serialize_run_value = true;
            size_t rows_available = state.run_length_remaining - offset;
            size_t rows_to_read = std::min(rows_available, limit);

            rows_read += rows_to_read;
            offsets.push_back(start + rows_read - 1);
            offset = 0;

            // If the run is still not read to the end, keep it as remaining
            if (rows_to_read < rows_available)
            {
                state.run_in_progress = true;
                state.run_length_remaining = rows_available - rows_to_read;
                return rows_read;
            }
            // Otherwise complete current run and proceed to the following one
            else
            {
                state.run_in_progress = false;
                state.run_length_remaining = 0;
            }
        }
    }

    if (rows_read == limit)
        return rows_read;

    while (!istr.eof())
    {
        size_t group_size;
        readVarUInt(group_size, istr);

        // Skip the whole run if it hasn't reached offset
        if (offset >= group_size)
        {
            offset -= group_size;
            skipped_runs++;
            continue;
        }

        // Otherwise, consume at most `limit` rows
        size_t rows_available = group_size - offset;
        size_t rows_to_read = std::min(rows_available, limit - rows_read);

        rows_read += rows_to_read;
        offsets.push_back(start + rows_read - 1);
        offset = 0;

        // If the run is still not read till the end, keep it as remaining
        if (rows_to_read < rows_available)
        {
            state.run_in_progress = true;
            state.run_length_remaining = rows_available - rows_to_read;
            break;
        }

        if (rows_read == limit)
            break;
    }

    return rows_read;
}

void extractRunEnds(IColumn::Offsets & indices, const IColumn & column, size_t offset, size_t limit)
{
    if (offset >= column.size())
        return;

    size_t range_end = column.size();
    if (limit && offset + limit < range_end)
        range_end = offset + limit;

    if (offset >= range_end)
        return;

    size_t run_start_index = offset;
    for (size_t i = offset + 1; i < range_end; ++i)
    {
        if (column.compareAt(i, run_start_index, column, 1) != 0)
        {
            indices.push_back(i - 1);
            run_start_index = i;
        }
    }

    indices.push_back(range_end - 1);
}

void extractRunEnds(IColumn::Offsets & indices, const ColumnRLE & column, size_t offset, size_t limit)
{
    const size_t size = column.size();
    if (offset >= size)
        return;

    size_t range_end;
    if (limit && offset + limit < size)
        range_end = offset + limit;
    else
        range_end = size;

    if (offset >= range_end)
        return;

    for (const auto run_end_offset : column.getOffsetsData())
    {
        if (run_end_offset < offset)
            continue;

        if (run_end_offset >= range_end - 1)
        {
            indices.push_back(range_end - 1);
            return;
        }
        else
        {
            indices.push_back(run_end_offset);
        }
    }
}

}

SerializationRLE::SerializationRLE(const SerializationPtr & nested_)
    : nested(nested_)
{
}

ISerialization::KindStack SerializationRLE::getKindStack() const
{
    auto kind_stack = nested->getKindStack();
    kind_stack.push_back(Kind::RLE);
    return kind_stack;
}

SerializationPtr SerializationRLE::SubcolumnCreator::create(const SerializationPtr & prev, const DataTypePtr &) const
{
    return std::make_shared<SerializationRLE>(prev);
}

ColumnPtr SerializationRLE::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnRLE::create(prev, offsets);
}

void SerializationRLE::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    const auto * column_rle = data.column ? typeid_cast<const ColumnRLE *>(data.column.get()) : nullptr;

    settings.path.push_back(Substream::RLESizes);
    auto offsets_data = SubstreamData(std::make_shared<SerializationNumber<UInt64>>())
                            .withType(data.type ? std::make_shared<DataTypeUInt64>() : nullptr)
                            .withColumn(column_rle ? column_rle->getOffsetsPtr() : nullptr)
                            .withSerializationInfo(data.serialization_info);

    settings.path.back().data = offsets_data;
    callback(settings.path);

    settings.path.back() = Substream::RLEValues;
    settings.path.back().creator = std::make_shared<SubcolumnCreator>(offsets_data.column);
    settings.path.back().data = data;

    auto next_data = SubstreamData(nested)
                         .withType(data.type)
                         .withColumn(column_rle ? column_rle->getValuesPtr() : data.column)
                         .withSerializationInfo(data.serialization_info);

    nested->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationRLE::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::RLEValues);
    if (const auto * column_rle = typeid_cast<const ColumnRLE *>(&column))
        nested->serializeBinaryBulkStatePrefix(column_rle->getValuesColumn(), settings, state);
    else
        nested->serializeBinaryBulkStatePrefix(column, settings, state);

    settings.path.pop_back();
}

void SerializationRLE::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    auto offsets_column = DataTypeNumber<IColumn::Offset>().createColumn();
    auto & offsets_data = assert_cast<ColumnVector<IColumn::Offset> &>(*offsets_column).getData();

    if (const auto * column_rle = typeid_cast<const ColumnRLE *>(&column))
        extractRunEnds(offsets_data, *column_rle, offset, limit);
    else
        extractRunEnds(offsets_data, column, offset, limit);

    settings.path.push_back(Substream::RLESizes);
    if (auto * stream = settings.getter(settings.path))
        serializeOffsets(offsets_data, *stream, offset);

    settings.path.back() = Substream::RLEValues;
    if (!offsets_data.empty())
    {
        if (const auto * column_rle = typeid_cast<const ColumnRLE *>(&column))
        {
            const auto & values = column_rle->getValuesColumn();

            size_t begin = column_rle->getValueIndex(offsets_data[0]);
            size_t end = column_rle->getValueIndex(offsets_data.back());

            nested->serializeBinaryBulkWithMultipleStreams(values, begin, end - begin + 1, settings, state);
        }
        else
        {
            auto values = column.index(*offsets_column, 0);
            nested->serializeBinaryBulkWithMultipleStreams(*values, 0, values->size(), settings, state);
        }
    }
    else
    {
        auto empty_column = column.cloneEmpty()->convertToFullColumnIfSparse();
        nested->serializeBinaryBulkWithMultipleStreams(*empty_column, 0, 0, settings, state);
    }

    settings.path.pop_back();
}

void SerializationRLE::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::RLEValues);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationRLE::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto state_rle = std::make_shared<DeserializeStateRLE>();

    settings.path.push_back(Substream::RLEValues);
    nested->deserializeBinaryBulkStatePrefix(settings, state_rle->nested, cache);
    settings.path.pop_back();

    state = std::move(state_rle);
}

void SerializationRLE::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * state_rle = checkAndGetState<DeserializeStateRLE>(state);

    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
        return;

    if (!settings.continuous_reading)
        state_rle->reset();

    auto mutable_column = column->assumeMutable();
    auto & column_rle = assert_cast<ColumnRLE &>(*mutable_column);

    size_t old_size = 0;
    size_t skipped_values_rows = 0;
    settings.path.push_back(Substream::RLESizes);

    const auto * cached_element = getElementFromSubstreamsCache(cache, settings.path);
    if (cached_element)
    {
        const auto & cached_offsets_element = assert_cast<const SubstreamsCacheRLEOffsetsElement &>(*cached_element);
        column_rle.getOffsetsPtr() = cached_offsets_element.offsets;
        old_size = cached_offsets_element.old_size;
        skipped_values_rows = cached_offsets_element.skipped_values_rows;
    }
    else
    {
        if (auto * stream = settings.getter(settings.path))
        {
            auto & offsets_data = column_rle.getOffsetsData();
            old_size = offsets_data.size();
            deserializeOffsets(offsets_data, *stream, column_rle.size(), rows_offset, limit, skipped_values_rows, *state_rle);

            addElementToSubstreamsCache(
                cache,
                settings.path,
                std::make_unique<SubstreamsCacheRLEOffsetsElement>(
                    column_rle.getOffsetsPtr(), old_size, /*read_rows*/ skipped_values_rows));
        }
    }

    auto & offsets_data = column_rle.getOffsetsData();
    auto & values_column = column_rle.getValuesPtr();
    size_t values_limit = offsets_data.size() - old_size;

    settings.path.back() = Substream::RLEValues;
    {
        if (state_rle->serialize_run_value)
        {
            column_rle.getValuesColumn().insert(state_rle->run_value);
            state_rle->serialize_run_value = false;
            values_limit -= 1;
        }
        nested->deserializeBinaryBulkWithMultipleStreams(
            values_column, skipped_values_rows, values_limit, settings, state_rle->nested, cache);

        if (state_rle->run_in_progress)
            values_column->get(values_column->size() - 1, state_rle->run_value);
    }
    settings.path.pop_back();

    if (offsets_data.size() != values_column->size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Inconsistent sizes of values and offsets in SerializationRLE."
            " Offsets size: {}, values size: {}",
            offsets_data.size(),
            values_column->size());

    column = std::move(mutable_column);

    // addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column, column->size() - prev_size);
}

/// All methods below just wrap nested serialization.

void SerializationRLE::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(field, ostr, settings);
}

void SerializationRLE::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(field, istr, settings);
}

template <typename Reader>
void SerializationRLE::deserialize(IColumn & column, Reader && reader) const
{
    auto & column_rle = assert_cast<ColumnRLE &>(column);
    auto & values = column_rle.getValuesColumn();

    reader(column_rle.getValuesColumn());

    if (column_rle.size() && values.compareAt(column_rle.size() - 1, column_rle.size() - 2, values, 1) == 0)
    {
        values.popBack(1);
        column_rle.getOffsetsData().back() += 1;
    }
    else
    {
        column_rle.getOffsetsData().push_back(column_rle.size());
    }
}

void SerializationRLE::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_rle = assert_cast<const ColumnRLE &>(column);
    nested->serializeBinary(column_rle.getValuesColumn(), column_rle.getValueIndex(row_num), ostr, settings);
}

void SerializationRLE::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column) { nested->deserializeBinary(nested_column, istr, settings); });
}

void SerializationRLE::serializeTextEscaped(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_rle = assert_cast<const ColumnRLE &>(column);
    nested->serializeTextEscaped(column_rle.getValuesColumn(), column_rle.getValueIndex(row_num), ostr, settings);
}

void SerializationRLE::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column) { nested->deserializeTextEscaped(nested_column, istr, settings); });
}

void SerializationRLE::serializeTextQuoted(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_rle = assert_cast<const ColumnRLE &>(column);
    nested->serializeTextQuoted(column_rle.getValuesColumn(), column_rle.getValueIndex(row_num), ostr, settings);
}

void SerializationRLE::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column) { nested->deserializeTextQuoted(nested_column, istr, settings); });
}

void SerializationRLE::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_rle = assert_cast<const ColumnRLE &>(column);
    nested->serializeTextCSV(column_rle.getValuesColumn(), column_rle.getValueIndex(row_num), ostr, settings);
}

void SerializationRLE::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column) { nested->deserializeTextCSV(nested_column, istr, settings); });
}

void SerializationRLE::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_rle = assert_cast<const ColumnRLE &>(column);
    nested->serializeText(column_rle.getValuesColumn(), column_rle.getValueIndex(row_num), ostr, settings);
}

void SerializationRLE::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column) { nested->deserializeWholeText(nested_column, istr, settings); });
}

void SerializationRLE::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_rle = assert_cast<const ColumnRLE &>(column);
    nested->serializeTextJSON(column_rle.getValuesColumn(), column_rle.getValueIndex(row_num), ostr, settings);
}

void SerializationRLE::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column) { nested->deserializeTextJSON(nested_column, istr, settings); });
}

void SerializationRLE::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_rle = assert_cast<const ColumnRLE &>(column);
    nested->serializeTextXML(column_rle.getValuesColumn(), column_rle.getValueIndex(row_num), ostr, settings);
}

}
