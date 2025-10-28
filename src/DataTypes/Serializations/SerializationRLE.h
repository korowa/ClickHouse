#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{


/** Serialization for RLE (Run-Length Encoding) representation.
 *
 *  Format:
 *    Values and run lengths are written to separate substreams.
 *
 *    Run lengths have position independent format: each run length
 *    is the number of consecutive identical values in that run.
 *    Run lengths are written in VarInt encoding.
 */
class SerializationRLE final : public ISerialization
{
public:
    explicit SerializationRLE(const SerializationPtr & nested_);

    KindStack getKindStack() const override;

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    /// Allows to write ColumnRLE and other columns in RLE serialization.
    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    /// Allows to read only ColumnRLE.
    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

private:
    struct SubcolumnCreator : public ISubcolumnCreator
    {
        const ColumnPtr offsets;

        explicit SubcolumnCreator(const ColumnPtr & offsets_)
            : offsets(offsets_)
        {
        }

        DataTypePtr create(const DataTypePtr & prev) const override { return prev; }
        SerializationPtr create(const SerializationPtr & prev, const DataTypePtr &) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
    };

    template <typename Reader>
    void deserialize(IColumn & column, Reader && reader) const;

    SerializationPtr nested;
};

struct SubstreamsCacheRLEOffsetsElement : public ISerialization::ISubstreamsCacheElement
{
    explicit SubstreamsCacheRLEOffsetsElement(
        ColumnPtr offsets_,
        size_t old_size_,
        // size_t read_rows_,
        size_t skipped_values_rows_)
        : offsets(std::move(offsets_))
        , old_size(old_size_)
        // , read_rows(read_rows_)
        , skipped_values_rows(skipped_values_rows_)
    {
    }

    ColumnPtr offsets;
    size_t old_size = 0;
    // size_t read_rows = 0;
    size_t skipped_values_rows = 0;
};

}
