#include <Columns/ColumnRLE.h>

#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>
#include <Parsers/CommonParsers.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/WeakHash.h>
#include <Common/iota.h>

#include <algorithm>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

ColumnRLE::ColumnRLE(MutableColumnPtr && values_)
    : values(std::move(values_))
{
    if (!values->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not empty values passed to ColumnRLE, but no offsets passed");

    offsets = ColumnUInt64::create();
}

ColumnRLE::ColumnRLE(MutableColumnPtr && values_, MutableColumnPtr && offsets_)
    : values(std::move(values_))
    , offsets(std::move(offsets_))
{
    const ColumnUInt64 * offsets_concrete = typeid_cast<const ColumnUInt64 *>(offsets.get());

    if (!offsets_concrete)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "'offsets' column must be a ColumnUInt64, got: {}", offsets->getName());

    if (offsets->size() != values->size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Values size ({}) is inconsistent with offsets size ({})", values->size(), offsets->size());

    const auto & offsets_data = getOffsetsData();
    if (!offsets_data.empty() && offsets_data.back() < offsets_concrete->size() - 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Last offset of RLE column ({}) cannot be lower than number of offsets ({})",
            offsets_concrete->getData().back(),
            offsets->size());

#ifndef NDEBUG
    const auto * it = std::adjacent_find(offsets_data.begin(), offsets_data.end(), std::greater_equal<>());
    if (it != offsets_data.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Offsets of ColumnRLE must be strictly sorted");
#endif
}

MutableColumnPtr ColumnRLE::cloneResized(size_t new_size) const
{
    if (new_size == 0)
        return ColumnRLE::create(values->cloneEmpty(), offsets->cloneEmpty());

    if (new_size >= size())
    {
        auto res = ColumnRLE::create(IColumn::mutate(values), IColumn::mutate(offsets));
        res->insertManyDefaults(new_size - size());
        return res;
    }

    auto res = ColumnRLE::create(values->cloneEmpty(), offsets->cloneEmpty());
    res->insertRangeFrom(*this, 0, new_size);
    return res;
}

bool ColumnRLE::isDefaultAt(size_t n) const
{
    return getValueIndex(n) == 0;
}

bool ColumnRLE::isNullAt(size_t n) const
{
    return values->isNullAt(getValueIndex(n));
}

Field ColumnRLE::operator[](size_t n) const
{
    return (*values)[getValueIndex(n)];
}

void ColumnRLE::get(size_t n, Field & res) const
{
    values->get(getValueIndex(n), res);
}

DataTypePtr ColumnRLE::getValueNameAndTypeImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const
{
    return values->getValueNameAndTypeImpl(name_buf, getValueIndex(n), options);
}

bool ColumnRLE::getBool(size_t n) const
{
    return values->getBool(getValueIndex(n));
}

Float64 ColumnRLE::getFloat64(size_t n) const
{
    return values->getFloat64(getValueIndex(n));
}

Float32 ColumnRLE::getFloat32(size_t n) const
{
    return values->getFloat32(getValueIndex(n));
}

UInt64 ColumnRLE::getUInt(size_t n) const
{
    return values->getUInt(getValueIndex(n));
}

Int64 ColumnRLE::getInt(size_t n) const
{
    return values->getInt(getValueIndex(n));
}

UInt64 ColumnRLE::get64(size_t n) const
{
    return values->get64(getValueIndex(n));
}

StringRef ColumnRLE::getDataAt(size_t n) const
{
    return values->getDataAt(getValueIndex(n));
}

ColumnPtr ColumnRLE::convertToFullColumnIfSparse() const
{
    auto res = values->cloneEmpty();
    res->reserve(size());

    const auto & offsets_data = getOffsetsData();

    size_t run_start_offset = 0;
    for (size_t run_idx = 0; run_idx < offsets_data.size(); ++run_idx)
    {
        size_t run_end_offset = offsets_data[run_idx];
        size_t length = run_end_offset - run_start_offset + 1;
        res->insertMany((*values)[run_idx], length);
        run_start_offset = run_end_offset + 1;
    }
    return res;
}

void ColumnRLE::insertData(const char * pos, size_t length)
{
    if (values->empty())
    {
        values->insertData(pos, length);
        getOffsetsData().push_back(0);
        return;
    }

    values->insertData(pos, length);

    if (values->compareAt(values->size() - 2, values->size() - 1, *values, 1) == 0)
    {
        values->popBack(1);
        ++getOffsetsData().back();
    }
    else
    {
        getOffsetsData().push_back(size());
    }
}

StringRef ColumnRLE::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return values->serializeValueIntoArena(getValueIndex(n), arena, begin);
}

StringRef ColumnRLE::serializeAggregationStateValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return values->serializeAggregationStateValueIntoArena(getValueIndex(n), arena, begin);
}

char * ColumnRLE::serializeValueIntoMemory(size_t n, char * memory) const
{
    return values->serializeValueIntoMemory(getValueIndex(n), memory);
}

std::optional<size_t> ColumnRLE::getSerializedValueSize(size_t n) const
{
    return values->getSerializedValueSize(getValueIndex(n));
}

void ColumnRLE::deserializeAndInsertFromArena(ReadBuffer &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method deserializeAndInsertFromArena is not supported for ColumnRLE.");
}

void ColumnRLE::deserializeAndInsertAggregationStateValueFromArena(ReadBuffer &)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method deserializeAndInsertAggregationStateValueFromArena is not supported for ColumnRLE.");
}

void ColumnRLE::skipSerializedInArena(ReadBuffer &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method skipSerializedInArena is not supported for ColumnRLE.");
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnRLE::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnRLE::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    if (length == 0)
        return;

    if (start + length > src.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parameter out of bound in ColumnRLE::insertRangeFrom method.");

    auto & offsets_data = getOffsetsData();

    size_t end = start + length - 1;

    if (const auto * src_rle = typeid_cast<const ColumnRLE *>(&src))
    {
        const auto & src_offsets = src_rle->getOffsetsData();
        const auto & src_values = src_rle->getValuesColumn();

        const auto original_size = size();
        for (size_t run_idx = 0; run_idx < src_offsets.size(); ++run_idx)
        {
            const auto src_offset = src_offsets[run_idx];
            if (src_offset >= end)
            {
                offsets_data.push_back(original_size + end - start);
                values->insertFrom(src_values, run_idx);
                break;
            }
            else if (src_offset >= start)
            {
                offsets_data.push_back(original_size + src_offset - start);
                values->insertFrom(src_values, run_idx);
                if (src_offset >= end)
                    break;
            }
        }
    }
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertRangeFrom is supported for source ColumnRLE.");
}

void ColumnRLE::insert(const Field & x)
{
    if (values->size() && (*values)[values->size() - 1] == x)
        ++getOffsetsData().back();
    else
    {
        values->insert(x);
        getOffsetsData().push_back(size());
    }
}

bool ColumnRLE::tryInsert(const Field & x)
{
    if (values->size() && (*values)[values->size() - 1] == x)
        ++getOffsetsData().back();
    else
    {
        if (!values->tryInsert(x))
            return false;

        getOffsetsData().push_back(size());
    }

    return true;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnRLE::insertFrom(const IColumn & src, size_t n)
#else
void ColumnRLE::doInsertFrom(const IColumn & src, size_t n)
#endif
{
    if (const auto * src_rle = typeid_cast<const ColumnRLE *>(&src))
    {
        size_t value_index = src_rle->getValueIndex(n);
        const auto & src_values = src_rle->getValuesColumn();

        if (values->size() && values->compareAt(values->size() - 1, value_index, src_values, 1) == 0)
        {
            ++getOffsetsData().back();
        }
        else
        {
            values->insertFrom(src_values, value_index);
            getOffsetsData().push_back(size());
        }
    }
    else
    {
        if (values->size() && values->compareAt(values->size() - 1, n, src, 1) == 0)
            ++getOffsetsData().back();
        else
        {
            values->insertFrom(src, n);
            getOffsetsData().push_back(size());
        }
    }
}

void ColumnRLE::insertDefault()
{
    if (values->size() && values->isDefaultAt(values->size() - 1))
        ++getOffsetsData().back();
    else
    {
        values->insertDefault();
        offsets->insert(size());
    }
}

void ColumnRLE::insertManyDefaults(size_t length)
{
    if (values->size() && values->isDefaultAt(values->size() - 1))
        ++getOffsetsData().back() += length;
    else
    {
        values->insertDefault();
        offsets->insert(size() + length);
    }
}

void ColumnRLE::popBack(size_t n)
{
    assert(n <= size());

    if (n == 0)
        return;

    size_t new_size = size() - n;

    if (new_size == 0)
    {
        values = values->cloneEmpty();
        offsets = offsets->cloneEmpty();
        return;
    }

    size_t new_last_run_idx = getValueIndex(new_size - 1);
    size_t num_runs_to_pop = values->size() - (new_last_run_idx + 1);

    if (num_runs_to_pop > 0)
    {
        values->popBack(num_runs_to_pop);
        offsets->popBack(num_runs_to_pop);
    }

    getOffsetsData().back() = new_size - 1;
}

ColumnPtr ColumnRLE::filter(const Filter & filt, ssize_t) const
{
    if (size() != filt.size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), size());

    if (size() == 0)
        return cloneEmpty();

    auto res_values = values->cloneEmpty();
    auto res_offsets = offsets->cloneEmpty();
    auto & res_offsets_data = assert_cast<ColumnUInt64 &>(*res_offsets).getData();

    const auto & offsets_data = getOffsetsData();
    const auto * filt_data = filt.data();

    size_t res_size = 0;
    size_t run_start_offset = 0;

    for (size_t run_idx = 0; run_idx < values->size(); ++run_idx)
    {
        const size_t run_end_offset = offsets_data[run_idx];
        const size_t res_run_length = countBytesInFilter(filt_data, run_start_offset, run_end_offset + 1);

        if (res_run_length > 0)
        {
            res_values->insertFrom(*values, run_idx);
            res_offsets_data.push_back(res_size + res_run_length - 1);
            res_size += res_run_length;
        }

        run_start_offset = run_end_offset + 1;
    }

    return create(std::move(res_values), std::move(res_offsets));
}

void ColumnRLE::expand(const Filter &, bool)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method expand is not supported for ColumnRLE.");
}

ColumnPtr ColumnRLE::permute(const Permutation & perm, size_t limit) const
{
    return permuteImpl(*this, perm, limit);
}

ColumnPtr ColumnRLE::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnRLE::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    assert(limit <= indexes.size());

    if (limit == 0)
        limit = size();

    if (offsets->empty())
    {
        auto res = cloneEmpty();
        return res;
    }

    auto res_offsets = offsets->cloneEmpty();
    auto & res_offsets_data = assert_cast<ColumnUInt64 &>(*res_offsets).getData();
    auto res_values = values->cloneEmpty();

    const auto & offsets_data = getOffsetsData();

    size_t value_idx = getValueIndex(indexes[0]);
    size_t current_start_offset = value_idx == 0 ? 0 : offsets_data[value_idx - 1] + 1;
    size_t current_end_offset = offsets_data[value_idx];
    for (size_t i = 0; i < limit; ++i)
    {
        if (current_start_offset > indexes[i] || indexes[i] > current_end_offset)
        {
            res_values->insertFrom(*values, value_idx);
            res_offsets_data.push_back(i - 1);

            value_idx = getValueIndex(indexes[i]);
            current_start_offset = value_idx == 0 ? 0 : offsets_data[value_idx - 1] + 1;
            current_end_offset = offsets_data[value_idx];
        }
    }

    if (res_offsets_data.empty() || res_offsets_data.back() < limit - 1)
    {
        res_values->insertFrom(*values, value_idx);
        res_offsets_data.push_back(limit - 1);
    }

    return ColumnRLE::create(std::move(res_values), std::move(res_offsets));
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnRLE::compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const
#else
int ColumnRLE::doCompareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const
#endif
{
    if (const auto * rhs_rle = typeid_cast<const ColumnRLE *>(&rhs_))
        return values->compareAt(getValueIndex(n), rhs_rle->getValueIndex(m), rhs_rle->getValuesColumn(), null_direction_hint);

    return values->compareAt(getValueIndex(n), m, rhs_, null_direction_hint);
}

void ColumnRLE::compareColumn(
    const IColumn & rhs,
    size_t rhs_row_num,
    PaddedPODArray<UInt64> * row_indexes,
    PaddedPODArray<Int8> & compare_results,
    int direction,
    int nan_direction_hint) const
{
    if (row_indexes || !typeid_cast<const ColumnRLE *>(&rhs))
    {
        auto this_full = convertToFullColumnIfSparse();
        auto rhs_full = rhs.convertToFullColumnIfSparse();
        this_full->compareColumn(*rhs_full, rhs_row_num, row_indexes, compare_results, direction, nan_direction_hint);
    }
    else
    {
        const auto & rhs_rle = assert_cast<const ColumnRLE &>(rhs);
        PaddedPODArray<Int8> nested_result;
        values->compareColumn(
            rhs_rle.getValuesColumn(), rhs_rle.getValueIndex(rhs_row_num), nullptr, nested_result, direction, nan_direction_hint);

        const auto & offsets_data = getOffsetsData();
        compare_results.resize(size());

        size_t current_offset = 0;
        for (size_t run_idx = 0; run_idx < nested_result.size(); ++run_idx)
        {
            size_t run_end_offset = offsets_data[run_idx];
            std::fill(compare_results.begin() + current_offset, compare_results.begin() + run_end_offset + 1, nested_result[run_idx]);
            current_offset = run_end_offset + 1;
        }
    }
}

int ColumnRLE::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int null_direction_hint, const Collator & collator) const
{
    if (const auto * rhs_rle = typeid_cast<const ColumnRLE *>(&rhs))
        return values->compareAtWithCollation(
            getValueIndex(n), rhs_rle->getValueIndex(m), rhs_rle->getValuesColumn(), null_direction_hint, collator);

    return values->compareAtWithCollation(getValueIndex(n), m, rhs, null_direction_hint, collator);
}

bool ColumnRLE::hasEqualValues() const
{
    for (size_t i = 2; i < values->size(); ++i)
        if (values->compareAt(0, i, *values, 1) != 0)
            return false;

    return true;
}

void ColumnRLE::getPermutationImpl(
    IColumn::PermutationSortDirection direction,
    IColumn::PermutationSortStability stability,
    size_t limit,
    int null_direction_hint,
    Permutation & res,
    const Collator * collator) const
{
    if (size() == 0)
        return;

    if (limit == 0 || limit > size())
        limit = size();

    res.resize(limit);

    Permutation perm;

    if (collator)
        values->getPermutationWithCollation(*collator, direction, stability, limit, null_direction_hint, perm);
    else
        values->getPermutation(direction, stability, limit, null_direction_hint, perm);

    const auto & offsets_data = getOffsetsData();

    size_t current_offset = 0;
    for (size_t i = 0; i < perm.size() && current_offset < limit; ++i)
    {
        const size_t run_idx = perm[i];
        size_t row_offset;
        size_t run_end_offset;
        if (run_idx == 0)
        {
            row_offset = 0;
            run_end_offset = offsets_data[run_idx] + 1;
        }
        else
        {
            row_offset = offsets_data[run_idx - 1] + 1;
            run_end_offset = offsets_data[run_idx] + 1;
        }

        while (row_offset < run_end_offset && current_offset < limit)
            res[current_offset++] = row_offset++;
    }
}

void ColumnRLE::getPermutation(
    IColumn::PermutationSortDirection direction,
    IColumn::PermutationSortStability stability,
    size_t limit,
    int null_direction_hint,
    Permutation & res) const
{
    if (unlikely(stability == IColumn::PermutationSortStability::Stable))
    {
        auto this_full = convertToFullColumnIfSparse();
        this_full->getPermutation(direction, stability, limit, null_direction_hint, res);
        return;
    }

    getPermutationImpl(direction, stability, limit, null_direction_hint, res, nullptr);
}

void ColumnRLE::updatePermutation(
    IColumn::PermutationSortDirection direction,
    IColumn::PermutationSortStability stability,
    size_t limit,
    int null_direction_hint,
    Permutation & res,
    EqualRanges & equal_ranges) const
{
    auto this_full = convertToFullColumnIfSparse();
    this_full->updatePermutation(direction, stability, limit, null_direction_hint, res, equal_ranges);
}

void ColumnRLE::getPermutationWithCollation(
    const Collator & collator,
    IColumn::PermutationSortDirection direction,
    IColumn::PermutationSortStability stability,
    size_t limit,
    int null_direction_hint,
    Permutation & res) const
{
    getPermutationImpl(direction, stability, limit, null_direction_hint, res, &collator);
}

void ColumnRLE::updatePermutationWithCollation(
    const Collator & collator,
    IColumn::PermutationSortDirection direction,
    IColumn::PermutationSortStability stability,
    size_t limit,
    int null_direction_hint,
    Permutation & res,
    EqualRanges & equal_ranges) const
{
    auto this_full = convertToFullColumnIfSparse();
    this_full->updatePermutationWithCollation(collator, direction, stability, limit, null_direction_hint, res, equal_ranges);
}

size_t ColumnRLE::byteSize() const
{
    return values->byteSize() + offsets->byteSize();
}

size_t ColumnRLE::byteSizeAt(size_t n) const
{
    return values->byteSizeAt(getValueIndex(n)) + sizeof(UInt64);
}

size_t ColumnRLE::allocatedBytes() const
{
    return values->allocatedBytes() + offsets->allocatedBytes();
}

void ColumnRLE::protect()
{
    values->protect();
    offsets->protect();
}

ColumnPtr ColumnRLE::replicate(const Offsets & replicate_offsets) const
{
    if (size() != replicate_offsets.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of offsets doesn't match size of column.");

    if (size() == 0)
        return ColumnRLE::create(values->cloneEmpty());

    auto res_offsets = offsets->cloneEmpty();
    auto & res_offsets_data = assert_cast<ColumnUInt64 &>(*res_offsets).getData();
    auto res_values = values->cloneEmpty();

    size_t run_idx = 0;
    size_t run_end_offset = getOffsetsData()[0];
    size_t replicated_run_length = 0;
    for (size_t i = 0; i < replicate_offsets.size(); ++i)
    {
        replicated_run_length += replicate_offsets[i] - (i == 0 ? 0 : replicate_offsets[i - 1]);
        if (i == run_end_offset)
        {
            if (replicated_run_length)
            {
                auto replicated_run_end
                    = res_offsets_data.size() ? (replicated_run_length + res_offsets_data.back()) : (replicated_run_length - 1);
                res_values->insertFrom(*values, run_idx);
                res_offsets_data.push_back(replicated_run_end);
            }

            run_idx += 1;
            if (run_idx < getOffsetsData().size())
            {
                run_end_offset = getOffsetsData()[run_idx];
                replicated_run_length = 0;
            }
            else
                break;
        }
    }

    return ColumnRLE::create(std::move(res_values), std::move(res_offsets));
}

void ColumnRLE::updateHashWithValue(size_t n, SipHash & hash) const
{
    values->updateHashWithValue(getValueIndex(n), hash);
}

WeakHash32 ColumnRLE::getWeakHash32() const
{
    WeakHash32 values_hash = values->getWeakHash32();
    WeakHash32 hash(size());

    auto & hash_data = hash.getData();
    auto & values_hash_data = values_hash.getData();
    const auto & offsets_data = getOffsetsData();

    size_t current_offset = 0;
    for (size_t run_idx = 0; run_idx < values->size(); ++run_idx)
    {
        size_t run_end_offset = offsets_data[run_idx];
        size_t run_length = run_end_offset - current_offset + 1;
        std::fill_n(hash_data.begin() + current_offset, run_length, values_hash_data[run_idx]);
        current_offset += run_length;
    }

    return hash;
}

void ColumnRLE::updateHashFast(SipHash & hash) const
{
    values->updateHashFast(hash);
    offsets->updateHashFast(hash);
}

void ColumnRLE::getExtremes(Field & min, Field & max) const
{
    if (size() == 0)
    {
        values->get(0, min);
        values->get(0, max);
        return;
    }

    values->getExtremes(min, max);
}

void ColumnRLE::getIndicesOfNonDefaultRows(IColumn::Offsets &, size_t, size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getIndicesOfNonDefaultRows is not supported for ColumnRLE.");
}

double ColumnRLE::getRatioOfDefaultRows(double) const
{
    return static_cast<double>(getNumberOfDefaultRows()) / size();
}

UInt64 ColumnRLE::getNumberOfDefaultRows() const
{
    UInt64 res = 0;
    const auto & offsets_data = getOffsetsData();
    for (size_t run_idx = 0; run_idx < values->size(); ++run_idx)
        if (values->isDefaultAt(run_idx))
        {
            const auto run_start_offset = run_idx == 0 ? 0 : (offsets_data[run_idx - 1] + 1);
            res += offsets_data[run_idx] - run_start_offset + 1;
        }
    return res;
}

ColumnPtr ColumnRLE::compress(bool force_compression) const
{
    auto values_compressed = values->compress(force_compression);
    auto offsets_compressed = offsets->compress(force_compression);

    size_t byte_size = values_compressed->byteSize() + offsets_compressed->byteSize();

    return ColumnCompressed::create(
        size(),
        byte_size,
        [my_values_compressed = std::move(values_compressed), my_offsets_compressed = std::move(offsets_compressed), size = size()]
        { return ColumnRLE::create(my_values_compressed->decompress(), my_offsets_compressed->decompress()); });
}

bool ColumnRLE::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_rle = typeid_cast<const ColumnRLE *>(&rhs))
        return values->structureEquals(*rhs_rle->values);
    return false;
}

void ColumnRLE::forEachMutableSubcolumn(MutableColumnCallback callback)
{
    callback(values);
    callback(offsets);
}

void ColumnRLE::forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    callback(*values);
    values->forEachMutableSubcolumnRecursively(callback);
    callback(*offsets);
    offsets->forEachMutableSubcolumnRecursively(callback);
}

void ColumnRLE::forEachSubcolumn(ColumnCallback callback) const
{
    callback(values);
    callback(offsets);
}

void ColumnRLE::forEachSubcolumnRecursively(RecursiveColumnCallback callback) const
{
    callback(*values);
    values->forEachSubcolumnRecursively(callback);
    callback(*offsets);
    offsets->forEachSubcolumnRecursively(callback);
}

const IColumn::Offsets & ColumnRLE::getOffsetsData() const
{
    return assert_cast<const ColumnUInt64 &>(*offsets).getData();
}

IColumn::Offsets & ColumnRLE::getOffsetsData()
{
    return assert_cast<ColumnUInt64 &>(*offsets).getData();
}

size_t ColumnRLE::getValueIndex(size_t n) const
{
    assert(n < size());

    const auto & offsets_data = getOffsetsData();
    const auto * it = std::lower_bound(offsets_data.begin(), offsets_data.end(), n);

    return it - offsets_data.begin();
}

void ColumnRLE::takeDynamicStructureFromSourceColumns(const Columns & source_columns, std::optional<size_t> max_dynamic_subcolumns)
{
    Columns values_source_columns;
    values_source_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
        values_source_columns.push_back(assert_cast<const ColumnRLE &>(*source_column).getValuesPtr());
    values->takeDynamicStructureFromSourceColumns(values_source_columns, max_dynamic_subcolumns);
}

void ColumnRLE::takeDynamicStructureFromColumn(const ColumnPtr & source_column)
{
    values->takeDynamicStructureFromColumn(assert_cast<const ColumnRLE &>(*source_column).getValuesPtr());
}

}
