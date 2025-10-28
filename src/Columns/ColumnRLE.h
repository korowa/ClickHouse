#pragma once

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

class Collator;

namespace DB
{


/** Column for RLE representation.
 *  It stores one column with the distinct run values and another with
 *  the corresponding end offsets. For example, the column
 *    [1, 1, 1, 2, 2, 3, 3, 3]
 *  is represented as
 *    values:  [1, 2, 3]
 *    offsets: [2, 4, 7]
 */
class ColumnRLE final : public COWHelper<IColumnHelper<ColumnRLE>, ColumnRLE>
{
private:
    friend class COWHelper<IColumnHelper<ColumnRLE>, ColumnRLE>;

    explicit ColumnRLE(MutableColumnPtr && values_);
    ColumnRLE(MutableColumnPtr && values_, MutableColumnPtr && offsets_);
    ColumnRLE(const ColumnRLE &) = default;

public:
    using Base = COWHelper<IColumnHelper<ColumnRLE>, ColumnRLE>;
    static Ptr create(const ColumnPtr & values_, const ColumnPtr & offsets_)
    {
        return Base::create(values_->assumeMutable(), offsets_->assumeMutable());
    }

    template <typename TColumnPtr>
    requires IsMutableColumns<TColumnPtr>::value
    static MutablePtr create(TColumnPtr && values_, TColumnPtr && offsets_)
    {
        return Base::create(std::forward<TColumnPtr>(values_), std::forward<TColumnPtr>(offsets_));
    }

    static Ptr create(const ColumnPtr & values_) { return Base::create(values_->assumeMutable()); }

    template <typename TColumnPtr>
    requires IsMutableColumns<TColumnPtr>::value
    static MutablePtr create(TColumnPtr && values_)
    {
        return Base::create(std::forward<TColumnPtr>(values_));
    }

    bool isRLE() const override { return true; }
    const char * getFamilyName() const override { return "RLE"; }
    std::string getName() const override { return "RLE(" + values->getName() + ")"; }
    TypeIndex getDataType() const override { return values->getDataType(); }
    MutableColumnPtr cloneResized(size_t new_size) const override;
    size_t size() const override { return offsets->empty() ? 0 : getOffsetsData().back() + 1; }
    bool isDefaultAt(size_t n) const override;
    bool isNullAt(size_t n) const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    DataTypePtr getValueNameAndTypeImpl(WriteBufferFromOwnString &, size_t, const Options &) const override;
    bool getBool(size_t n) const override;
    Float64 getFloat64(size_t n) const override;
    Float32 getFloat32(size_t n) const override;
    UInt64 getUInt(size_t n) const override;
    Int64 getInt(size_t n) const override;
    UInt64 get64(size_t n) const override;
    StringRef getDataAt(size_t n) const override;

    ColumnPtr convertToFullColumnIfSparse() const override;

    /// Will insert null value if pos=nullptr
    void insertData(const char * pos, size_t length) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    StringRef serializeAggregationStateValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    char * serializeValueIntoMemory(size_t n, char * memory) const override;
    std::optional<size_t> getSerializedValueSize(size_t n) const override;
    void deserializeAndInsertFromArena(ReadBuffer & in) override;
    void deserializeAndInsertAggregationStateValueFromArena(ReadBuffer & in) override;
    void skipSerializedInArena(ReadBuffer & in) const override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif
    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src, size_t n) override;
#else
    void doInsertFrom(const IColumn & src, size_t n) override;
#endif
    void insertDefault() override;
    void insertManyDefaults(size_t length) override;

    void popBack(size_t n) override;
    ColumnPtr filter(const Filter & filt, ssize_t) const override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const override;
#endif
    void compareColumn(
        const IColumn & rhs,
        size_t rhs_row_num,
        PaddedPODArray<UInt64> * row_indexes,
        PaddedPODArray<Int8> & compare_results,
        int direction,
        int nan_direction_hint) const override;

    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int null_direction_hint, const Collator & collator) const override;
    bool hasEqualValues() const override;

    void getPermutationImpl(
        IColumn::PermutationSortDirection direction,
        IColumn::PermutationSortStability stability,
        size_t limit,
        int null_direction_hint,
        Permutation & res,
        const Collator * collator) const;

    void getPermutation(
        IColumn::PermutationSortDirection direction,
        IColumn::PermutationSortStability stability,
        size_t limit,
        int null_direction_hint,
        Permutation & res) const override;

    void updatePermutation(
        IColumn::PermutationSortDirection direction,
        IColumn::PermutationSortStability stability,
        size_t limit,
        int null_direction_hint,
        Permutation & res,
        EqualRanges & equal_ranges) const override;

    void getPermutationWithCollation(
        const Collator & collator,
        IColumn::PermutationSortDirection direction,
        IColumn::PermutationSortStability stability,
        size_t limit,
        int null_direction_hint,
        Permutation & res) const override;

    void updatePermutationWithCollation(
        const Collator & collator,
        IColumn::PermutationSortDirection direction,
        IColumn::PermutationSortStability stability,
        size_t limit,
        int null_direction_hint,
        Permutation & res,
        EqualRanges & equal_ranges) const override;

    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    ColumnPtr replicate(const Offsets & replicate_offsets) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    WeakHash32 getWeakHash32() const override;
    void updateHashFast(SipHash & hash) const override;
    void getExtremes(Field & min, Field & max) const override;

    void getIndicesOfNonDefaultRows(IColumn::Offsets & indices, size_t from, size_t limit) const override;
    double getRatioOfDefaultRows(double sample_ratio) const override;
    UInt64 getNumberOfDefaultRows() const override;

    ColumnPtr compress(bool force_compression) const override;

    void forEachMutableSubcolumn(MutableColumnCallback callback) override;
    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    void forEachSubcolumn(ColumnCallback callback) const override;
    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override;

    bool structureEquals(const IColumn & rhs) const override;

    bool isNullable() const override { return values->isNullable(); }
    bool isFixedAndContiguous() const override { return false; }
    bool valuesHaveFixedSize() const override { return values->valuesHaveFixedSize(); }
    size_t sizeOfValueIfFixed() const override { return values->sizeOfValueIfFixed() + values->sizeOfValueIfFixed(); }
    bool isCollationSupported() const override { return values->isCollationSupported(); }

    bool hasDynamicStructure() const override { return values->hasDynamicStructure(); }
    void takeDynamicStructureFromSourceColumns(const Columns & source_columns, std::optional<size_t> max_dynamic_subcolumns) override;
    void takeDynamicStructureFromColumn(const ColumnPtr & source_column) override;

    /// Return position of element in 'values' columns,
    /// that corresponds to n-th element of full column.
    /// O(log(offsets.size())) complexity,
    size_t getValueIndex(size_t n) const;

    const IColumn & getValuesColumn() const { return *values; }
    IColumn & getValuesColumn() { return *values; }

    const ColumnPtr & getValuesPtr() const { return values; }
    ColumnPtr & getValuesPtr() { return values; }

    const IColumn::Offsets & getOffsetsData() const;
    IColumn::Offsets & getOffsetsData();

    const ColumnPtr & getOffsetsPtr() const { return offsets; }
    ColumnPtr & getOffsetsPtr() { return offsets; }

    const IColumn & getOffsetsColumn() const { return *offsets; }
    IColumn & getOffsetsColumn() { return *offsets; }

private:
    /// Contains values for repetitive runs.
    WrappedPtr values;
    /// Sorted offsets of run ends in the full column.
    WrappedPtr offsets;
};

}
