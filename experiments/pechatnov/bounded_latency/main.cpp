#include <iostream>
#include <vector>
#include <span>
#include <unordered_map>
#include <cstring>

using namespace std::literals::string_view_literals;

bool operator==(std::span<char> a, std::string_view b) {
    return std::string_view(a.data(), a.size()) == b;
}

class TTrivialStringsStorage
{
public:
    using TIndex = uint32_t;
    using TValue = std::span<char>;
    static inline constexpr TIndex NilIndex = static_cast<TIndex>(-1);
    static inline constexpr TValue NilValue = {static_cast<char*>(nullptr), 0u};

    TTrivialStringsStorage(uint64_t)
    { }

    std::pair<TValue, TIndex> Allocate(uint64_t size)
    {
        ElementsCount_ += 1;
        auto& edata = EasyData_[++CurrentIndex_];
        edata.resize(size);
        return {{edata.data(), edata.size()}, CurrentIndex_};
    }

    TValue Get(TIndex index)
    {
        auto it = EasyData_.find(index);
        if (it == EasyData_.end()) {
            return NilValue;
        }
        auto& edata = it->second;
        return {edata.data(), edata.size()};
    }

    bool Free(TIndex index)
    {
        auto it = EasyData_.find(index);
        if (it == EasyData_.end()) {
            return false;
        }
        EasyData_.erase(it);
        --ElementsCount_;
        return true;
    }

    uint64_t ElementsCount()
    {
        return ElementsCount_;
    }
private:
    // Easy
    TIndex CurrentIndex_ = 0;
    std::unordered_map<TIndex, std::vector<char>> EasyData_;

    uint64_t ElementsCount_ = 0;
};


class TBlobStringsStorage
{
public:
    using TIndex = uint32_t;
    using TValue = std::span<char>;
    static inline constexpr TIndex NilIndex = static_cast<TIndex>(-1);
    static inline constexpr TValue NilValue = {static_cast<char*>(nullptr), 0u};

    TBlobStringsStorage(uint64_t bufferSize)
    {
        Data_.resize(bufferSize);
    }

    std::pair<TValue, TIndex> Allocate(uint64_t size)
    {
        ElementsCount_ += 1;
        const auto idx = AllocateIndex();
        // std::cerr << "Allocated index (Index: " << idx << ")" << std::endl;
        Insert(idx, rand(), size, RootIndex_, Data_.data(), Data_.data() + Data_.size());
        // CheckTree(RootIndex_);
        return {Get(idx), idx};
    }

    TValue Get(TIndex index)
    {
        if (index >= Positions_.size() || Positions_[index] < 0) {
            return NilValue;
        }
        return GetValue(index);
    }

    bool Free(TIndex index)
    {
        if (index >= Positions_.size() || Positions_[index] < 0) {
            return false;
        }
        Erase(index, RootIndex_);
        FreeIndex(index);
        --ElementsCount_;
        // CheckTree(RootIndex_);
        return true;
    }

    uint64_t ElementsCount()
    {
        return ElementsCount_;
    }
private:

    struct __attribute__ ((__packed__)) alignas(TIndex) THeader {
        uint64_t InnerFreeSpace : 40; // Computable.
        uint64_t ValueSize : 40; // Const.
        uint64_t HeapPriority : 48; // Const.
        TIndex LeftIndex; // Tree structure.
        TIndex RightIndex; // Tree structure.
        TIndex LeftestIndex; // Computable. Never nil.
        TIndex RightestIndex; // Computable. Never nil.

        char* GetFirstPosition()
        {
            return reinterpret_cast<char*>(this);
        }

        char* GetLastPosition()
        {
            const auto roundedValueSize = (ValueSize + 3) / 4 * 4; // Hack for aligned memory. Can be done better.
            return reinterpret_cast<char*>(this) + sizeof(THeader) + roundedValueSize;
        }
    };
    static_assert(alignof(TIndex) == 4);
    static_assert(alignof(THeader) == 4);
    static_assert(sizeof(THeader) == 32); // Not invariant, just check.

    uint64_t GetFreeSpace(TIndex idx, char* first, char* last)
    {
        if (idx == NilIndex) {
            return last - first;
        }
        auto& header = GetHeader(idx);
        assert(first <= GetHeader(header.LeftestIndex).GetFirstPosition());
        assert(GetHeader(header.RightestIndex).GetLastPosition() <= last);
        return (GetHeader(header.LeftestIndex).GetFirstPosition() - first) + header.InnerFreeSpace + (last - GetHeader(header.RightestIndex).GetLastPosition());
    }

    bool Fix(TIndex idx)
    {
        auto& header = GetHeader(idx);
        THeader oldHeader = header;
        header.InnerFreeSpace = 0;
        header.LeftestIndex = idx;
        header.RightestIndex = idx;
        if (header.LeftIndex != NilIndex) {
            auto& leftHeader = GetHeader(header.LeftIndex);
            header.InnerFreeSpace += leftHeader.InnerFreeSpace + (header.GetFirstPosition() - GetHeader(leftHeader.RightestIndex).GetLastPosition());
            header.LeftestIndex = leftHeader.LeftestIndex;
        }
        if (header.RightIndex != NilIndex) {
            auto& rightHeader = GetHeader(header.RightIndex);
            header.InnerFreeSpace += rightHeader.InnerFreeSpace + (GetHeader(rightHeader.LeftestIndex).GetFirstPosition() - header.GetLastPosition());
            header.RightestIndex = rightHeader.RightestIndex;
        }
        return (oldHeader.InnerFreeSpace != header.InnerFreeSpace) || (oldHeader.LeftestIndex != header.LeftestIndex) || (oldHeader.RightestIndex != header.RightestIndex);
    }

    void CheckTree(TIndex root)
    {
        if (root == NilIndex) {
            return;
        }
        auto& header = GetHeader(root);
        CheckTree(header.LeftIndex);
        CheckTree(header.RightIndex);
        assert(!Fix(root));
    }

    void Erase(TIndex idx, TIndex& root)
    {
        assert(root != NilIndex);
        auto& header = GetHeader(root);
        if (idx == root) {
            root = Merge(header.LeftIndex, header.RightIndex);
            return;
        }
        if (Positions_[idx] < Positions_[root]) {
            Erase(idx, header.LeftIndex);
        } else {
            Erase(idx, header.RightIndex);
        }
        Fix(root);
    }

    TIndex Merge(TIndex left, TIndex right)
    {
        if (left == NilIndex) {
            return right;
        }
        if (right == NilIndex) {
            return left;
        }
        auto& leftHeader = GetHeader(left);
        auto& rightHeader = GetHeader(right);
        if (leftHeader.HeapPriority > rightHeader.HeapPriority) {
            leftHeader.RightIndex = Merge(leftHeader.RightIndex, right);
            Fix(left);
            return left;
        } else {
            rightHeader.LeftIndex = Merge(left, rightHeader.LeftIndex);
            Fix(right);
            return right;
        }
    }

    enum class TAction
    {
        GO_LEFT,
        GO_RIGHT,
        DEFRAGMENTATE,
    };

    TAction SelectAction(uint64_t size, THeader& header, char* first, char* last)
    {
        const auto leftFreeSpace = GetFreeSpace(header.LeftIndex, first, header.GetFirstPosition());
        const auto rightFreeSpace = GetFreeSpace(header.RightIndex, header.GetLastPosition(), last);

        if (leftFreeSpace < size && rightFreeSpace < size) {
            return TAction::DEFRAGMENTATE;
        }
        if (
            // If right is not acceptable - surely take left.
            rightFreeSpace < size
            || (
                // If left is acceptable.
                leftFreeSpace >= size
                // And if there is smaller FillRate (FreeSpace / TotalSpace) in left part.
                && (leftFreeSpace * (last - header.GetLastPosition()) > rightFreeSpace * (header.GetFirstPosition() - first))
            )
        ) {
            return TAction::GO_LEFT;
        } else {
            assert(rightFreeSpace >= size);
            return TAction::GO_RIGHT;
        }
    }

    // Allocate `size` bytes for node `idx` under `root` on window [`first`, `last`)
    void Insert(TIndex idx, uint32_t priority, uint64_t size, TIndex& root, char* first, char* last)
    {
        assert(GetFreeSpace(root, first, last) >= size + sizeof(THeader));
        if (root == NilIndex || priority > GetHeader(root).HeapPriority) {
            TIndex left = NilIndex;
            TIndex right = NilIndex;
            char* position = HardSplit(size + sizeof(THeader), root, left, right, first, last);
            Positions_[idx] = position - Data_.data();
            auto& newHeader = GetHeader(idx);
            newHeader.ValueSize = size;
            newHeader.LeftIndex = left;
            newHeader.RightIndex = right;
            newHeader.HeapPriority = priority;
            root = idx;
            Fix(root);
            return;
        }
        {
            auto& header = GetHeader(root);
            auto action = SelectAction(size + sizeof(THeader), header, first, last);
            if (action == TAction::GO_LEFT) {
                Insert(idx, priority, size, header.LeftIndex, first, header.GetFirstPosition());
                Fix(root);
                return;
            }
            if (action == TAction::GO_RIGHT) {
                Insert(idx, priority, size, header.RightIndex, header.GetLastPosition(), last);
                Fix(root);
                return;
            }
            assert(action == TAction::DEFRAGMENTATE);
        }
        DefragmentatePartial(root, first); // Modify `first`, invalidate header!
        Insert(idx, priority, size, GetHeader(root).RightIndex, first, last);
        Fix(root);
    }

    char* HardSplit(uint64_t size, TIndex root, TIndex& left, TIndex& right, char* first, char* last)
    {
        const uint64_t freeSpace = GetFreeSpace(root, first, last);
        assert(freeSpace >= size);
        if (root == NilIndex) {
            assert(last - first >= size);
            left = NilIndex;
            right = NilIndex;
            return first;
        }
        {
            auto& header = GetHeader(root);
            // std::cerr << "HardSplit (Root: " << root << ", RootFirst: " << (void*)header.GetFirstPosition() << ", First: " << (void*)first << ", Last:" << (void*)last
            //     << ", FreeSpace: " << freeSpace << ", Size: " << size
            //     << ")" << std::endl;

            auto action = SelectAction(size, header, first, last);

            if (action == TAction::GO_LEFT) {
                right = root;
                char* position = HardSplit(size, header.LeftIndex, left, header.LeftIndex, first, header.GetFirstPosition());
                Fix(root);
                return position;
            }
            if (action == TAction::GO_RIGHT) {
                left = root;
                char* position = HardSplit(size, header.RightIndex, header.RightIndex, right, header.GetLastPosition(), last);
                Fix(root);
                return position;
            }
            assert(action == TAction::DEFRAGMENTATE);
        }
        DefragmentatePartial(root, first); // Modify `first`.
        {
            auto& header = GetHeader(root);
            left = root;
            char* position = HardSplit(size, header.RightIndex, header.RightIndex, right, first, last);
            Fix(root);
            return position;
        }
    }

    // Only left part, no fix of root.
    void DefragmentatePartial(TIndex root, char*& first)
    {
        assert(root != NilIndex);

        auto& header = GetHeader(root);
        Defragmentate(header.LeftIndex, first);

        const uint64_t fullSize = header.GetLastPosition() - header.GetFirstPosition();
        if (first != header.GetFirstPosition()) {
            Positions_[root] = first - Data_.data();
            std::memmove(first, header.GetFirstPosition(), fullSize); // Now `header` is invalid.
        }
        first += fullSize;
    }

    void Defragmentate(TIndex root, char*& first)
    {
        if (root == NilIndex) {
            return;
        }
        // std::cerr << "Defragmentate (Root: " << root << ", RootFirst: " << (void*)header.GetFirstPosition() << ", First: " << (void*)first << ")" << std::endl;

        DefragmentatePartial(root, first);
        Defragmentate(GetHeader(root).RightIndex, first);
        Fix(root);
    }

    TIndex AllocateIndex()
    {
        if (FirstFreeIndex_ == NilIndex) {
            TIndex idx = Positions_.size();
            Positions_.resize(std::max<size_t>(Positions_.size(), 2u) * 3 / 2);
            for (; idx < Positions_.size(); idx++) {
                FreeIndex(idx);
            }
        }
        auto idx = FirstFreeIndex_;
        FirstFreeIndex_ = -(Positions_[idx] + 1);
        return idx;
    }

    void FreeIndex(TIndex index)
    {
        Positions_[index] = -static_cast<int64_t>(FirstFreeIndex_ + 1);
        FirstFreeIndex_ = index;
    }

    THeader& GetHeader(TIndex index)
    {
        assert(index < Positions_.size() && Positions_[index] >= 0);
        return *reinterpret_cast<THeader*>(Data_.data() + Positions_[index]);
    }

    TValue GetValue(TIndex index)
    {
        return {Data_.data() + Positions_[index] + sizeof(THeader), GetHeader(index).ValueSize};
    }

private:
    std::vector<char> Data_;

    // Overhead per one element is sizeof(char*) * 3 / 2 = 12.
    // Positions_[idx] >= 0 -> it is a position of idx node in Data_,
    // Positions_[idx] < 0 -> -(Positions_[idx] + 1) is a next free node index (can be nil).
    std::vector<int64_t> Positions_;
    TIndex RootIndex_ = NilIndex;
    TIndex FirstFreeIndex_ = NilIndex;

    uint64_t ElementsCount_ = 0;
};

using TStringsStorage = TBlobStringsStorage;
// using TStringsStorage = TTrivialStringsStorage;

void SS_SimpleTest()
{
    TStringsStorage storage(1000000);

    static auto fill = [](TStringsStorage::TIndex index, TStringsStorage::TValue value) {
        for (auto& e : value) {
            e = index;
        }
    };

    static auto check = [&](TStringsStorage::TIndex index) {
        auto value = storage.Get(index);
        for (auto& e : value) {
            assert(e == index);
        }
    };

    auto [val1, idx1] = storage.Allocate(10);
    fill(idx1, val1);
    auto [val2, idx2] = storage.Allocate(20);
    fill(idx2, val2);
    auto [val3, idx3] = storage.Allocate(30);
    fill(idx3, val3);

    check(idx2);
    storage.Free(idx2);
    check(idx1);
    storage.Free(idx1);
    check(idx3);
    storage.Free(idx3);

    {
        auto [val1, idx1] = storage.Allocate(400000);
        auto [val2, idx2] = storage.Allocate(400000);
        storage.Free(idx1);
        auto [val3, idx3] = storage.Allocate(500000);
        storage.Free(idx2);
        storage.Free(idx3);
    }
}

class TStrStrHashMap
{
public:
    using TIndex = TStringsStorage::TIndex;
    using TValue = TStringsStorage::TValue;
    static inline constexpr TIndex NilIndex = TStringsStorage::NilIndex;
    static inline constexpr TValue NilValue = TStringsStorage::NilValue;

    TStrStrHashMap(uint64_t bufferSize)
        : Storage_(bufferSize)
    {
        HashTable_.assign(1, NilIndex);
    }

    std::pair<TValue, TIndex> PutUnitialized(std::string_view key, uint64_t valueSize)
    {
        if (Storage_.ElementsCount() + 1 > HashTable_.size() * 2) {
            DoubleHashTable();
        }

        const uint64_t keyHash = Hash(key);
        const uint64_t bucket = keyHash % HashTable_.size();
        EraseFromBucket(bucket, keyHash, key); // Remove old element if exists.

        auto [sval, idx] = Storage_.Allocate(CalculateSize(key.size(), valueSize));
        THeader& header = GetHeader(sval);
        header.KeyHash = keyHash;
        header.KeySize = key.size();
        std::memcpy(sval.data() + sizeof(THeader), key.data(), header.KeySize);

        InsertToBucket(bucket, idx, header);

        return {GetValue(sval), idx};
    }

    std::pair<TValue, TIndex> Put(std::string_view key, std::string_view value)
    {
        auto [val, idx] = PutUnitialized(key, value.size());
        std::memcpy(val.data(), value.data(), value.size());
        return {val, idx};
    }

    std::pair<TValue, TIndex> Get(std::string_view key)
    {
        const uint64_t keyHash = Hash(key);
        auto [prevIdx, idx] = FindInBucket(keyHash % HashTable_.size(), keyHash, key);
        return {Get(idx), idx};
    }

    TValue Get(TIndex index)
    {
        auto svalue = Storage_.Get(index);
        if (svalue.data() == nullptr) {
            return NilValue;
        }
        return GetValue(svalue);
    }

    bool Erase(std::string_view key)
    {
        const uint64_t keyHash = Hash(key);
        auto erasedIdx = EraseFromBucket(keyHash % HashTable_.size(), keyHash, key);
        if (erasedIdx == NilIndex) {
            return false;
        }
        bool success = Storage_.Free(erasedIdx);
        assert(success);
        return true;;
    }

    bool Erase(TIndex index)
    {
        auto sval = Storage_.Get(index);
        if (sval.data() == nullptr) {
            return false;
        }
        auto& header = GetHeader(sval);
        auto erasedIdx = EraseFromBucket(header.KeyHash % HashTable_.size(), header.KeyHash, GetKey(sval));
        assert(index == erasedIdx);

        bool success = Storage_.Free(erasedIdx);
        assert(success);
        return true;
    }

private:
    struct __attribute__ ((__packed__)) alignas(TIndex) THeader
    {
        static constexpr uint64_t KeyHashMask = (1ull << 56) - 1;

        uint64_t KeyHash : 56;
        uint64_t KeySize : 40;
        TIndex ListNext;
    };
    static_assert(alignof(THeader) == 4);
    static_assert(sizeof(THeader) == 16); // Not invariant, just check.

    uint64_t Hash(std::string_view key)
    {
        return std::hash<std::string_view>{}(key) & THeader::KeyHashMask;
    }

    uint64_t CalculateSize(uint64_t keySize, uint64_t valueSize)
    {
        return sizeof(THeader) + keySize + valueSize;
    }

    THeader& GetHeader(TValue svalue)
    {
        return *reinterpret_cast<THeader*>(svalue.data());
    }

    std::string_view GetKey(TValue svalue)
    {
        auto s = svalue.subspan(sizeof(THeader), GetHeader(svalue).KeySize);
        return {s.data(), s.size()};
    }

    TValue GetValue(TValue svalue)
    {
        return svalue.subspan(sizeof(THeader) + GetHeader(svalue).KeySize);
    }

    // (prevIndex, foundIndex) or (nil, nil)
    std::pair<TIndex, TIndex> FindInBucket(uint64_t bucket, uint64_t hash, std::string_view key)
    {
        TIndex prevIdx = NilIndex;
        TIndex idx = HashTable_[bucket];
        while (idx != NilIndex) {
            auto sval = Storage_.Get(idx);
            auto& header = GetHeader(sval);
            if (header.KeyHash == hash && key == GetKey(sval)) {
                return {prevIdx, idx};
            }
            prevIdx = idx;
            idx = header.ListNext;
        }
        return {NilIndex, NilIndex};
    }

    TIndex EraseFromBucket(uint64_t bucket, uint64_t hash, std::string_view key)
    {
        auto [prevIdx, idx] = FindInBucket(bucket, hash, key);
        if (idx != NilIndex) {
            auto sval = Storage_.Get(idx);
            auto& header = GetHeader(sval);
            if (prevIdx == NilIndex) {
                assert(idx == HashTable_[bucket]);
                HashTable_[bucket] = header.ListNext;
            } else {
                GetHeader(Storage_.Get(prevIdx)).ListNext = header.ListNext;
            }
            return idx;
        }
        return NilIndex;
    }

    void InsertToBucket(uint64_t bucket, TIndex idx, THeader& header)
    {
        header.ListNext = HashTable_[bucket];
        HashTable_[bucket] = idx;
    }

    void DoubleHashTable()
    {
        auto oldHashTable = std::move(HashTable_);
        HashTable_.assign(oldHashTable.size() * 2, NilIndex);
        for (auto startIdx : oldHashTable) {
            auto idx = startIdx;
            while (idx != NilIndex) {
                auto sval = Storage_.Get(idx);
                auto& header = GetHeader(sval);
                auto nextIdx = header.ListNext;
                InsertToBucket(header.KeyHash % HashTable_.size(), idx, header);
                idx = nextIdx;
            }
        }
    }

private:
    TStringsStorage Storage_;
    // Overhead per one element is sizeof(TIndex) = 4.
    std::vector<TIndex> HashTable_;
};

void SSHM_SimpleTest()
{
    TStrStrHashMap m(1000000);
    auto [val1, idx1] = m.Put("key1", "value1");
    assert(m.Get("key1").first == "value1"sv);
    auto [val2, idx2] = m.Put("key2", "value2");
    assert(m.Get("key2").first == "value2"sv);
    assert(m.Erase("key1"));
    assert(m.Get("key1").first.data() == nullptr);
    assert(!m.Erase("key1"));
    assert(m.Erase(idx2));
    assert(m.Get("key2").first.data() == nullptr);
    assert(!m.Erase(idx2));

    for (int i = 0; i < 98; ++i) {
        auto val = m.PutUnitialized(std::to_string(i), 10000).first;
        for (auto& e : val) {
            e = i;
        }
    }
    for (int i = 0; i < 98; ++i) {
        auto val = m.Get(std::to_string(i)).first;
        for (auto& e : val) {
            assert(e == i);
        }
    }
    for (int i = 0; i < 98; i += 2) {
        assert(m.Erase(std::to_string(i)));
    }
    for (int i = 0; i < 98; i += 2) {
        auto val = m.PutUnitialized(std::to_string(i), 10000).first;
        for (auto& e : val) {
            e = i;
        }
    }
    for (int i = 0; i < 98; ++i) {
        auto val = m.Get(std::to_string(i)).first;
        for (auto& e : val) {
            assert(e == i);
        }
    }
    for (int i = 0; i < 98; i += 2) {
        assert(m.Erase(std::to_string(i)));
    }
    for (int i = 100; i < 120; ++i) {
        auto val = m.PutUnitialized(std::to_string(i), 20000).first;
        for (auto& e : val) {
            e = i;
        }
    }
    for (int i = 1; i < 98; i += 2) {
        auto val = m.Get(std::to_string(i)).first;
        for (auto& e : val) {
            assert(e == i);
        }
    }
}


int main()
{
    std::cerr << "Start tests" << std::endl;
    SS_SimpleTest();
    SSHM_SimpleTest();
    std::cerr << "Finish tests" << std::endl;


    std::cerr << "Finish" << std::endl;
    return 0;
}
