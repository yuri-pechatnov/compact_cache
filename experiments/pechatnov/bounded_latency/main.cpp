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

    TBlobStringsStorage()
    {
        Data_.resize(1000000); // TODO
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
        if (index >= Positions_.size() || Positions_[index] == nullptr) {
            return NilValue;
        }
        return GetValue(index);
    }

    bool Free(TIndex index)
    {
        if (index >= Positions_.size() || Positions_[index] == nullptr) {
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

    struct THeader {
        uint64_t InnerFreeSpace; // Computable.
        uint64_t ValueSize; // Const.
        TIndex LeftIndex; // Tree structure.
        TIndex RightIndex; // Tree structure.
        TIndex LeftestIndex; // Computable. Never nil.
        TIndex RightestIndex; // Computable. Never nil.
        uint32_t HeapPriority; // Const.

        char* GetFirstPosition()
        {
            return reinterpret_cast<char*>(this);
        }

        char* GetLastPosition()
        {
            const auto roundedValueSize = (ValueSize + 7) / 8 * 8; // Hack for aligned memory. Can be done better.
            return reinterpret_cast<char*>(this) + sizeof(THeader) + roundedValueSize;
        }
    };

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

    // Allocate `size` bytes for node `idx` under `root` on window [`first`, `last`)
    void Insert(TIndex idx, uint32_t priority, uint64_t size, TIndex& root, char* first, char* last)
    {
        assert(GetFreeSpace(root, first, last) >= size + sizeof(THeader));
        // Can be easily optimized.
        TIndex left = NilIndex;
        TIndex right = NilIndex;
        char* position = HardSplit(size + sizeof(THeader), root, left, right, first, last);
        Positions_[idx] = position;
        auto& header = GetHeader(idx);
        header.ValueSize = size;
        header.LeftIndex = NilIndex;
        header.RightIndex = NilIndex;
        header.HeapPriority = priority;
        Fix(idx);
        root = Merge(left, idx);
        root = Merge(root, right);
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
        auto& header = GetHeader(root);
        // std::cerr << "HardSplit (Root: " << root << ", RootFirst: " << (void*)header.GetFirstPosition() << ", First: " << (void*)first << ", Last:" << (void*)last
        //     << ", FreeSpace: " << freeSpace << ", Size: " << size
        //     << ")" << std::endl;

        const auto leftFreeSpace = GetFreeSpace(header.LeftIndex, first, header.GetFirstPosition());
        const auto rightFreeSpace = GetFreeSpace(header.RightIndex, header.GetLastPosition(), last);

        if (leftFreeSpace >= size || rightFreeSpace >= size) {
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
                right = root;
                char* position = HardSplit(size, header.LeftIndex, left, header.LeftIndex, first, header.GetFirstPosition());
                Fix(root);
                return position;
            } else {
                assert(rightFreeSpace >= size);
                left = root;
                char* position = HardSplit(size, header.RightIndex, header.RightIndex, right, header.GetLastPosition(), last);
                Fix(root);
                return position;
            }
        }

        char* startPosition = first;
        Defragmentate(root, startPosition);
        assert(last - startPosition == freeSpace); // Amount of free space must not be changed here.
        return HardSplit(size, root, left, right, first, last); // Just retry after defragmentation.
    }

    void Defragmentate(TIndex root, char*& first)
    {
        if (root == NilIndex) {
            return;
        }
        auto& header = GetHeader(root);

        // std::cerr << "Defragmentate (Root: " << root << ", RootFirst: " << (void*)header.GetFirstPosition() << ", First: " << (void*)first << ")" << std::endl;

        auto rightChildIndex = header.RightIndex;
        Defragmentate(header.LeftIndex, first);

        Positions_[root] = first;
        const uint64_t fullSize = header.GetLastPosition() - header.GetFirstPosition();
        std::memmove(first, header.GetFirstPosition(), fullSize); // Now `header` is invalid.
        first += fullSize;

        Defragmentate(rightChildIndex, first);
        Fix(root);
    }

    TIndex AllocateIndex()
    {
        if (FreeIndexes_.empty()) {
            TIndex idx = Positions_.size();
            Positions_.resize(std::max<size_t>(Positions_.size(), 1u) * 2);
            for (; idx < Positions_.size(); idx++) {
                Positions_[idx] = nullptr;
                FreeIndexes_.push_back(idx);
            }
        }
        auto idx = FreeIndexes_.back();
        FreeIndexes_.pop_back();
        return idx;
    }

    void FreeIndex(TIndex index)
    {
        Positions_[index] = nullptr;
        FreeIndexes_.push_back(index);
    }

    THeader& GetHeader(TIndex index)
    {
        assert(index < Positions_.size() && Positions_[index] != nullptr);
        return *reinterpret_cast<THeader*>(Positions_[index]);
    }

    TValue GetValue(TIndex index)
    {
        return {Positions_[index] + sizeof(THeader), GetHeader(index).ValueSize};
    }

private:
    std::vector<char> Data_;
    std::vector<char*> Positions_;
    std::vector<TIndex> FreeIndexes_;
    TIndex RootIndex_ = NilIndex;

    uint64_t ElementsCount_ = 0;
};

using TStringsStorage = TBlobStringsStorage;
// using TStringsStorage = TTrivialStringsStorage;

void SS_SimpleTest()
{
    TStringsStorage storage;

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

    TStrStrHashMap()
    {
        HashTable_.assign(1, NilIndex);
    }

    std::pair<TValue, TIndex> PutUnitialized(std::string_view key, uint64_t valueSize)
    {
        if (Storage_.ElementsCount() + 1 > HashTable_.size()) {
            DoubleHashTable();
        }

        const uint64_t keyHash = std::hash<std::string_view>{}(key);
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
        const uint64_t keyHash = std::hash<std::string_view>{}(key);
        auto idx = FindInBucket(keyHash % HashTable_.size(), keyHash, key);
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
        const uint64_t keyHash = std::hash<std::string_view>{}(key);
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
    struct THeader
    {
        uint64_t KeyHash;
        TIndex ListLeft;
        TIndex ListRight;
        uint64_t KeySize;
    };

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

    TIndex FindInBucket(uint64_t bucket, uint64_t hash, std::string_view key)
    {
        TIndex idx = HashTable_[bucket];
        while (idx != NilIndex) {
            auto sval = Storage_.Get(idx);
            auto& header = GetHeader(sval);
            if (header.KeyHash == hash && key == GetKey(sval)) {
                return idx;
            }
            idx = header.ListRight;
        }
        return NilIndex;
    }

    TIndex EraseFromBucket(uint64_t bucket, uint64_t hash, std::string_view key)
    {
        auto idx = FindInBucket(bucket, hash, key);
        if (idx != NilIndex) {
            auto sval = Storage_.Get(idx);
            auto& header = GetHeader(sval);
            if (header.ListLeft == NilIndex) {
                assert(idx == HashTable_[bucket]);
                HashTable_[bucket] = header.ListRight;
            } else {
                GetHeader(Storage_.Get(header.ListLeft)).ListRight = header.ListRight;
            }
            if (header.ListRight != NilIndex) {
                GetHeader(Storage_.Get(header.ListRight)).ListLeft = header.ListLeft;
            }
            return idx;
        }
        return NilIndex;
    }

    void InsertToBucket(uint64_t bucket, TIndex idx, THeader& header)
    {
        const TIndex oldIdx = HashTable_[bucket];
        HashTable_[bucket] = idx;

        header.ListLeft = NilIndex;
        header.ListRight = oldIdx;

        if (oldIdx != NilIndex) {
            GetHeader(Storage_.Get(oldIdx)).ListLeft = idx;
        }
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
                auto rightIdx = header.ListRight;
                InsertToBucket(header.KeyHash % HashTable_.size(), idx, header);
                idx = rightIdx;
            }
        }
    }

private:
    TStringsStorage Storage_;
    std::vector<TIndex> HashTable_;
};

void SSHM_SimpleTest()
{
    TStrStrHashMap m;
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
