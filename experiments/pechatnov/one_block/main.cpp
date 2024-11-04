#include <iostream>
#include <chrono>
#include <vector>
#include <span>
#include <unordered_map>
#include <map>
#include <cstring>
#include <thread>

#ifdef NDEBUG
    #define verify(flag) do { if (!(flag)) { abort(); } } while (false)
#else
    #define verify assert
#endif

double Now()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() / 1000.0;
}

#if defined(__APPLE__) && defined(__MACH__)
#include <mach/mach.h>
double Rss()
{
    struct mach_task_basic_info info;
    mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                  (task_info_t)&info, &infoCount) != KERN_SUCCESS)
        return (size_t)0L; /* Can't access? */
    return (size_t)info.resident_size / 1e6;
}
#else
double Rss()
{
    int64_t s = 0;
    FILE* f = fopen("/proc/self/statm", "r");
    if (!f) {
        return 0;
    }
    fscanf(f, "%lld", &s);
    fclose(f);
    return s / 1e6;
}
#endif


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
        auto idx = AllocateIndex();
        auto& d = Data_[idx];
        d = std::vector<char>{};
        d->resize(size);
        return {{d->data(), d->size()}, idx};
    }

    TValue Get(TIndex index)
    {
        if (index >= Data_.size()) {
            return NilValue;
        }
        auto& d = Data_[index];
        if (!d.has_value()) {
            return NilValue;
        }
        return {d->data(), d->size()};
    }

    bool Free(TIndex index)
    {
        if (!Data_[index].has_value()) {
            return false;
        }
        Data_[index].reset();
        FreeIndex(index);
        return true;
    }

    uint64_t ElementsCount()
    {
        return CurrentIndex_ - FreeIndexes_.size();
    }

    void Clear()
    {
        CurrentIndex_ = 0;
        Data_.clear();
        FreeIndexes_.clear();
    }

    double FillRate()
    {
        return 0;
    }

    uint64_t DefragmentatedBytes()
    {
        return 0;
    }
private:
    TIndex AllocateIndex()
    {
        if (FreeIndexes_.empty()) {
            FreeIndexes_.push_back(CurrentIndex_++);
            if (CurrentIndex_ > Data_.size()) {
                Data_.resize(CurrentIndex_ * 2);
            }
        }
        auto idx = FreeIndexes_.back();
        FreeIndexes_.pop_back();
        return idx;
    }

    void FreeIndex(TIndex idx)
    {
        FreeIndexes_.push_back(idx);
    }

private:
    TIndex CurrentIndex_ = 0;
    std::vector<TIndex> FreeIndexes_;
    std::vector<std::optional<std::vector<char>>> Data_;
};

constexpr int GetRank(uint64_t x) {
    int lg = 64 - __builtin_clzll(x);
    return (lg << 4) | ((x << 5 >> lg) & 15);
}

template <int BitsCount>
class TBitMask {
public:
    TBitMask() = default;

    void Set(int i)
    {
        Data_[i / 64] |= 1ull << (i & 63);
    }

    void Reset(int i)
    {
        Data_[i / 64] &= ~(1ull << (i & 63));
    }

    int Find(int start)
    {
        const int startBlock = start / 64;
        const int startBlockOffset = (start & 63);
        const int startBlockRelPos = __builtin_ctzll(Data_[startBlock] >> startBlockOffset);
        if (startBlockRelPos != 64) {
            return start + startBlockRelPos;
        }
        for (int block = startBlock + 1; block < DataSize_; ++block) {
            const int blockAbsPos = __builtin_ctzll(Data_[block]);
            if (blockAbsPos != 64) {
                return block * 64 + blockAbsPos;
            }
        }
        return -1;
    }

private:
    static constexpr int DataSize_ = (BitsCount + 63) / 64;
    uint64_t Data_[DataSize_] = {};
};


class TBlobStringsStorage
{
public:
    static constexpr uint64_t MaxSize = 200'000'000'000;
    static constexpr int MaxSizeRank = GetRank(MaxSize);

    using TIndex = uint32_t;
    using TValue = std::span<char>;
    static inline constexpr TIndex NilIndex = static_cast<TIndex>(-1);
    static inline constexpr TValue NilValue = {static_cast<char*>(nullptr), 0u};

    TBlobStringsStorage(uint64_t bufferSize)
    {
        bufferSize = RoundValueSize(bufferSize);
        if (bufferSize < OccupiedMetaSize_) {
            throw std::runtime_error("too small buffer size");
        }
        Data_.resize(bufferSize);
        Clear();
    }

    std::pair<TValue, TIndex> Allocate(uint64_t size)
    {
        //std::cerr << "OccupiedSpace_=" << OccupiedSpace_ << std::endl;
        const uint64_t roundedSize = RoundValueSize(size);
        const uint64_t fullSize = roundedSize + sizeof(THeader);

        if (fullSize > Data_.size() - OccupiedSpace_) {
            throw std::runtime_error("no space");
        }
        THeader& header = FindHeaderWithFreeSpace(fullSize);

        ElementsCount_ += 1;
        OccupiedSpace_ += fullSize;
        const auto idx = AllocateIndex();

        UnregisterFreeSpace(header);
        THeader& newHeader = *reinterpret_cast<THeader*>(Data_.data() + header.GetLastOffset(Data_.data()));
        const uint64_t newHeaderOffset = newHeader.GetFirstOffset(Data_.data());
        Positions_[idx] = newHeaderOffset;
        newHeader.OwnIndex = idx;
        newHeader.ValueSize = size;
        newHeader.RightOffset = header.RightOffset;
        newHeader.LeftOffset = header.GetFirstOffset(Data_.data());
        header.RightOffset = newHeaderOffset;
        newHeader.GetRightHeader(Data_.data()).LeftOffset = newHeaderOffset;
        RegisterFreeSpace(header);
        RegisterFreeSpace(newHeader);

        return {GetValue(idx), idx};
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
        --ElementsCount_;
        auto& header = GetHeader(index);
        verify(OccupiedSpace_ >= OccupiedMetaSize_ + header.GetFullSize());
        OccupiedSpace_ -= header.GetFullSize();
        auto& leftHeader = header.GetLeftHeader(Data_.data());
        auto& rightHeader = header.GetRightHeader(Data_.data());
        UnregisterFreeSpace(leftHeader);
        UnregisterFreeSpace(header);
        leftHeader.RightOffset = header.RightOffset;
        rightHeader.LeftOffset = header.LeftOffset;
        RegisterFreeSpace(leftHeader);
        FreeIndex(index);
        return true;
    }

    uint64_t ElementsCount()
    {
        return ElementsCount_;
    }

    void Clear()
    {
        ElementsCount_ = 0;
        OccupiedSpace_ = OccupiedMetaSize_;
        Positions_.clear();
        FirstFreeIndex_ = NilIndex;
        AvailableRanks_ = {};

        // Rank nodes. Special service nodes. Never moved.
        RankNodes_ = reinterpret_cast<THeader*>(Data_.data());
        for (int i = 0; i < MaxSizeRank; ++i) {
            THeader& rankNode = RankNodes_[i];
            const uint64_t offset = rankNode.GetFirstOffset(Data_.data());
            rankNode.OwnIndex = NilIndex; // Not important.
            rankNode.ValueSize = 0; // Not important.
            rankNode.LeftOffset = offset; // Not important.
            rankNode.RightOffset = offset; // Not important.
            rankNode.LeftInRankOffset = offset; // Important.
            rankNode.RightInRankOffset = offset; // Important.
        }

        // Special border nodes. Never moved.
        {
            THeader& leftestNode = RankNodes_[MaxSizeRank];
            THeader& rightestNode = *reinterpret_cast<THeader*>(Data_.data() + Data_.size() - sizeof(THeader));
            const uint64_t leftestOffset = leftestNode.GetFirstOffset(Data_.data());
            const uint64_t rightestOffset = rightestNode.GetFirstOffset(Data_.data());

            leftestNode.OwnIndex = NilIndex; // Important.
            leftestNode.ValueSize = 0; // Important.
            leftestNode.LeftOffset = 0; // Important. It is marker.
            leftestNode.RightOffset = rightestOffset; // Important.
            leftestNode.LeftInRankOffset = leftestOffset; // Important.
            leftestNode.RightInRankOffset = leftestOffset; // Important.

            rightestNode.OwnIndex = NilIndex; // Important.
            rightestNode.ValueSize = 0; // Important.
            rightestNode.LeftOffset = leftestOffset; // Important.
            rightestNode.RightOffset = Data_.size(); // Important. It is marker.
            rightestNode.LeftInRankOffset = rightestOffset; // Important.
            rightestNode.RightInRankOffset = rightestOffset; // Important.

            RegisterFreeSpace(leftestNode);
        }
    }

    double FillRate()
    {
        return static_cast<double>(OccupiedSpace_) / Data_.size();
    }

    uint64_t DefragmentatedBytes()
    {
        return DefragmentatedBytes_;
    }
private:

    static constexpr uint64_t RoundValueSize(uint64_t valueSize)
    {
        return (valueSize + 3) & ~3ull;
    }

    struct __attribute__ ((__packed__)) alignas(TIndex) THeader {
        uint64_t LeftOffset : 38; // Absolute offset from begin of Data_.
        uint64_t RightOffset : 38; // Absolute offset from begin of Data_.
        uint64_t LeftInRankOffset : 38; // Absolute offset from begin of Data_.
        uint64_t RightInRankOffset : 38; // Absolute offset from begin of Data_.
        uint64_t ValueSize : 38;
        TIndex OwnIndex;

        uint64_t GetRightFreeSize(char* start)
        {
            return RightOffset - GetLastOffset(start);
        }

        uint64_t GetFullSize()
        {
            return sizeof(THeader) + RoundValueSize(ValueSize); // Hack for aligned memory. Can be done better?
        }

        uint64_t GetFirstOffset(char* start)
        {
            return reinterpret_cast<char*>(this) - start;
        }

        uint64_t GetLastOffset(char* start)
        {
            return reinterpret_cast<char*>(this) + GetFullSize() - start;
        }

        THeader& GetLeftHeader(char* start)
        {
            return *reinterpret_cast<THeader*>(start + LeftOffset);
        }

        THeader& GetRightHeader(char* start)
        {
            return *reinterpret_cast<THeader*>(start + RightOffset);
        }

        THeader& GetLeftInRankHeader(char* start)
        {
            return *reinterpret_cast<THeader*>(start + LeftInRankOffset);
        }

        THeader& GetRightInRankHeader(char* start)
        {
            return *reinterpret_cast<THeader*>(start + RightInRankOffset);
        }
    };
    static_assert(alignof(TIndex) == 4);
    static_assert(alignof(THeader) == 4);
    //static_assert(RoundValueSize(alignof(THeader) / 2) == alignof(THeader));
    static_assert(sizeof(THeader) == 28); // Not invariant, just check.


    THeader& FindHeaderWithFreeSpace(uint64_t fullSize)
    {
        const int requiredRank = GetRank(fullSize) + 1;
        const int availableRank = AvailableRanks_.Find(requiredRank);
        if (availableRank == -1) {
            return Defragmentate(fullSize);
        }
        // std::cout << "FindHeaderWithFreeSpace requiredRank=" << requiredRank << " availableRank=" << availableRank << std::endl;
        THeader& rankNodeHeader = RankNodes_[availableRank];
        verify(rankNodeHeader.LeftInRankOffset != rankNodeHeader.GetFirstOffset(Data_.data()));
        return rankNodeHeader.GetRightInRankHeader(Data_.data());
    }

    THeader& Defragmentate(uint64_t fullSize)
    {
        THeader* header = nullptr;
        // Find point to start defragmentation.
        {
            // Find random allocation.
            TIndex startIndex = NilIndex;
            while (true) {
                startIndex = rand() % Positions_.size();
                if (Positions_[startIndex] >= 0) {
                    break;
                }
            }
            // std::cerr << "Positions. ";
            // for (int i = 0; i < Positions_.size(); ++i) {
            //     std::cerr << i << ":" << Positions_[i] << ", ";
            // }
            // std::cerr << " first:" << FirstFreeIndex_ << std::endl;
            header = &GetHeader(startIndex);
            // Search for free space to the right.
            uint64_t rightFreeSpace = 0;
            THeader* currentHeader = header;
            while (rightFreeSpace < fullSize && currentHeader->RightOffset != Data_.size()) {
                rightFreeSpace += currentHeader->GetRightFreeSize(Data_.data());
                currentHeader = &currentHeader->GetRightHeader(Data_.data());
            }
            // And to the left if need more.
            while (rightFreeSpace < fullSize && header->LeftOffset != 0) {
                header = &header->GetLeftHeader(Data_.data());
                rightFreeSpace += header->GetRightFreeSize(Data_.data());
            }
            verify(rightFreeSpace >= fullSize);
        }
        while (true) {
            if (header->GetRightFreeSize(Data_.data()) >= fullSize) {
                return *header;
            }
            THeader& nextHeader = header->GetRightHeader(Data_.data());
            verify(nextHeader.RightOffset != Data_.size()); // If there is no space - abort.

            const uint64_t nextFullSize = nextHeader.GetFullSize();
            const uint64_t oldNextOffset = nextHeader.GetFirstOffset(Data_.data());
            const uint64_t newNextOffset = header->GetLastOffset(Data_.data());

            if (newNextOffset == oldNextOffset) {
                header = &nextHeader;
                continue;
            }

            THeader& afterNextHeader = nextHeader.GetRightHeader(Data_.data());

            UnregisterFreeSpace(*header);
            UnregisterFreeSpace(nextHeader);

            header->RightOffset = newNextOffset;
            afterNextHeader.LeftOffset = newNextOffset;
            Positions_[nextHeader.OwnIndex] = newNextOffset;

            std::memmove(Data_.data() + newNextOffset, Data_.data() + oldNextOffset, nextFullSize); // Now `nextHeader` is invalid.
            DefragmentatedBytes_ += nextFullSize;

            header = &header->GetRightHeader(Data_.data());

            RegisterFreeSpace(*header);
        }
        verify(false);
    }

    void UnregisterFreeSpace(THeader& header)
    {
        const uint64_t freeSize = header.GetRightFreeSize(Data_.data());
        if (freeSize == 0) {
            return;
        }
        if (header.LeftInRankOffset == header.RightInRankOffset) { // If it was last element.
            AvailableRanks_.Reset(GetRank(freeSize));
        }
        header.GetLeftInRankHeader(Data_.data()).RightInRankOffset = header.RightInRankOffset;
        header.GetRightInRankHeader(Data_.data()).LeftInRankOffset = header.LeftInRankOffset;

        const uint64_t offset = header.GetFirstOffset(Data_.data());
        header.LeftInRankOffset = offset;
        header.RightInRankOffset = offset;
    }

    void RegisterFreeSpace(THeader& header)
    {
        const uint64_t freeSize = header.GetRightFreeSize(Data_.data());
        if (freeSize == 0) {
            return;
        }
        const int rank = GetRank(freeSize);
        const uint64_t offset = header.GetFirstOffset(Data_.data());
        THeader& rankNodeHeader = RankNodes_[rank];
        const uint64_t rankNodeOffset = rankNodeHeader.GetFirstOffset(Data_.data());

        if (rankNodeHeader.RightInRankOffset == rankNodeOffset) { // If was empty.
            AvailableRanks_.Set(rank);
        }

        // std::cout << "RegisterFreeSpace1 freeSize=" << freeSize << " rank=" << rank << " " << rankNodeHeader.RightInRankOffset << " " << rankNodeHeader.LeftInRankOffset << " " << rankNodeOffset << " " << offset << std::endl;


        header.LeftInRankOffset = rankNodeOffset;
        header.RightInRankOffset = rankNodeHeader.RightInRankOffset;
        rankNodeHeader.RightInRankOffset = offset;
        header.GetRightInRankHeader(Data_.data()).LeftInRankOffset = offset;

        // std::cout << "RegisterFreeSpace rank=" << rank << " " << rankNodeHeader.RightInRankOffset << " " << rankNodeHeader.LeftInRankOffset << " " << rankNodeOffset << " " << offset << std::endl;
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
        FirstFreeIndex_ = -(Positions_[idx] + 2);
        return idx;
    }

    void FreeIndex(TIndex index)
    {
        Positions_[index] = -static_cast<int64_t>(FirstFreeIndex_ + 2);
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
    static constexpr uint64_t OccupiedMetaSize_ = sizeof(THeader) * (MaxSizeRank + 2);

    TBitMask<MaxSizeRank + 1> AvailableRanks_;

    std::vector<char> Data_;
    THeader* RankNodes_;

    // Overhead per one element is sizeof(char*) * 3 / 2 = 12.
    // Positions_[idx] >= 0 -> it is a position of idx node in Data_,
    // Positions_[idx] < 0 -> -(Positions_[idx] + 1) is a next free node index (can be nil).
    std::vector<int64_t> Positions_;
    TIndex FirstFreeIndex_ = NilIndex;

    uint64_t ElementsCount_ = 0;
    uint64_t OccupiedSpace_ = 0;
    uint64_t DefragmentatedBytes_ = 0;
};

#ifdef TRIVIAL_STORAGE
using TStringsStorage = TTrivialStringsStorage;
std::string RunDesc = "Mode: TRIVIAL";
#else
using TStringsStorage = TBlobStringsStorage;
std::string RunDesc = "Mode: BLOB";
#endif

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
            verify(e == index);
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
        if (Storage_.ElementsCount() + 1 > HashTable_.size() * 2) { // Multiplier has significant effect on speed.
            DoubleHashTable();
        }

        const uint64_t keyHash = Hash(key);
        const uint64_t bucket = keyHash % HashTable_.size();
        auto erasedIdx = EraseFromBucket(bucket, keyHash, key); // Remove old element if exists.
        if (erasedIdx != NilIndex) {
            Storage_.Free(erasedIdx);
        }

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

    void Clear()
    {
        Storage_.Clear();
        HashTable_.assign(1, NilIndex);
    }

    uint64_t ElementsCount()
    {
        return Storage_.ElementsCount();
    }

    double FillRate()
    {
        return Storage_.FillRate();
    }

    uint64_t DefragmentatedBytes()
    {
        return Storage_.DefragmentatedBytes();
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
    srand(45);
    TStrStrHashMap m(1000000);
    auto [val1, idx1] = m.Put("key1", "value1");
    verify(m.Get("key1").first == "value1"sv);
    auto [val2, idx2] = m.Put("key2", "value2");
    verify(m.Get("key2").first == "value2"sv);
    verify(m.Erase("key1"));
    verify(m.Get("key1").first.data() == nullptr);
    verify(!m.Erase("key1"));
    verify(m.Erase(idx2));
    verify(m.Get("key2").first.data() == nullptr);
    verify(!m.Erase(idx2));

    for (int i = 0; i < 94; ++i) {
        auto val = m.PutUnitialized(std::to_string(i), 10000).first;
        for (auto& e : val) {
            e = i;
        }
    }
    for (int i = 0; i < 94; ++i) {
        auto val = m.Get(std::to_string(i)).first;
        for (auto& e : val) {
            verify(e == i);
        }
    }
    for (int i = 0; i < 94; i += 2) {
        verify(m.Erase(std::to_string(i)));
    }
    for (int i = 0; i < 94; i += 2) {
        auto val = m.PutUnitialized(std::to_string(i), 10000).first;
        for (auto& e : val) {
            e = i;
        }
    }
    for (int i = 0; i < 94; ++i) {
        auto val = m.Get(std::to_string(i)).first;
        for (auto& e : val) {
            verify(e == i);
        }
    }
    for (int i = 0; i < 94; i += 2) {
        verify(m.Erase(std::to_string(i)));
    }
    for (int i = 100; i < 120; ++i) {
        auto val = m.PutUnitialized(std::to_string(i), 20000).first;
        for (auto& e : val) {
            e = i;
        }
    }
    for (int i = 1; i < 94; i += 2) {
        auto val = m.Get(std::to_string(i)).first;
        for (auto& e : val) {
            verify(e == i);
        }
    }

    m.Clear();
}

void SSHM_StressTest()
{
    srand(45);
    TStrStrHashMap m(1'000'000'000);
    const int N = 4500000;
    std::vector<bool> filled(N, false);
    std::vector<std::string> keys(N);
    std::vector<char> valueData(1'000'000);
    for (auto& c : valueData) {
        c = rand() % 100;
    }
    std::vector<std::string_view> values(N);
    for (int i = 0; i < N; ++i) {
        keys[i] = std::to_string(i);
        keys[i].resize(keys[i].size() + rand() % 10);
        int valueSize = rand() % 200;
        if (rand() % 10 == 0) {
            valueSize = rand() % 2000;
        }
        if (rand() % 400 == 0) {
            valueSize = rand() % 20000;
        }
        if (rand() % 5000 == 0) {
            valueSize = rand() % 200000;
        }
        values[i] = std::string_view{valueData.begin(), valueData.end()}.substr(rand() % (valueData.size() - valueSize), valueSize);
    }

    {
        auto start = Now();
        for (int i = 0; i < N * 3; ++i) {
            const int j = rand() % N;
            if (rand() % 4 > 0) {
                m.Put(keys[j], values[j]);
                filled[j] = true;
            } else {
                auto [val, idx] = m.Get(keys[j]);
                if (val.data() != nullptr) {
                    verify(filled[j]);
                    verify(val == values[j]);
                } else {
                    verify(!filled[j]);
                }
                m.Erase(keys[j]);
                filled[j] = false;
            }
        }
        std::cerr << "Put-Erase " << "(Time: " << Now() - start << ", FillRate: " << m.FillRate()
            << ", Rss: " << Rss() << ", DefragmentatedBytes:" << m.DefragmentatedBytes() << ")" << std::endl;
    }
    {
        auto start = Now();
        for (int i = 0; i < 3000000; ++i) {
            const int j = rand() % N;
            auto [val, idx] = m.Get(keys[j]);
        }
        std::cerr << "Get " << "(Time: " << Now() - start << ", FillRate: " << m.FillRate() << ", Rss: " << Rss() << ")" << std::endl;
    }
    // Change pattern.
    {
        auto start = Now();
        int J = 10;
        uint64_t sz = 0;
        for (int i = 0; i < N; ++i) {
            if (filled[i]) {
                if (rand() % J != 0) {
                    sz += m.Get(keys[i]).first.size();
                    m.Erase(keys[i]);
                    filled[i] = false;
                    if (rand() % (J - 1) == 0) {
                        m.Put(keys[i], std::string_view{valueData.begin(), valueData.end()}.substr(0, sz));
                        sz = 0;
                    }
                }
            }

        }
        std::cerr << "Change-pattern " << "(J: " << J << ", Time: " << Now() - start << ", FillRate: " << m.FillRate()
            << ", Rss: " << Rss() << ", DefragmentatedBytes:" << m.DefragmentatedBytes() << ")" << std::endl;
    }
}

void test_bitmask()
{
    constexpr int N = 1024;
    auto mask = TBitMask<N>();
    for (int i = 0; i < N; ++i) {
        verify(mask.Find(i) == -1);
        mask.Set(i);
        for (int j = 0; j <= i; ++j) {
            verify(mask.Find(j) == i);
        }
        for (int j = i + 1; j < N; ++j) {
            verify(mask.Find(j) == -1);
        }
        mask.Reset(i);
    }

}

void test_rank()
{
    verify(GetRank(TBlobStringsStorage::MaxSize) < 640);
    for (int i = 0; i < 1000000; ++i) {
        verify(GetRank(i) <= GetRank(i + 1));
    }
}

void show_rank()
{
    for (long long i = 0; i < 128; i += 1) {
        std::cout << i << " " << GetRank(i) << std::endl;
    }
    for (long long i = 16; i < 128; i += 16) {
        std::cout << i << " " << GetRank(i) << std::endl;
    }
    for (long long i = 128; i < 8096; i += 256) {
        std::cout << i << " " << GetRank(i) << std::endl;
    }
    for (long long i = 8096; i < 8000000; i += 1000000) {
        std::cout << i << " " << GetRank(i) << std::endl;
    }
    for (long long i : {100000000}) {
        std::cout << i << " " << GetRank(i) << std::endl;
    }
}

int main()
{
    std::cerr << RunDesc << "\nStart tests" << std::endl;
    test_rank();
    test_bitmask();
    SS_SimpleTest();
    SSHM_SimpleTest();
    SSHM_StressTest();
    std::cerr << "Finish tests" << std::endl;
    // show_rank();
    std::cerr << "Finish" << std::endl;
    return 0;
}
