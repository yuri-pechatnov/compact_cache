#include <unordered_map>
#include <string_view>
#include <cstring>
#include <memory>
#include <iostream>
#include <vector>
#include <optional>

template <typename TMoveObserver, typename THasher = std::hash<std::string_view>>
class TStringHashMapWithIterators;

struct Node
{
    explicit Node(std::byte* prev, std::byte* next, size_t key_length, size_t value_length) :
            prev_{reinterpret_cast<Node*>(prev)}, next_{reinterpret_cast<Node*>(next)},
            key_length_{key_length}, value_length_{value_length}
    {

    }

    Node* prev_ = nullptr;
    Node* next_ = nullptr;
    size_t key_length_ = 0;
    size_t value_length_ = 0;

    static Node* get_start_ptr(Node* node_ptr) {return node_ptr;}
    static std::byte* get_end_ptr(Node* node_ptr)
    {return reinterpret_cast<std::byte*>(node_ptr) + sizeof(Node) + node_ptr->key_length_ + node_ptr->value_length_;}

};

template <typename TMoveObserver, typename THasher = std::hash<std::string_view>>
class TStringHashMap {
public:
    inline constexpr static size_t const_node_part_size_ = sizeof(Node);

    explicit TStringHashMap(size_t totalBufferSize) : buffer_{new std::byte[totalBufferSize]},
        buffer_end_{buffer_.get() + totalBufferSize}, free_area_begin_{buffer_.get()}
    {
        
    }

    TStringHashMap(const TStringHashMap& other) = delete;
    TStringHashMap(TStringHashMap&& other) = delete;

    TStringHashMap& operator=(const TStringHashMap& other) = delete;
    TStringHashMap& operator=(TStringHashMap&& other) = delete;

    ~TStringHashMap()
    {
        Node* cur = reinterpret_cast<Node*>(head_);
        while (cur)
        {
            cur->~Node();
            cur = cur->next_;
        }
    }



    Node* Insert(std::string_view key, std::string_view value)
    {
        const size_t key_size = key.size();
        const size_t value_size = value.size();

        const size_t total_size = const_node_part_size_ + key_size + value_size;

        if (FreeSpace() <= total_size)
        {
            throw;
        }

        Node* new_node = new(free_area_begin_) Node{tail_, nullptr, key.size(), value.size()};
        if (!head_)
        {
            head_ = reinterpret_cast<std::byte*>(new_node);
        }

        key.copy(reinterpret_cast<char*>(free_area_begin_ + const_node_part_size_), key_size);
        value.copy(reinterpret_cast<char*>(free_area_begin_ + const_node_part_size_ + key_size), value_size);

        free_area_begin_ += total_size;

        std::string_view key_in_buffer_view{reinterpret_cast<char*>(new_node) + const_node_part_size_, key_size};
        // std::string_view value_in_buffer_view{new_node + Node::const_node_part_size_ + key_size, value_size};
        hash_map_[key_in_buffer_view] = new_node;

        // tell TMoveObserver about occupied memory

        return new_node;
    }

    Node* Erase(std::string_view key)
    {
        // Can we ask for a non-existent key?
        Node* node = hash_map_.at(key); 

        if (node->prev_)
            node->prev_->next_ = node->next_;

        if (node->next_)
            node->next_->prev_ = node->prev_;

        memset(reinterpret_cast<std::byte*>(node) + const_node_part_size_, 0, node->key_length_ + node->value_length_);
        node->~Node();

        // tell TMoveObserver about freed memory

        hash_map_.erase(key);
        return node;
    }

    std::string_view Get(std::string_view key)
    {
        // Can we ask for a non-existent key?
        if (!hash_map_.count(key))
            return "\0";
        const Node* node = hash_map_[key];

        // tell TMoveObserver that we accessed this key

        return std::string_view{reinterpret_cast<const char*>(node) + const_node_part_size_ + node->key_length_, node->value_length_};
    }

    size_t FreeSpace() const 
    {
        return static_cast<size_t>(buffer_end_ - free_area_begin_);
    }
    size_t TotalSpace() const
    {
        return static_cast<size_t>(buffer_end_ - buffer_.get());
    }

private:
    std::unordered_map<std::string_view, Node*, THasher> hash_map_;

    std::unique_ptr<std::byte> buffer_ = nullptr;
    const std::byte* buffer_end_ = nullptr;
    std::byte* free_area_begin_ = nullptr;

    std::byte* head_ = nullptr;
    std::byte* tail_ = nullptr;

    // std::map<std::pair<size_t, std::byte*>> memory_pieces_;

    friend class TStringHashMapWithIterators<TMoveObserver, THasher>;
    // Малое число крупных аллокаций, в идеале вообще одна
    // Примерное устройство
    // Meta + block with intrusive nodes of linked list + hashtable(hash -> start node)

    // ......................................
    // ...[ptrToPrev:8][ptrToNext:8][keyLength:8][valueLength:8][key:*][value:*]....     

    // Если не хватило места - политики
};

template <typename TMoveObserver, typename THasher>
class TStringHashMapWithIterators {
public:
    explicit TStringHashMapWithIterators(size_t totalBufferSize, TMoveObserver& obs) :
    hash_map_{totalBufferSize},
    move_observer_{obs}
    {

    }

    size_t Insert(std::string_view key, std::string_view value)
    {
        Node* node = hash_map_.Insert(key, value);
        auxiliary_vector_.push_back(node);

        move_observer_.OnElementAdd(auxiliary_vector_.size() - 1, node);
        return auxiliary_vector_.size() - 1;
    }

    void Erase(std::string_view key)
    {
        Node* erased_node = hash_map_.Erase(key);
        auto it = std::find(auxiliary_vector_.begin(), auxiliary_vector_.end(), erased_node);

        *it = nullptr;
    }

    void Erase(size_t iter)
    {
        const Node* node_to_erase = auxiliary_vector_[iter];
        const size_t key_length = node_to_erase->key_length_;
        const char* key_begin = reinterpret_cast<const char*>(node_to_erase) +
                TStringHashMap<TMoveObserver, THasher>::const_node_part_size_;
        hash_map_.Erase(std::string_view{key_begin, key_length});

        auxiliary_vector_[iter] = nullptr;
    }

    size_t Find(std::string_view key)
    {
        const Node* found_node_ptr = hash_map_.hash_map_.at(key);
        auto it = std::find(auxiliary_vector_.begin(), auxiliary_vector_.end(), found_node_ptr);
        return std::distance(auxiliary_vector_.begin(), it);
    }
    
    std::string_view Get(std::string_view key)
    {
        return hash_map_.Get(key);
    }

private:
    TStringHashMap<TMoveObserver, THasher> hash_map_;
    // vector[index] -> &value
    std::vector<Node*> auxiliary_vector_;
    TMoveObserver& move_observer_;

};


class TCleaner {
public:
    void OnElementAdd(size_t index, Node* valueView)
    {
        if (index > stats_.size())
        {
            stats_.resize(index + 1);
        }
        stats_[index].epoch_ = cur_epoch_;
    }

    // Упрощение - для lra стратегии с эпохами не нужны следующие три метода
    // void OnElementTouch(index)
    // void OnElementMove(index, newValueView) // to be called after move
    // void OnElementRemove(index, valueView)

    // Упрощение - если нечего удалить, то можно просто абортиться на вызове
    std::optional<size_t> GetElementToRemove()
    {
        auto it = std::find_if(stats_.begin(), stats_.end(), [this](const EntryStat& stat){
            return (cur_epoch_ - stat.epoch_) > 2;
        } );

        if (it != stats_.end())
            return it->epoch_;

        return std::nullopt;
    }

    struct EntryStat
    {
        size_t epoch_;
    };

    void set_epoch(size_t new_epoch)
    {
        cur_epoch_ = new_epoch;
    }

    size_t get_epoch() const
    {
        return cur_epoch_;
    }
private:

    std::vector<EntryStat> stats_;
    size_t cur_epoch_;
};



 template <typename TMoveObserver, typename THasher = std::hash<std::string_view>>
 class TStringCache {
 public:
     TStringCache(size_t cache_size, TCleaner& cleaner) :
     str_hash_map_{cache_size, cleaner}, cleaner_{cleaner}
     {

     }

     void Insert(std::string_view key, std::string_view value)
     {
         str_hash_map_.Insert(key, value);
     }
     std::optional<std::string_view> Get(std::string_view key)
     {
         std::string_view result = str_hash_map_.Get(key);

         if (result == "\0")
             return std::nullopt;

         return result;
     }

     // Упрощение - делать все в Insert. Если места мало - падать
//     void DoHeavyWork()

 private:
     TStringHashMapWithIterators<TMoveObserver, THasher> str_hash_map_;
     TCleaner& cleaner_;
 };


 // lra cache
 class TStateCache {
 public:

     TStateCache(size_t size, size_t epochsToMandatoryStore, TCleaner& cleaner) :
     str_cache_{size, cleaner},
     cleaner_{cleaner}
     {

     }

     void Insert(std::string_view key, std::string_view value, size_t epoch){
         if (epoch > cleaner_.get_epoch())
             StartNewEpoch(epoch);

         str_cache_.Insert(key, value);
     }
     std::optional<std::string_view> Get(std::string_view key){
         return str_cache_.Get(key);
     }

     void StartNewEpoch(size_t epoch)
     {
         cleaner_.set_epoch(epoch);
     }

 private:
     TStringCache<TCleaner> str_cache_;
     TCleaner& cleaner_;
 };