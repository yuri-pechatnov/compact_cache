```cpp
// В первой версии latency не экономим
template <typename THasher, typename TMoveObserver>
class TStringHashMap {
public:
    TStringHashMap(totalBufferSize, )

    valueView& Insert(key, value); // string_view
    void Erase(key)
    valueView& Get(key)

    size_t FreeSpace()
    size_t TotalSpace()

    // Малое число крупных аллокаций, в идеале вообще одна
    // Примерное устройство
    // Meta + block with intrusive nodes of linked list + hashtable(hash -> start node)

    // ......................................
    // ...[ptrToPrev:8][ptrToNext:8][keyLength:8][valueLength:8][key:*][value:*]....     

    // Если не хватило места - политики
};

template <typename THasher, typename TMoveObserver>
class TStringHashMapWithIterators {
    TStringHashMap(totalBufferSize, )

    iter Insert(key, value);
    void Erase(key)
    void Erase(iter)
    iter Find(key)
    valueView Get(key)
    

    // vector[index] -> &value
}


class TCleaner {
    void OnElementAdd(index, valueView)
    
    // Упрощение - для lra стратегии с эпохами не нужны следующие три метода
    void OnElementTouch(index)
    void OnElementMove(index, newValueView) // to be called after move
    void OnElementRemove(index, valueView)
    
    // Упрощение - если нечего удалить, то можно просто абортиться на вызове
    std::optional<index> GetElementToRemove();

    vector[index] -> address (-> stat)
}


template <typename THasher, typename TCleaner>
class TStringCache {
    TStringCache(size, TCleaner cleaner)

    void Insert(key, value)
    optional<value> Get(key)
    
    // Упрощение - делать все в Insert. Если места мало - падать
    void DoHeavyWork()
}


// lra cache
class TStateCache {
    TStringCache(size, epochsToMandatoryStore)

    void Insert(key, value, epoch)
    optional<value> Get(key)

    void StartNewEpoch(epoch);
}
```

План на первый этап:
* Реализовать бейзлайн максимально глупым образом через std::unordered_map, std::.... 
  Проверить интерфейсы, что они адекватны. Досогласовать их
* Написать тесты на GTEST, запустить бенчмарк
  Тут нужны логи от нас: <epoch> <keyHash> <keyLen> <valueLen>. 
  На каждое событие из лога: сделать get, сделать erase если чет было, сделать вставку.
  Целевая метрика - byte cache hit (successReturnedBytes / totalBytes).
* Написать версию с маленьким числом аллокаций - своя хешмапа с intrusive list на своем большом буффере. С дефрагментацией