#include "BlueStore.h"
#include "kv/RocksDBStore.h"
#include "include/ceph_hash.h"
#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "shard-db "


class BlueStore_DB_Hash : public KeyValueDB {
public:
  typedef std::map<std::string, size_t> ShardingSchema;

private:
  RocksDBStore* db;
  CephContext* cct;
  const rocksdb::Comparator* comparator;
  ShardingSchema sharding_schema;
  typedef std::map<std::string, std::vector<KeyValueDB::ColumnFamilyHandle> > ActiveShards;
  ActiveShards shards;

public:
  BlueStore_DB_Hash(RocksDBStore* db, const ShardingSchema& sharding_schema)
  : db(db), cct(db->cct), sharding_schema(sharding_schema) {
    comparator = db->rocksdb_options.comparator;
    ceph_assert(db);
  }
  virtual ~BlueStore_DB_Hash() {
    delete db;
  }
private:
  int open_shards() {
    for (auto& s_it: sharding_schema) {
      if (s_it.second == 0) {
        auto cf_handle = db->column_family_handle("default");
        shards[s_it.first].push_back(cf_handle);
        dout(5) << "Column family '" << s_it.first << "' handle: " << (void*)cf_handle.priv << " " << dendl;
      } else {
        for (size_t i = 0; i < s_it.second; i++) {
          std::string name = s_it.first + "-" + to_string(i);
          auto cf_handle = db->column_family_handle(name);
          dout(5) << "Column family '" << name << "' handle: " << (void*)cf_handle.priv << " " << dendl;
          shards[s_it.first].push_back(cf_handle);
        }
      }
    }
    return 0;
  }
  KeyValueDB::ColumnFamilyHandle get_db_shard(const std::string &prefix, const char *k, size_t keylen) {
    auto it = shards.find(prefix);
    ceph_assert(it != shards.end());
    unsigned hash = ceph_str_hash_linux(k, keylen);
    return it->second[hash % it->second.size()];
  }
  std::vector<KeyValueDB::ColumnFamilyHandle>& get_shards(const std::string &prefix) {
    auto it = shards.find(prefix);
    ceph_assert(it != shards.end());
    return it->second;
  }

#undef dout_context
#define dout_context db_hash.cct

  class HashSharded_TransactionImpl : public RocksDBStore::RocksDBTransactionImpl //KeyValueDB::TransactionImpl
  {
  private:
    typedef RocksDBStore::RocksDBTransactionImpl base;
    BlueStore_DB_Hash &db_hash;
  public:
    HashSharded_TransactionImpl(BlueStore_DB_Hash &db_hash)
    : RocksDBStore::RocksDBTransactionImpl(db_hash.db)
    , db_hash(db_hash) {
    }
    virtual ~HashSharded_TransactionImpl() {
    }

    void set(
      const string &prefix,
      const string &k,
      const bufferlist &bl) override {
      KeyValueDB::ColumnFamilyHandle cf = db_hash.get_db_shard(prefix, k.c_str(), k.size());
      base::select(cf);
      base::set(prefix, k, bl);
    }
    void set(
      const string &prefix,
      const char *k,
      size_t keylen,
      const bufferlist &bl) override {
      KeyValueDB::ColumnFamilyHandle cf = db_hash.get_db_shard(prefix, k, keylen);
      base::select(cf);
      base::set(prefix, k, keylen, bl);
    }
    void rmkey(
      const string &prefix,
      const string &k) override {
      KeyValueDB::ColumnFamilyHandle cf = db_hash.get_db_shard(prefix, k.c_str(), k.size());
      base::select(cf);
      base::rmkey(prefix, k);
    }
    void rmkey(
      const string &prefix,
      const char *k,
      size_t keylen) override {
      KeyValueDB::ColumnFamilyHandle cf = db_hash.get_db_shard(prefix, k, keylen);
      base::select(cf);
      base::rmkey(prefix, k, keylen);
    }
    void rm_single_key(
      const string &prefix,
      const string &k) override {
      KeyValueDB::ColumnFamilyHandle cf = db_hash.get_db_shard(prefix, k.c_str(), k.size());
      base::select(cf);
      base::rm_single_key(prefix, k);
    }
    void rmkeys_by_prefix(
      const string &prefix
      ) override {
      std::vector<KeyValueDB::ColumnFamilyHandle> &shards = db_hash.get_shards(prefix);
      for (auto &s : shards) {
        base::select(s);
        base::rmkeys_by_prefix(prefix);
      }
    }
    void rm_range_keys(
      const string &prefix,
      const string &start,
      const string &end) override {
      std::vector<KeyValueDB::ColumnFamilyHandle> &shards = db_hash.get_shards(prefix);
      for (auto &s : shards) {
        base::select(s);
        base::rm_range_keys(prefix, start, end);
      }
    }
    void merge(
      const string& prefix,
      const string& k,
      const bufferlist &bl) override {
      KeyValueDB::ColumnFamilyHandle cf = db_hash.get_db_shard(prefix, k.c_str(), k.size());
      base::select(cf);
      base::merge(prefix, k, bl);
    }
    void select(
        KeyValueDB::ColumnFamilyHandle column_family_handle) override {
      ceph_abort("Not expected");
    }
  };

#undef dout_context
#define dout_context cct

  int init(string option_str="") override {
    int r = db->init(option_str);
    return r;
  }
  int open(std::ostream &out, const std::vector<ColumnFamily>& options = {}) override {
    return _do_open(out, false, options);
  }
  int create_and_open(std::ostream &out, const std::vector<ColumnFamily>& new_cfs = {}) override {
    int r = db->create_and_open(out, new_cfs);
    if (r != 0)
      return r;
    for (auto& s_it: sharding_schema) {
      for (size_t i = 0; i < s_it.second; i++) {
        std::string name = s_it.first + "-" + to_string(i);
        r = db->column_family_create(name, "");
        if (r != 0) {
          derr << "Unable to create column family: '" << name << "' " << dendl;
          ceph_abort();
        }
      }
    }
    r = open_shards();
    return r;
  }
  int open_read_only(std::ostream &out, const std::vector<ColumnFamily>& options = {}) override {
    return _do_open(out, true, options);
  }
  int _do_open(std::ostream &out, bool read_only, const std::vector<ColumnFamily>& options = {}) {
    int r = read_only ?
      db->open_read_only(out, options) :
      db->open(out, options);
    if (r != 0)
      return r;
    vector<std::string> cf_names;
    db->column_family_list(cf_names);
    for (auto& s_it: sharding_schema) {
      for (size_t i = 0; i < s_it.second; i++) {
        std::string name = s_it.first + "-" + to_string(i);
        auto n_it = std::find(std::begin(cf_names), std::end(cf_names), name);
        if (n_it == cf_names.end()) {
          derr << "Missing column family: '" << name << "' " << dendl;
          ceph_abort();
        }
      }
    }
    r = open_shards();
    return r;
  }
  void close() override {
    db->close();
  }
  int column_family_list(vector<std::string>& cf_names) override {
    return db->column_family_list(cf_names);
  }
  int column_family_create(const std::string& cf_name, const std::string& cf_options) override {
    return db->column_family_create(cf_name, cf_options);
  }
  int column_family_delete(const std::string& cf_name) override {
    return db->column_family_delete(cf_name);
  }
  ColumnFamilyHandle column_family_handle(const std::string& cf_name) const override {
    return db->column_family_handle(cf_name);
  }
  int repair(std::ostream &out) override {
    return db->repair(out);
  }
  Transaction get_transaction() override {
    KeyValueDB::Transaction t = std::make_shared<HashSharded_TransactionImpl>(*this);
    return t;
  }
  int submit_transaction(Transaction t) override {
    return db->submit_transaction(t);
  }
  int submit_transaction_sync(Transaction t) override {
    return db->submit_transaction_sync(t);
  }

  int get(const std::string &prefix,
          const std::set<std::string> &keys,
          std::map<std::string, bufferlist> *out) override {
    std::map<void*, std::set<std::string> > sharded_keys;
    for (auto& key : keys) {
      std::string value;
      KeyValueDB::ColumnFamilyHandle cf = get_db_shard(prefix, key.c_str(), key.size());
      sharded_keys[cf.priv].emplace(key);
    }
    int r = 0;
    for (auto sh = sharded_keys.begin(); r == 0 && sh != sharded_keys.end(); sh++) {
      ColumnFamilyHandle cf;
      cf.priv = sh->first;
      db->get(cf, prefix, sh->second, out);
    }
    return r;
  }

  int get(const std::string &prefix,
          const std::string &key,
          bufferlist *value) override {
    KeyValueDB::ColumnFamilyHandle cf = get_db_shard(prefix, key.c_str(), key.size());
    return db->get(cf, prefix, key, value);
  }
  int get(const string &prefix,
          const char *key, size_t keylen,
          bufferlist *value) override {
    KeyValueDB::ColumnFamilyHandle cf = get_db_shard(prefix, key, keylen);
    std::string s_key(key, keylen);
    return db->get(cf, prefix, s_key, value);
  }

  int get(ColumnFamilyHandle cf_handle,
                  const std::string &prefix,
                  const std::set<std::string> &keys,
                  std::map<std::string, bufferlist> *out) override {
    ceph_assert(false && "invalid call");
    return 0;
  }

  int get(ColumnFamilyHandle cf_handle,///< [in] Column family handle
                  const std::string &prefix,    ///< [in] prefix or CF name
                  const std::string &key,       ///< [in] key
                  bufferlist *value) override {          ///< [out] value
    ceph_assert(false && "invalid call");
    return 0;
  }

  static int compare(const rocksdb::Comparator* comparator,
                     const std::string& a,
                     const std::string& b) {
    rocksdb::Slice _a(a.data(), a.size());
    rocksdb::Slice _b(b.data(), b.size());
    return comparator->Compare(_a, _b);
  }

  struct KeyLess {
    const rocksdb::Comparator* comparator;
    KeyLess(const rocksdb::Comparator* comparator) : comparator(comparator) { };
    bool operator()(KeyValueDB::Iterator a, KeyValueDB::Iterator b) const
    {
      if (a->valid()) {
        if (b->valid()) {
          return compare(comparator, a->key(), b->key()) < 0;
        } else {
          return true;
        }
      } else {
        if (b->valid()) {
          return false;
        } else {
          return (void*)a.get() < (void*)b.get();
        }
      }
    }
  };


  class ShardedIteratorBase {
    ssize_t position = 0;
    std::vector<KeyValueDB::Iterator> shards;
    const rocksdb::Comparator* comparator;
  public:
    ShardedIteratorBase(std::vector<KeyValueDB::Iterator>&& shards, const rocksdb::Comparator* comparator)
    : shards(std::move(shards))
    , comparator(comparator) { }

    ~ShardedIteratorBase() { }

    int open(const std::vector<KeyValueDB::Iterator>& new_shards) {
      shards = new_shards;
      return 0;
    }
    KeyValueDB::Iterator& item() {
      return shards[position];
    }
    int seek_to_last() {
      ceph_assert(false && "expected seek_to_last() not called");
      return 0;
    }
    int seek_to_first() {
      for (auto& it: shards) {
        it->seek_to_first();
      }
      position = 0;
      std::sort(shards.begin(), shards.end(), KeyLess(comparator));
      return 0;
    }
    int upper_bound(const string &after) {
      for(auto& it: shards) {
        it->upper_bound(after);
      }
      position = 0;
      std::sort(shards.begin(), shards.end(), KeyLess(comparator));
      return 0;
    }
    int lower_bound(const string &to) {
      for(auto& it: shards) {
        it->lower_bound(to);
      }
      position = 0;
      std::sort(shards.begin(), shards.end(), KeyLess(comparator));
      return 0;
    }
    bool valid() {
      if (position >= (ssize_t)shards.size() )
        return false;
      return shards[position]->valid();
    }
    /*
     * simulation of next/prev work
     * v_______________________v   ---
     * it0 it1 it2 it3 it4 it5 it6 it7
     * next
     * it1 it2 it0 it3 it4 it5 it6 it7
     * next
     * it2 it0 it3 it4 it1 it5 it6 it7
     * next
     * it2 it0 it3 it4 it1 it5 it6 it7
     * next, it2 expired
     * --- v___________________v   ---
     * it2 it0 it3 it4 it1 it5 it6 it7
     * next, it0 expired
     * --- --- v_______________v   ---
     * prev, requires checking before, revived it0
     * --- v___________________v   ---
     * it2 it0 it3 it4 it1 it5 it6 it7
     * prev, checks before, but not revives
     * it2 it0 it3 it4 it1 it5 it6 it7
    */
    int next() {
      shards[position]->next();
      if (shards[position]->valid()) {
        /* sort, as other shards may point to key that is less then
         * key that we just moved to */
        std::string key0 = shards[position]->key();
        for (size_t p = position + 1; p < shards.size(); p++) {
          std::string key1 = shards[p]->key();
          if (compare(comparator, key0, key1) < 0) {
            /* all in order, no need to sort more */
            break;
          }
          std::swap(shards[p - 1], shards[p]);
        }
      } else {
        position++;
      }
      /* signal if we iterated out of range */
      if (position >= (ssize_t)shards.size())
        return -1;
      return 0;
    }

    int prev() {
      return -1;
    }
  };

  class WholeSpaceIteratorMerged_Impl: public WholeSpaceIteratorImpl {
  private:
    BlueStore_DB_Hash &db_hash;
    ActiveShards::iterator shards_it;
    ShardedIteratorBase iter;

    void open_shards(std::vector<KeyValueDB::Iterator>& shards) {
      for (auto& it: shards_it->second) {
        Iterator wsi = db_hash.db->get_iterator_cf(it, shards_it->first);
        wsi->seek_to_first();
        if (wsi->valid())
          shards.emplace_back(wsi);
      }
    }
    void open_shards(std::vector<KeyValueDB::Iterator>& shards, const std::string& prefix) {
      shards.emplace_back(db_hash.db->get_iterator(prefix));
    }

  public:
    WholeSpaceIteratorMerged_Impl(BlueStore_DB_Hash &db_hash)
      : db_hash(db_hash)
      , iter(std::vector<KeyValueDB::Iterator>(), db_hash.comparator) {}

    virtual ~WholeSpaceIteratorMerged_Impl() {}

    int seek_to_first_existing() {
      while (shards_it != db_hash.shards.end()) {
        std::vector<KeyValueDB::Iterator> shards;
        if (shards_it->second.size() == 0) {
          /* no separate shard, is part of default column family */
          open_shards(shards, shards_it->first);
        } else {
          open_shards(shards);
        }
        iter.open(shards);
        iter.seek_to_first();
        if (iter.valid()) {
          return 0;
        }
        //this shard is empty, go next
        ++shards_it;
      };
      return -1;
    }

    int seek_to_first() override {
      shards_it = db_hash.shards.begin();
      return seek_to_first_existing();
    }
    int seek_to_first(const string &prefix) override {
      shards_it = db_hash.shards.find(prefix);
      return seek_to_first_existing();
    }
    int seek_to_last() override {
      ceph_assert(false && "expected seek_to_last() not called");
      return 0;
    }
    int seek_to_last(const string &prefix) override {
      ceph_assert(false && "expected seek_to_last() not called");
      return 0;
    }
    int upper_bound(const string &prefix, const string &after) override {
      shards_it = db_hash.shards.lower_bound(prefix);
      if (shards_it == db_hash.shards.end()) {
        return -1;
      }
      seek_to_first_existing();
      if (shards_it == db_hash.shards.end()) {
        return -1;
      }
      if (shards_it->first == prefix) {
        /* we are in intended prefix, we need more detailed upper_bound */
        iter.upper_bound(after);
        if (!iter.valid()) {
          /* nothing after target of upper_bound*/
          ++shards_it;
          return seek_to_first();
        }
      }
      return 0;
    }
    int lower_bound(const string &prefix, const string &to) override {
      shards_it = db_hash.shards.lower_bound(prefix);
      if (shards_it == db_hash.shards.end()) {
        return -1;
      }
      seek_to_first_existing();
      if (shards_it == db_hash.shards.end()) {
        return -1;
      }
      if (shards_it->first == prefix) {
        /* we are in intended prefix, we need more detailed upper_bound */
        iter.lower_bound(to);
        if (!iter.valid()) {
          /* nothing after target of upper_bound*/
          ++shards_it;
          return seek_to_first_existing();
        }
      }
      return 0;
    }
    bool valid() override {
      return iter.valid();
    }
    int next() override {
      int r = iter.next();
      if (r < 0) {
        /* if we iterated out of current shard, go on */
        ++shards_it;
        return seek_to_first_existing();
      }
      return r;
    }
    int prev() override {
      ceph_assert(false && "expected prev() not called");
      return 0;
    }
    string key() override {
      return iter.item()->key();
    }
    pair<string,string> raw_key() override {
      return iter.item()->raw_key();
    }
    bool raw_key_is_prefixed(const string &prefix) override {
      return prefix == shards_it->first;
    }
    bufferlist value() override {
      return iter.item()->value();
    }
    bufferptr value_as_ptr() override {
      return iter.item()->value_as_ptr();
    }
    int status() override {
      return iter.item()->status();
    }
  };

#undef dout_context
#define dout_context db_hash.cct

  class SinglePrefixIteratorMerged_Impl: public IteratorImpl {
  private:
    ShardedIteratorBase iter;

    static void prefix_to_iterators(
        std::vector<KeyValueDB::Iterator>& shards,
        const BlueStore_DB_Hash &db_hash,
        const string& prefix) {
      auto shards_it = db_hash.shards.find(prefix);
      if (shards_it != db_hash.shards.end()) {
        for (auto& it: shards_it->second) {
          Iterator wsi = db_hash.db->get_iterator_cf(it, shards_it->first);
          wsi->seek_to_first();
          if (wsi->valid())
            shards.emplace_back(wsi);
        }
        std::sort(shards.begin(), shards.end(), KeyLess(db_hash.comparator));
      }
    }

  public:
    SinglePrefixIteratorMerged_Impl(BlueStore_DB_Hash &db_hash, const string& prefix)
    : iter(std::vector<KeyValueDB::Iterator>(), db_hash.comparator) {
      std::vector<KeyValueDB::Iterator> shards;
      prefix_to_iterators(shards, db_hash, prefix);
      iter.open(shards);
    }

    virtual ~SinglePrefixIteratorMerged_Impl() {
    }

    int seek_to_first() override {
      return iter.seek_to_first();
    }

    int seek_to_last() override {
      ceph_assert(false && "expected seek_to_last() not called");
      return 0;
    }

    int upper_bound(const string &after) override {
      return iter.upper_bound(after);
    }

    int lower_bound(const string &to) override {
      return iter.lower_bound(to);
    }

    bool valid() override {
      return iter.valid();
    }

    int next() override {
      return iter.next();
    }

    int prev() override {
      ceph_assert(false && "expected prev() not called");
      return 0;
    }

    string key() override {
      return iter.item()->key();
    }

    pair<string,string> raw_key() override {
      return iter.item()->raw_key();
    }

    bufferlist value() override {
      return iter.item()->value();
    }

    bufferptr value_as_ptr() override {
      return iter.item()->value_as_ptr();
    }

    int status() override {
      return iter.item()->status();
    }
  };

#undef dout_context
#define dout_context cct

  // This class filters a WholeSpaceIterator by a prefix.
  class PrefixIteratorImpl : public IteratorImpl {
    const std::string prefix;
    WholeSpaceIterator generic_iter;
  public:
    PrefixIteratorImpl(const std::string &prefix, WholeSpaceIterator iter) :
      prefix(prefix), generic_iter(iter) { }
    ~PrefixIteratorImpl() override { }

    int seek_to_first() override {
      return generic_iter->seek_to_first(prefix);
    }
    int seek_to_last() override {
      return generic_iter->seek_to_last(prefix);
    }
    int upper_bound(const std::string &after) override {
      return generic_iter->upper_bound(prefix, after);
    }
    int lower_bound(const std::string &to) override {
      return generic_iter->lower_bound(prefix, to);
    }
    bool valid() override {
      if (!generic_iter->valid())
        return false;
      return generic_iter->raw_key_is_prefixed(prefix);
    }
    int next() override {
      return generic_iter->next();
    }
    int prev() override {
      return generic_iter->prev();
    }
    std::string key() override {
      return generic_iter->key();
    }
    std::pair<std::string, std::string> raw_key() override {
      return generic_iter->raw_key();
    }
    bufferlist value() override {
      return generic_iter->value();
    }
    bufferptr value_as_ptr() override {
      return generic_iter->value_as_ptr();
    }
    int status() override {
      return generic_iter->status();
    }
  };

public:

  WholeSpaceIterator get_wholespace_iterator() override {
    return std::make_shared<WholeSpaceIteratorMerged_Impl>(*this);
  }
  Iterator get_iterator(const std::string &prefix) override {
    auto shards_it = shards.find(prefix);
    if (shards_it != shards.end()) {
      return std::make_shared<SinglePrefixIteratorMerged_Impl>(*this, prefix);
    }
    return db->get_iterator(prefix);
  }
  WholeSpaceIterator get_wholespace_iterator_cf(ColumnFamilyHandle cfh) override {
    ceph_abort_msg("Not implemented"); return {};
  }
  Iterator get_iterator_cf(ColumnFamilyHandle cfh, const std::string &prefix) override {
    ceph_abort_msg("Not implemented"); return {};
  }

  uint64_t get_estimated_size(std::map<std::string,uint64_t> &extra) override {
    return db->get_estimated_size(extra);
  }
  int get_statfs(struct store_statfs_t *buf) override {
    return -EOPNOTSUPP;
  }

  int set_cache_size(uint64_t) override {
    return -EOPNOTSUPP;
  }

  int set_cache_high_pri_pool_ratio(double ratio) override {
    return db->set_cache_high_pri_pool_ratio(ratio);
  }

  int64_t get_cache_usage() const override {
    return db->get_cache_usage();
  }

  /// estimate space utilization for a prefix (in bytes)
  int64_t estimate_prefix_size(const string& prefix,
                               ColumnFamilyHandle cfh = ColumnFamilyHandle()) override {
    return 0;
  }

  void compact() override {
    db->compact();
  }
  void compact_async() override {
    db->compact_async();
  }
  void compact_prefix(const std::string& prefix) override {
    auto it = sharding_schema.find(prefix);
    if (it != sharding_schema.end()) {
      for (size_t i = 0; i < it->second; i++) {
        std::string cf_name = prefix + "-" + to_string(i);
        db->column_family_compact(cf_name, prefix, "", "");
      }
    }
  }
  void compact_prefix_async(const std::string& prefix) override {
    auto it = sharding_schema.find(prefix);
    if (it != sharding_schema.end()) {
      for (size_t i = 0; i < it->second; i++) {
        std::string cf_name = prefix + "-" + to_string(i);
        db->column_family_compact_async(cf_name, prefix, "", "");
      }
    }
  }
  void compact_range(const std::string& prefix,
                     const std::string& start, const std::string& end) override {
    auto it = sharding_schema.find(prefix);
    if (it != sharding_schema.end()) {
      for (size_t i = 0; i < it->second; i++) {
        std::string cf_name = prefix + "-" + to_string(i);
        db->column_family_compact(cf_name, prefix, start, end);
      }
    }
  }
  void compact_range_async(const std::string& prefix,
                           const std::string& start, const std::string& end) override {
    auto it = sharding_schema.find(prefix);
    if (it != sharding_schema.end()) {
      for (size_t i = 0; i < it->second; i++) {
        std::string cf_name = prefix + "-" + to_string(i);
        db->column_family_compact_async(cf_name, prefix, start, end);
      }
    }
  }

  int set_merge_operator(const std::string& prefix,
                         std::shared_ptr<MergeOperator> mop) override {
    auto it = sharding_schema.find(prefix);
    if (it != sharding_schema.end()) {
      if (it->second == 0) {
        db->set_merge_operator(prefix, mop);
      } else {
        for (size_t i = 0; i < it->second; i++) {
          std::string cf_name = prefix + "-" + to_string(i);
          db->set_merge_operator(cf_name, mop);
        }
      }
      return 0;
    }
    return -EINVAL;
  }

  void get_statistics(Formatter *f) override {
    db->get_statistics(f);
  }
};

KeyValueDB* make_BlueStore_DB_Hash(KeyValueDB* db, const BlueStore_DB_Hash::ShardingSchema& schema) {
  RocksDBStore* rdb = dynamic_cast<RocksDBStore*>(db);
  ceph_assert(db != nullptr);
  return new BlueStore_DB_Hash(rdb, schema);
}
