#ifndef BLUESTORE_DB_HASH_H
#define BLUESTORE_DB_HASH_H


#include "kv/RocksDBStore.h"

class BlueStore_DB_Hash : public KeyValueDB {
  friend class BlueStore;
public:
  typedef std::map<std::string, size_t> ShardingSchema;
  class HashSharded_TransactionImpl;
  class WholeSpaceIteratorMerged_Impl;
  class SinglePrefixIteratorMerged_Impl;
  class PrefixIteratorImpl;
private:
  RocksDBStore* db;
  CephContext* cct;
  const rocksdb::Comparator* comparator;
  ShardingSchema sharding_schema;
  typedef std::map<std::string, std::vector<KeyValueDB::ColumnFamilyHandle> > ActiveShards;
  ActiveShards shards;

public:
  BlueStore_DB_Hash(RocksDBStore* db, const ShardingSchema& sharding_schema);
  virtual ~BlueStore_DB_Hash();
  void unlink_db();

private:
  int open_shards();
  KeyValueDB::ColumnFamilyHandle get_db_shard(const std::string &prefix, const char *k, size_t keylen);
  std::vector<KeyValueDB::ColumnFamilyHandle>& get_shards(const std::string &prefix);

public:
  int init(string option_str="") override;
  int open(std::ostream &out, const std::vector<ColumnFamily>& options = {}) override;
  int create_and_open(std::ostream &out, const std::vector<ColumnFamily>& new_cfs = {}) override;
  int open_read_only(std::ostream &out, const std::vector<ColumnFamily>& options = {}) override;
  int _do_open(std::ostream &out, bool read_only, const std::vector<ColumnFamily>& options = {});
  void close() override;
  int column_family_list(vector<std::string>& cf_names) override;
  int column_family_create(const std::string& cf_name, const std::string& cf_options) override;
  int column_family_delete(const std::string& cf_name) override;
  ColumnFamilyHandle column_family_handle(const std::string& cf_name) const override;
  int repair(std::ostream &out) override;
  Transaction get_transaction() override;
  int submit_transaction(Transaction t) override;
  int submit_transaction_sync(Transaction t) override;

  int get(const std::string &prefix,
          const std::set<std::string> &keys,
          std::map<std::string, bufferlist> *out) override;
  int get(const std::string &prefix,
          const std::string &key,
          bufferlist *value) override;
  int get(const string &prefix,
          const char *key, size_t keylen,
          bufferlist *value) override;
  int get(ColumnFamilyHandle cf_handle,
                  const std::string &prefix,
                  const std::set<std::string> &keys,
                  std::map<std::string, bufferlist> *out) override;
  int get(ColumnFamilyHandle cf_handle,           ///< [in] Column family handle
                  const std::string &prefix,      ///< [in] prefix or CF name
                  const std::string &key,         ///< [in] key
                  bufferlist *value) override;    ///< [out] value

  WholeSpaceIterator get_wholespace_iterator() override;
  Iterator get_iterator(const std::string &prefix) override;
  WholeSpaceIterator get_wholespace_iterator_cf(ColumnFamilyHandle cfh) override;
  Iterator get_iterator_cf(ColumnFamilyHandle cfh, const std::string &prefix) override;
  uint64_t get_estimated_size(std::map<std::string,uint64_t> &extra) override;
  int get_statfs(struct store_statfs_t *buf) override;
  int set_cache_size(uint64_t) override;
  int set_cache_high_pri_pool_ratio(double ratio) override;
  int64_t get_cache_usage() const override;
  /// estimate space utilization for a prefix (in bytes)
  int64_t estimate_prefix_size(const string& prefix,
                               ColumnFamilyHandle cfh = ColumnFamilyHandle()) override;
  void compact() override;
  void compact_async() override;
  void compact_prefix(const std::string& prefix) override;
  void compact_prefix_async(const std::string& prefix) override;
  void compact_range(const std::string& prefix,
                     const std::string& start, const std::string& end) override;
  void compact_range_async(const std::string& prefix,
                           const std::string& start, const std::string& end) override;
  int set_merge_operator(const std::string& prefix,
                         std::shared_ptr<MergeOperator> mop) override;
  void get_statistics(Formatter *f) override;
  int locate_column_name(const std::pair<std::string, std::string>& raw_key,
                           std::string& column_name);
};

KeyValueDB* make_BlueStore_DB_Hash(KeyValueDB* db, const BlueStore_DB_Hash::ShardingSchema& schema);

#endif
