// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <set>
#include <map>
#include <string>
#include <tr1/memory>
#include <errno.h>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"

using std::string;
#include "common/perf_counters.h"
#include "KeyValueDB.h"
#include "RocksDBStore.h"


int RocksDBStore::init()
{
  options.write_buffer_size = g_conf->rocksdb_write_buffer_size;
  options.cache_size = g_conf->rocksdb_cache_size;
  options.block_size = g_conf->rocksdb_block_size;
  options.bloom_size = g_conf->rocksdb_bloom_size;
  options.compression_type = g_conf->rocksdb_compression;
  options.paranoid_checks = g_conf->rocksdb_paranoid;
  options.max_open_files = g_conf->rocksdb_max_open_files;
  options.log_file = g_conf->rocksdb_log;
  options.write_buffer_num = g_conf->rocksdb_write_buffer_num;
  options.max_background_compactions = g_conf->rocksdb_background_compactions;
  options.max_background_flushes = g_conf->rocksdb_background_flushes;
  options.target_file_size_base = g_conf->rocksdb_target_file_size_base;
  options.level0_file_num_compaction_trigger = g_conf->rocksdb_level0_file_num_compaction_trigger;
  options.level0_slowdown_writes_trigger = g_conf->rocksdb_level0_slowdown_writes_trigger;
  options.level0_stop_writes_trigger = g_conf->rocksdb_level0_stop_writes_trigger;
  options.disableDataSync = g_conf->rocksdb_disableDataSync;
  options.num_levels = g_conf->rocksdb_num_levels;
  options.disableWAL = g_conf->rocksdb_disableWAL;
  options.wal_dir = g_conf->rocksdb_wal_dir;
  return 0;
}

int RocksDBStore::do_open(ostream &out, bool create_if_missing)
{
  rocksdb::Options ldoptions;

  if (options.write_buffer_size)
    ldoptions.write_buffer_size = options.write_buffer_size;
  if (options.write_buffer_num)
    ldoptions.max_write_buffer_number = options.write_buffer_num;
  if (options.max_background_compactions)
    ldoptions.max_background_compactions = options.max_background_compactions;
  if (options.max_background_flushes)
    ldoptions.max_background_flushes = options.max_background_flushes;
  if (options.target_file_size_base)
    ldoptions.target_file_size_base = options.target_file_size_base;
  if (options.max_open_files)
    ldoptions.max_open_files = options.max_open_files;
  if (options.cache_size) {
    ldoptions.block_cache = rocksdb::NewLRUCache(options.cache_size);
  }
  if (options.block_size)
    ldoptions.block_size = options.block_size;
  if (options.bloom_size) {
    const rocksdb::FilterPolicy *_filterpolicy =
	rocksdb::NewBloomFilterPolicy(options.bloom_size);
    ldoptions.filter_policy = _filterpolicy;
    filterpolicy = _filterpolicy;
  }
  if (options.compression_type.length() == 0)
    ldoptions.compression = rocksdb::kNoCompression;
  else if(options.compression_type == "snappy")
    ldoptions.compression = rocksdb::kSnappyCompression;
  else if(options.compression_type == "zlib")
    ldoptions.compression = rocksdb::kZlibCompression;
  else if(options.compression_type == "bzip2")
    ldoptions.compression = rocksdb::kBZip2Compression;
  else
    ldoptions.compression = rocksdb::kNoCompression;
  if (options.block_restart_interval)
    ldoptions.block_restart_interval = options.block_restart_interval;

  ldoptions.error_if_exists = options.error_if_exists;
  ldoptions.paranoid_checks = options.paranoid_checks;
  ldoptions.create_if_missing = create_if_missing;

  if (options.log_file.length()) {
    rocksdb::Env *env = rocksdb::Env::Default();
    env->NewLogger(options.log_file, &ldoptions.info_log);
  }
  if(options.disableDataSync)
    ldoptions.disableDataSync = options.disableDataSync;
  if(options.num_levels)
    ldoptions.num_levels = options.num_levels;
  if(options.level0_file_num_compaction_trigger)
    ldoptions.level0_file_num_compaction_trigger = options.level0_file_num_compaction_trigger;
  if(options.level0_slowdown_writes_trigger)
    ldoptions.level0_slowdown_writes_trigger = options.level0_slowdown_writes_trigger;
  if(options.level0_stop_writes_trigger)
    ldoptions.level0_stop_writes_trigger = options.level0_stop_writes_trigger;
  if(options.wal_dir.length())
    ldoptions.wal_dir = options.wal_dir;


  //rocksdb::DB *_db;
  rocksdb::Status status = rocksdb::DB::Open(ldoptions, path, &db);
  if (!status.ok()) {
    out << status.ToString() << std::endl;
    return -EINVAL;
  }
  //db.reset(_db);

  if (g_conf->rocksdb_compact_on_mount) {
    derr << "Compacting rocksdb store..." << dendl;
    compact();
    derr << "Finished compacting rocksdb store" << dendl;
  }


  PerfCountersBuilder plb(g_ceph_context, "rocksdb", l_rocksdb_first, l_rocksdb_last);
  plb.add_u64_counter(l_rocksdb_gets, "rocksdb_get");
  plb.add_u64_counter(l_rocksdb_txns, "rocksdb_transaction");
  plb.add_u64_counter(l_rocksdb_compact, "rocksdb_compact");
  plb.add_u64_counter(l_rocksdb_compact_range, "rocksdb_compact_range");
  plb.add_u64_counter(l_rocksdb_compact_queue_merge, "rocksdb_compact_queue_merge");
  plb.add_u64(l_rocksdb_compact_queue_len, "rocksdb_compact_queue_len");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  return 0;
}

bool RocksDBStore::_test_init(const string& dir)
{
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db;
  rocksdb::Status status = rocksdb::DB::Open(options, dir, &db);
  delete db;
  return status.ok();
}

RocksDBStore::~RocksDBStore()
{
  close();
  delete logger;

  // Ensure db is destroyed before dependent db_cache and filterpolicy
  delete db;
}

void RocksDBStore::close()
{
  // stop compaction thread
  compact_queue_lock.Lock();
  if (compact_thread.is_started()) {
    compact_queue_stop = true;
    compact_queue_cond.Signal();
    compact_queue_lock.Unlock();
    compact_thread.join();
  } else {
    compact_queue_lock.Unlock();
  }

  if (logger)
    cct->get_perfcounters_collection()->remove(logger);
}

int RocksDBStore::submit_transaction(KeyValueDB::Transaction t)
{
  RocksDBTransactionImpl * _t =
    static_cast<RocksDBTransactionImpl *>(t.get());
  rocksdb::WriteOptions woptions;
  woptions.disableWAL = options.disableWAL;
  rocksdb::Status s = db->Write(woptions, _t->bat);
  logger->inc(l_rocksdb_txns);
  return s.ok() ? 0 : -1;
}

int RocksDBStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  RocksDBTransactionImpl * _t =
    static_cast<RocksDBTransactionImpl *>(t.get());
  rocksdb::WriteOptions woptions;
  woptions.sync = true;
  woptions.disableWAL = options.disableWAL;
  rocksdb::Status s = db->Write(woptions, _t->bat);
  logger->inc(l_rocksdb_txns);
  return s.ok() ? 0 : -1;
}

RocksDBStore::RocksDBTransactionImpl::RocksDBTransactionImpl(RocksDBStore *_db)
{
  db = _db;
  bat = new rocksdb::WriteBatch();
}
RocksDBStore::RocksDBTransactionImpl::~RocksDBTransactionImpl()
{
  delete bat;
}
void RocksDBStore::RocksDBTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  buffers.push_back(to_set_bl);
  buffers.rbegin()->rebuild();
  bufferlist &bl = *(buffers.rbegin());
  string key = combine_strings(prefix, k);
  keys.push_back(key);
  bat->Delete(rocksdb::Slice(*(keys.rbegin())));
  bat->Put(rocksdb::Slice(*(keys.rbegin())),
	  rocksdb::Slice(bl.c_str(), bl.length()));
}

void RocksDBStore::RocksDBTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  string key = combine_strings(prefix, k);
  keys.push_back(key);
  bat->Delete(rocksdb::Slice(*(keys.rbegin())));
}

void RocksDBStore::RocksDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    keys.push_back(key);
    bat->Delete(*(keys.rbegin()));
  }
}

int RocksDBStore::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  KeyValueDB::Iterator it = get_iterator(prefix);
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    it->lower_bound(*i);
    if (it->valid() && it->key() == *i) {
      out->insert(make_pair(*i, it->value()));
    } else if (!it->valid())
      break;
  }
  logger->inc(l_rocksdb_gets);
  return 0;
}

string RocksDBStore::combine_strings(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(0);
  out.append(value);
  return out;
}

bufferlist RocksDBStore::to_bufferlist(rocksdb::Slice in)
{
  bufferlist bl;
  bl.append(bufferptr(in.data(), in.size()));
  return bl;
}

int RocksDBStore::split_key(rocksdb::Slice in, string *prefix, string *key)
{
  string in_prefix = in.ToString();
  size_t prefix_len = in_prefix.find('\0');
  if (prefix_len >= in_prefix.size())
    return -EINVAL;

  if (prefix)
    *prefix = string(in_prefix, 0, prefix_len);
  if (key)
    *key= string(in_prefix, prefix_len + 1);
  return 0;
}

void RocksDBStore::compact()
{
  logger->inc(l_rocksdb_compact);
  db->CompactRange(NULL, NULL);
}


void RocksDBStore::compact_thread_entry()
{
  compact_queue_lock.Lock();
  while (!compact_queue_stop) {
    while (!compact_queue.empty()) {
      pair<string,string> range = compact_queue.front();
      compact_queue.pop_front();
      logger->set(l_rocksdb_compact_queue_len, compact_queue.size());
      compact_queue_lock.Unlock();
      logger->inc(l_rocksdb_compact_range);
      compact_range(range.first, range.second);
      compact_queue_lock.Lock();
      continue;
    }
    compact_queue_cond.Wait(compact_queue_lock);
  }
  compact_queue_lock.Unlock();
}

void RocksDBStore::compact_range_async(const string& start, const string& end)
{
  Mutex::Locker l(compact_queue_lock);

  // try to merge adjacent ranges.  this is O(n), but the queue should
  // be short.  note that we do not cover all overlap cases and merge
  // opportunities here, but we capture the ones we currently need.
  list< pair<string,string> >::iterator p = compact_queue.begin();
  while (p != compact_queue.end()) {
    if (p->first == start && p->second == end) {
      // dup; no-op
      return;
    }
    if (p->first <= end && p->first > start) {
      // merge with existing range to the right
      compact_queue.push_back(make_pair(start, p->second));
      compact_queue.erase(p);
      logger->inc(l_rocksdb_compact_queue_merge);
      break;
    }
    if (p->second >= start && p->second < end) {
      // merge with existing range to the left
      compact_queue.push_back(make_pair(p->first, end));
      compact_queue.erase(p);
      logger->inc(l_rocksdb_compact_queue_merge);
      break;
    }
    ++p;
  }
  if (p == compact_queue.end()) {
    // no merge, new entry.
    compact_queue.push_back(make_pair(start, end));
    logger->set(l_rocksdb_compact_queue_len, compact_queue.size());
  }
  compact_queue_cond.Signal();
  if (!compact_thread.is_started()) {
    compact_thread.create();
  }
}
bool RocksDBStore::check_omap_dir(string &omap_dir)
{
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db;
  rocksdb::Status status = rocksdb::DB::Open(options, omap_dir, &db);
  delete db;
  return status.ok();
}
void RocksDBStore::compact_range(const string& start, const string& end)
{
    rocksdb::Slice cstart(start);
    rocksdb::Slice cend(end);
    db->CompactRange(&cstart, &cend);
}
RocksDBStore::RocksDBWholeSpaceIteratorImpl::~RocksDBWholeSpaceIteratorImpl()
{
  delete dbiter;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_first()
{
  dbiter->SeekToFirst();
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_first(const string &prefix)
{
  rocksdb::Slice slice_prefix(prefix);
  dbiter->Seek(slice_prefix);
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_last()
{
  dbiter->SeekToLast();
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_last(const string &prefix)
{
  string limit = past_prefix(prefix);
  rocksdb::Slice slice_limit(limit);
  dbiter->Seek(slice_limit);

  if (!dbiter->Valid()) {
    dbiter->SeekToLast();
  } else {
    dbiter->Prev();
  }
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::upper_bound(const string &prefix, const string &after)
{
  lower_bound(prefix, after);
  if (valid()) {
  pair<string,string> key = raw_key();
    if (key.first == prefix && key.second == after)
      next();
  }
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::lower_bound(const string &prefix, const string &to)
{
  string bound = combine_strings(prefix, to);
  rocksdb::Slice slice_bound(bound);
  dbiter->Seek(slice_bound);
  return dbiter->status().ok() ? 0 : -1;
}
bool RocksDBStore::RocksDBWholeSpaceIteratorImpl::valid()
{
  return dbiter->Valid();
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::next()
{
  if (valid())
  dbiter->Next();
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::prev()
{
  if (valid())
    dbiter->Prev();
    return dbiter->status().ok() ? 0 : -1;
}
string RocksDBStore::RocksDBWholeSpaceIteratorImpl::key()
{
  string out_key;
  split_key(dbiter->key(), 0, &out_key);
  return out_key;
}
pair<string,string> RocksDBStore::RocksDBWholeSpaceIteratorImpl::raw_key()
{
  string prefix, key;
  split_key(dbiter->key(), &prefix, &key);
  return make_pair(prefix, key);
}
bufferlist RocksDBStore::RocksDBWholeSpaceIteratorImpl::value()
{
  return to_bufferlist(dbiter->value());
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::status()
{
  return dbiter->status().ok() ? 0 : -1;
}

bool RocksDBStore::in_prefix(const string &prefix, rocksdb::Slice key)
{
  return (key.compare(rocksdb::Slice(past_prefix(prefix))) < 0) &&
    (key.compare(rocksdb::Slice(prefix)) > 0);
}
string RocksDBStore::past_prefix(const string &prefix)
{
  string limit = prefix;
  limit.push_back(1);
  return limit;
}


RocksDBStore::WholeSpaceIterator RocksDBStore::_get_iterator()
{
  return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new RocksDBWholeSpaceIteratorImpl(
      db->NewIterator(rocksdb::ReadOptions())
    )
  );
}

RocksDBStore::WholeSpaceIterator RocksDBStore::_get_snapshot_iterator()
{
  const rocksdb::Snapshot *snapshot;
  rocksdb::ReadOptions options;

  snapshot = db->GetSnapshot();
  options.snapshot = snapshot;

  return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new RocksDBSnapshotIteratorImpl(db, snapshot,
      db->NewIterator(options))
  );
}

RocksDBStore::RocksDBSnapshotIteratorImpl::~RocksDBSnapshotIteratorImpl()
{
  db->ReleaseSnapshot(snapshot);
}
