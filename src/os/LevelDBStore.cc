// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "LevelDBStore.h"

#include <set>
#include <map>
#include <string>
#include <tr1/memory>
#include <errno.h>
using std::string;
#include "common/perf_counters.h"

int LevelDBStore::init(ostream &out, bool create_if_missing)
{
  leveldb::Options ldoptions;

  if (options.write_buffer_size)
    ldoptions.write_buffer_size = options.write_buffer_size;
  if (options.max_open_files)
    ldoptions.max_open_files = options.max_open_files;
  if (options.cache_size) {
    leveldb::Cache *_db_cache = leveldb::NewLRUCache(options.cache_size);
    db_cache.reset(_db_cache);
    ldoptions.block_cache = db_cache.get();
  }
  if (options.block_size)
    ldoptions.block_size = options.block_size;
  if (options.bloom_size) {
#ifdef HAVE_LEVELDB_FILTER_POLICY
    const leveldb::FilterPolicy *_filterpolicy =
	leveldb::NewBloomFilterPolicy(options.bloom_size);
    filterpolicy.reset(_filterpolicy);
    ldoptions.filter_policy = filterpolicy.get();
#else
    assert(0 == "bloom size set but installed leveldb doesn't support bloom filters");
#endif
  }
  if (options.compression_enabled)
    ldoptions.compression = leveldb::kSnappyCompression;
  else
    ldoptions.compression = leveldb::kNoCompression;
  if (options.block_restart_interval)
    ldoptions.block_restart_interval = options.block_restart_interval;

  ldoptions.error_if_exists = options.error_if_exists;
  ldoptions.paranoid_checks = options.paranoid_checks;
  ldoptions.create_if_missing = create_if_missing;

  if (options.log_file.length()) {
    leveldb::Env *env = leveldb::Env::Default();
    env->NewLogger(options.log_file, &ldoptions.info_log);
  }

  leveldb::DB *_db;
  leveldb::Status status = leveldb::DB::Open(ldoptions, path, &_db);
  db.reset(_db);
  if (!status.ok()) {
    out << status.ToString() << std::endl;
    return -EINVAL;
  }

  PerfCountersBuilder plb(g_ceph_context, "leveldb", l_leveldb_first, l_leveldb_last);
  plb.add_u64_counter(l_leveldb_gets, "leveldb_get");
  plb.add_u64_counter(l_leveldb_txns, "leveldb_transaction");
  plb.add_u64_counter(l_leveldb_compact, "leveldb_compact");
  plb.add_u64_counter(l_leveldb_compact_range, "leveldb_compact_range");
  plb.add_u64_counter(l_leveldb_compact_queue_merge, "leveldb_compact_queue_merge");
  plb.add_u64(l_leveldb_compact_queue_len, "leveldb_compact_queue_len");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  return 0;
}

LevelDBStore::~LevelDBStore()
{
  close();
  delete logger;
}

void LevelDBStore::close()
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

  cct->get_perfcounters_collection()->remove(logger);
}

int LevelDBStore::submit_transaction(KeyValueDB::Transaction t)
{
  LevelDBTransactionImpl * _t =
    static_cast<LevelDBTransactionImpl *>(t.get());
  leveldb::Status s = db->Write(leveldb::WriteOptions(), &(_t->bat));
  logger->inc(l_leveldb_txns);
  return s.ok() ? 0 : -1;
}

int LevelDBStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  LevelDBTransactionImpl * _t =
    static_cast<LevelDBTransactionImpl *>(t.get());
  leveldb::WriteOptions options;
  options.sync = true;
  leveldb::Status s = db->Write(options, &(_t->bat));
  logger->inc(l_leveldb_txns);
  return s.ok() ? 0 : -1;
}

void LevelDBStore::LevelDBTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  buffers.push_back(to_set_bl);
  buffers.rbegin()->rebuild();
  bufferlist &bl = *(buffers.rbegin());
  string key = combine_strings(prefix, k);
  keys.push_back(key);
  bat.Delete(leveldb::Slice(*(keys.rbegin())));
  bat.Put(leveldb::Slice(*(keys.rbegin())),
	  leveldb::Slice(bl.c_str(), bl.length()));
}

void LevelDBStore::LevelDBTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  string key = combine_strings(prefix, k);
  keys.push_back(key);
  bat.Delete(leveldb::Slice(*(keys.rbegin())));
}

void LevelDBStore::LevelDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    keys.push_back(key);
    bat.Delete(*(keys.rbegin()));
  }
}

int LevelDBStore::get(
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
  logger->inc(l_leveldb_gets);
  return 0;
}

string LevelDBStore::combine_strings(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(0);
  out.append(value);
  return out;
}

bufferlist LevelDBStore::to_bufferlist(leveldb::Slice in)
{
  bufferlist bl;
  bl.append(bufferptr(in.data(), in.size()));
  return bl;
}

int LevelDBStore::split_key(leveldb::Slice in, string *prefix, string *key)
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

void LevelDBStore::compact()
{
  logger->inc(l_leveldb_compact);
  db->CompactRange(NULL, NULL);
}


void LevelDBStore::compact_thread_entry()
{
  compact_queue_lock.Lock();
  while (!compact_queue_stop) {
    while (!compact_queue.empty()) {
      pair<string,string> range = compact_queue.front();
      compact_queue.pop_front();
      logger->set(l_leveldb_compact_queue_len, compact_queue.size());
      compact_queue_lock.Unlock();
      logger->inc(l_leveldb_compact_range);
      compact_range(range.first, range.second);
      compact_queue_lock.Lock();
      continue;
    }
    compact_queue_cond.Wait(compact_queue_lock);
  }
  compact_queue_lock.Unlock();
}

void LevelDBStore::compact_range_async(const string& start, const string& end)
{
  Mutex::Locker l(compact_queue_lock);

  // try to merge adjacent ranges.  this is O(n), but the queue should
  // be short.  note that we do not cover all overlap cases and merge
  // opportunities here, but we capture the ones we currently need.
  list< pair<string,string> >::iterator p;
  for (p = compact_queue.begin(); p != compact_queue.end(); ++p) {
    if (p->first == start && p->second == end) {
      // dup; no-op
      return;
    } else if (p->first <= end && p->first > start) {
      // merge with existing range to the right
      compact_queue.push_back(make_pair(start, p->second));
      compact_queue.erase(p);
      logger->inc(l_leveldb_compact_queue_merge);
      break;
    } else if (p->second >= start && p->second < end) {
      // merge with existing range to the left
      compact_queue.push_back(make_pair(p->first, end));
      compact_queue.erase(p);
      logger->inc(l_leveldb_compact_queue_merge);
      break;
    } else {
      ++p;
    }
  }
  if (p == compact_queue.end()) {
    // no merge, new entry.
    compact_queue.push_back(make_pair(start, end));
    logger->set(l_leveldb_compact_queue_len, compact_queue.size());
  }
  compact_queue_cond.Signal();
  if (!compact_thread.is_started()) {
    compact_thread.create();
  }
}
