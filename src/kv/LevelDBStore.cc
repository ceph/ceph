// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "LevelDBStore.h"

#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <errno.h>
using std::string;
#include "common/debug.h"
#include "common/perf_counters.h"

#define dout_subsys ceph_subsys_leveldb
#undef dout_prefix
#define dout_prefix *_dout << "leveldb: "

class CephLevelDBLogger : public leveldb::Logger {
  CephContext *cct;
public:
  explicit CephLevelDBLogger(CephContext *c) : cct(c) {
    cct->get();
  }
  ~CephLevelDBLogger() {
    cct->put();
  }

  // Write an entry to the log file with the specified format.
  void Logv(const char* format, va_list ap) {
    dout(1);
    char buf[65536];
    vsnprintf(buf, sizeof(buf), format, ap);
    *_dout << buf << dendl;
  }
};

leveldb::Logger *create_leveldb_ceph_logger()
{
  return new CephLevelDBLogger(g_ceph_context);
}

int LevelDBStore::init(string option_str)
{
  // init defaults.  caller can override these if they want
  // prior to calling open.
  options.write_buffer_size = g_conf->leveldb_write_buffer_size;
  options.cache_size = g_conf->leveldb_cache_size;
  options.block_size = g_conf->leveldb_block_size;
  options.bloom_size = g_conf->leveldb_bloom_size;
  options.compression_enabled = g_conf->leveldb_compression;
  options.paranoid_checks = g_conf->leveldb_paranoid;
  options.max_open_files = g_conf->leveldb_max_open_files;
  options.log_file = g_conf->leveldb_log;
  return 0;
}

int LevelDBStore::do_open(ostream &out, bool create_if_missing)
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

  if (g_conf->leveldb_log_to_ceph_log) {
    ceph_logger = new CephLevelDBLogger(g_ceph_context);
    ldoptions.info_log = ceph_logger;
  }
  
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
  plb.add_u64_counter(l_leveldb_gets, "leveldb_get", "Gets");
  plb.add_u64_counter(l_leveldb_txns, "leveldb_transaction", "Transactions");
  plb.add_time_avg(l_leveldb_get_latency, "leveldb_get_latency", "Get Latency");
  plb.add_time_avg(l_leveldb_submit_latency, "leveldb_submit_latency", "Submit Latency");
  plb.add_time_avg(l_leveldb_submit_sync_latency, "leveldb_submit_sync_latency", "Submit Sync Latency");
  plb.add_u64_counter(l_leveldb_compact, "leveldb_compact", "Compactions");
  plb.add_u64_counter(l_leveldb_compact_range, "leveldb_compact_range", "Compactions by range");
  plb.add_u64_counter(l_leveldb_compact_queue_merge, "leveldb_compact_queue_merge", "Mergings of ranges in compaction queue");
  plb.add_u64(l_leveldb_compact_queue_len, "leveldb_compact_queue_len", "Length of compaction queue");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);

  if (g_conf->leveldb_compact_on_mount) {
    derr << "Compacting leveldb store..." << dendl;
    compact();
    derr << "Finished compacting leveldb store" << dendl;
  }
  return 0;
}

int LevelDBStore::_test_init(const string& dir)
{
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::DB *db;
  leveldb::Status status = leveldb::DB::Open(options, dir, &db);
  delete db;
  return status.ok() ? 0 : -EIO;
}

LevelDBStore::~LevelDBStore()
{
  close();
  delete logger;

  // Ensure db is destroyed before dependent db_cache and filterpolicy
  db.reset();
  delete ceph_logger;
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

  if (logger)
    cct->get_perfcounters_collection()->remove(logger);
}

int LevelDBStore::submit_transaction(KeyValueDB::Transaction t)
{
  utime_t start = ceph_clock_now(g_ceph_context);
  LevelDBTransactionImpl * _t =
    static_cast<LevelDBTransactionImpl *>(t.get());
  leveldb::Status s = db->Write(leveldb::WriteOptions(), &(_t->bat));
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_leveldb_txns);
  logger->tinc(l_leveldb_submit_latency, lat);
  return s.ok() ? 0 : -1;
}

int LevelDBStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  utime_t start = ceph_clock_now(g_ceph_context);
  LevelDBTransactionImpl * _t =
    static_cast<LevelDBTransactionImpl *>(t.get());
  leveldb::WriteOptions options;
  options.sync = true;
  leveldb::Status s = db->Write(options, &(_t->bat));
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_leveldb_txns);
  logger->tinc(l_leveldb_submit_sync_latency, lat);
  return s.ok() ? 0 : -1;
}

void LevelDBStore::LevelDBTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  string key = combine_strings(prefix, k);
  size_t bllen = to_set_bl.length();
  // bufferlist::c_str() is non-constant, so we can't call c_str()
  if (to_set_bl.is_contiguous() && bllen > 0) {
    // bufferlist contains just one ptr or they're contiguous
    bat.Put(leveldb::Slice(key), leveldb::Slice(to_set_bl.buffers().front().c_str(), bllen));
  } else if ((bllen <= 32 * 1024) && (bllen > 0)) {
    // 2+ bufferptrs that are not contiguopus
    // allocate buffer on stack and copy bl contents to that buffer
    // make sure the buffer isn't too large or we might crash here...    
    char* slicebuf = (char*) alloca(bllen);
    leveldb::Slice newslice(slicebuf, bllen);
    std::list<buffer::ptr>::const_iterator pb;
    for (pb = to_set_bl.buffers().begin(); pb != to_set_bl.buffers().end(); ++pb) {
      size_t ptrlen = (*pb).length();
      memcpy((void*)slicebuf, (*pb).c_str(), ptrlen);
      slicebuf += ptrlen;
    } 
    bat.Put(leveldb::Slice(key), newslice);
  } else {
    // 2+ bufferptrs that are not contiguous, and enormous in size
    bufferlist val = to_set_bl;
    bat.Put(leveldb::Slice(key), leveldb::Slice(val.c_str(), val.length()));
  }
}

void LevelDBStore::LevelDBTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  string key = combine_strings(prefix, k);
  bat.Delete(leveldb::Slice(key));
}

void LevelDBStore::LevelDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    bat.Delete(key);
  }
}

int LevelDBStore::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  utime_t start = ceph_clock_now(g_ceph_context);
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
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_leveldb_gets);
  logger->tinc(l_leveldb_get_latency, lat);
  return 0;
}

int LevelDBStore::get(const string &prefix, 
		  const string &key,
		  bufferlist *value)
{
  assert(value && (value->length() == 0));
  utime_t start = ceph_clock_now(g_ceph_context);
  int r = 0;
  KeyValueDB::Iterator it = get_iterator(prefix);
  it->lower_bound(key);
  if (it->valid() && it->key() == key) {
    value->append(it->value_as_ptr());
  } else {
    r = -ENOENT;
  }
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_leveldb_gets);
  logger->tinc(l_leveldb_get_latency, lat);
  return r;
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
  size_t prefix_len = 0;
  
  // Find separator inside Slice
  char* separator = (char*) memchr(in.data(), 0, in.size());
  if (separator == NULL)
     return -EINVAL;
  prefix_len = size_t(separator - in.data());
  if (prefix_len >= in.size())
    return -EINVAL;

  if (prefix)
    *prefix = string(in.data(), prefix_len);
  if (key)
    *key = string(separator+1, in.size() - prefix_len - 1);
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
      logger->inc(l_leveldb_compact_queue_merge);
      break;
    }
    if (p->second >= start && p->second < end) {
      // merge with existing range to the left
      compact_queue.push_back(make_pair(p->first, end));
      compact_queue.erase(p);
      logger->inc(l_leveldb_compact_queue_merge);
      break;
    }
    ++p;
  }
  if (p == compact_queue.end()) {
    // no merge, new entry.
    compact_queue.push_back(make_pair(start, end));
    logger->set(l_leveldb_compact_queue_len, compact_queue.size());
  }
  compact_queue_cond.Signal();
  if (!compact_thread.is_started()) {
    compact_thread.create("levdbst_compact");
  }
}
