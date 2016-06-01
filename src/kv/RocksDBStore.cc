// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <set>
#include <map>
#include <string>
#include <memory>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/env.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/convenience.h"
#include "rocksdb/merge_operator.h"
using std::string;
#include "common/perf_counters.h"
#include "common/debug.h"
#include "include/str_list.h"
#include "include/str_map.h"
#include "KeyValueDB.h"
#include "RocksDBStore.h"

#include "common/debug.h"

#define dout_subsys ceph_subsys_rocksdb
#undef dout_prefix
#define dout_prefix *_dout << "rocksdb: "

//
// One of these per rocksdb instance, implements the merge operator prefix stuff
//
class RocksDBStore::MergeOperatorRouter : public rocksdb::AssociativeMergeOperator {
  RocksDBStore& store;
  public:
  const char *Name() const {
    // Construct a name that rocksDB will validate against. We want to
    // do this in a way that doesn't constrain the ordering of calls
    // to set_merge_operator, so sort the merge operators and then
    // construct a name from all of those parts.
    store.assoc_name.clear();
    map<std::string,std::string> names;
    for (auto& p : store.merge_ops) names[p.first] = p.second->name();
    for (auto& p : names) {
      store.assoc_name += '.';
      store.assoc_name += p.first;
      store.assoc_name += ':';
      store.assoc_name += p.second;
    }
    return store.assoc_name.c_str();
  }

  MergeOperatorRouter(RocksDBStore &_store) : store(_store) {}

  virtual bool Merge(const rocksdb::Slice& key,
                     const rocksdb::Slice* existing_value,
                     const rocksdb::Slice& value,
                     std::string* new_value,
                     rocksdb::Logger* logger) const {
    // Check each prefix
    for (auto& p : store.merge_ops) {
      if (p.first.compare(0, p.first.length(),
			  key.data(), p.first.length()) == 0 &&
	  key.data()[p.first.length()] == 0) {
        if (existing_value) {
          p.second->merge(existing_value->data(), existing_value->size(),
			  value.data(), value.size(),
			  new_value);
        } else {
          p.second->merge_nonexistant(value.data(), value.size(), new_value);
        }
        break;
      }
    }
    return true; // OK :)
  }

};

int RocksDBStore::set_merge_operator(
  const string& prefix,
  std::shared_ptr<KeyValueDB::MergeOperator> mop)
{
  // If you fail here, it's because you can't do this on an open database
  assert(db == nullptr);
  merge_ops.push_back(std::make_pair(prefix,mop));
  return 0;
}

class CephRocksdbLogger : public rocksdb::Logger {
  CephContext *cct;
public:
  explicit CephRocksdbLogger(CephContext *c) : cct(c) {
    cct->get();
  }
  ~CephRocksdbLogger() {
    cct->put();
  }

  // Write an entry to the log file with the specified format.
  void Logv(const char* format, va_list ap) {
    Logv(rocksdb::INFO_LEVEL, format, ap);
  }

  // Write an entry to the log file with the specified log level
  // and format.  Any log with level under the internal log level
  // of *this (see @SetInfoLogLevel and @GetInfoLogLevel) will not be
  // printed.
  void Logv(const rocksdb::InfoLogLevel log_level, const char* format,
	    va_list ap) {
    int v = rocksdb::NUM_INFO_LOG_LEVELS - log_level - 1;
    dout(v);
    char buf[65536];
    vsnprintf(buf, sizeof(buf), format, ap);
    *_dout << buf << dendl;
  }
};

rocksdb::Logger *create_rocksdb_ceph_logger()
{
  return new CephRocksdbLogger(g_ceph_context);
}

int string2bool(string val, bool &b_val)
{
  if (strcasecmp(val.c_str(), "false") == 0) {
    b_val = false;
    return 0;
  } else if (strcasecmp(val.c_str(), "true") == 0) {
    b_val = true;
    return 0;
  } else {
    std::string err;
    int b = strict_strtol(val.c_str(), 10, &err);
    if (!err.empty())
      return -EINVAL;
    b_val = !!b;
    return 0;
  }
}
  
int RocksDBStore::tryInterpret(const string key, const string val, rocksdb::Options &opt)
{
  if (key == "compaction_threads") {
    std::string err;
    int f = strict_sistrtoll(val.c_str(), &err);
    if (!err.empty())
      return -EINVAL;
    //Low priority threadpool is used for compaction
    opt.env->SetBackgroundThreads(f, rocksdb::Env::Priority::LOW);
  } else if (key == "flusher_threads") {
    std::string err;
    int f = strict_sistrtoll(val.c_str(), &err);
    if (!err.empty())
      return -EINVAL;
    //High priority threadpool is used for flusher
    opt.env->SetBackgroundThreads(f, rocksdb::Env::Priority::HIGH);
  } else if (key == "compact_on_mount") {
    int ret = string2bool(val, compact_on_mount);
    if (ret != 0)
      return ret;
  } else if (key == "disableWAL") {
    int ret = string2bool(val, disableWAL);
    if (ret != 0)
      return ret;
  } else {
    //unrecognize config options.
    return -EINVAL;
  }
  return 0;
}

int RocksDBStore::ParseOptionsFromString(const string opt_str, rocksdb::Options &opt)
{
  map<string, string> str_map;
  int r = get_str_map(opt_str, &str_map, ",\n;");
  if (r < 0)
    return r;
  map<string, string>::iterator it;
  for(it = str_map.begin(); it != str_map.end(); ++it) {
    string this_opt = it->first + "=" + it->second;
    rocksdb::Status status = rocksdb::GetOptionsFromString(opt, this_opt , &opt); 
    if (!status.ok()) {
      //unrecognized by rocksdb, try to interpret by ourselves.
      r = tryInterpret(it->first, it->second, opt);
      if (r < 0) {
	derr << status.ToString() << dendl;
	return -EINVAL;
      }
    }
    lgeneric_dout(cct, 0) << " set rocksdb option " << it->first
			  << " = " << it->second << dendl;
  }
  return 0;
}

int RocksDBStore::init(string _options_str)
{
  options_str = _options_str;
  rocksdb::Options opt;
  //try parse options
  if (options_str.length()) {
    int r = ParseOptionsFromString(options_str, opt);
    if (r != 0) {
      return -EINVAL;
    }
  }
  return 0;
}

int RocksDBStore::create_and_open(ostream &out)
{
  if (env) {
    unique_ptr<rocksdb::Directory> dir;
    env->NewDirectory(path, &dir);
  } else {
    int r = ::mkdir(path.c_str(), 0755);
    if (r < 0)
      r = -errno;
    if (r < 0 && r != -EEXIST) {
      derr << __func__ << " failed to create " << path << ": " << cpp_strerror(r)
	   << dendl;
      return r;
    }
  }
  return do_open(out, true);
}

int RocksDBStore::do_open(ostream &out, bool create_if_missing)
{
  rocksdb::Options opt;
  rocksdb::Status status;

  if (options_str.length()) {
    int r = ParseOptionsFromString(options_str, opt);
    if (r != 0) {
      return -EINVAL;
    }
  }
  opt.create_if_missing = create_if_missing;
  if (g_conf->rocksdb_separate_wal_dir) {
    opt.wal_dir = path + ".wal";
  }
  if (g_conf->rocksdb_db_paths.length()) {
    list<string> paths;
    get_str_list(g_conf->rocksdb_db_paths, "; \t", paths);
    for (auto& p : paths) {
      size_t pos = p.find(',');
      if (pos == std::string::npos) {
	derr << __func__ << " invalid db path item " << p << " in "
	     << g_conf->rocksdb_db_paths << dendl;
	return -EINVAL;
      }
      string path = p.substr(0, pos);
      string size_str = p.substr(pos + 1);
      uint64_t size = atoll(size_str.c_str());
      if (!size) {
	derr << __func__ << " invalid db path item " << p << " in "
	     << g_conf->rocksdb_db_paths << dendl;
	return -EINVAL;
      }
      opt.db_paths.push_back(rocksdb::DbPath(path, size));
      dout(10) << __func__ << " db_path " << path << " size " << size << dendl;
    }
  }

  if (g_conf->rocksdb_log_to_ceph_log) {
    opt.info_log.reset(new CephRocksdbLogger(g_ceph_context));
  }

  if (priv) {
    dout(10) << __func__ << " using custom Env " << priv << dendl;
    opt.env = static_cast<rocksdb::Env*>(priv);
  }

  auto cache = rocksdb::NewLRUCache(g_conf->rocksdb_cache_size);
  rocksdb::BlockBasedTableOptions bbt_opts;
  bbt_opts.block_size = g_conf->rocksdb_block_size;
  bbt_opts.block_cache = cache;
  opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbt_opts));
  dout(10) << __func__ << " set block size to " << g_conf->rocksdb_block_size
           << " cache size to " << g_conf->rocksdb_cache_size << dendl;

  opt.merge_operator.reset(new MergeOperatorRouter(*this));
  status = rocksdb::DB::Open(opt, path, &db);
  if (!status.ok()) {
    derr << status.ToString() << dendl;
    return -EINVAL;
  }

  PerfCountersBuilder plb(g_ceph_context, "rocksdb", l_rocksdb_first, l_rocksdb_last);
  plb.add_u64_counter(l_rocksdb_gets, "rocksdb_get", "Gets");
  plb.add_u64_counter(l_rocksdb_txns, "rocksdb_transaction", "Transactions");
  plb.add_time_avg(l_rocksdb_get_latency, "rocksdb_get_latency", "Get latency");
  plb.add_time_avg(l_rocksdb_submit_latency, "rocksdb_submit_latency", "Submit Latency");
  plb.add_time_avg(l_rocksdb_submit_sync_latency, "rocksdb_submit_sync_latency", "Submit Sync Latency");
  plb.add_u64_counter(l_rocksdb_compact, "rocksdb_compact", "Compactions");
  plb.add_u64_counter(l_rocksdb_compact_range, "rocksdb_compact_range", "Compactions by range");
  plb.add_u64_counter(l_rocksdb_compact_queue_merge, "rocksdb_compact_queue_merge", "Mergings of ranges in compaction queue");
  plb.add_u64(l_rocksdb_compact_queue_len, "rocksdb_compact_queue_len", "Length of compaction queue");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);

  if (compact_on_mount) {
    derr << "Compacting rocksdb store..." << dendl;
    compact();
    derr << "Finished compacting rocksdb store" << dendl;
  }
  return 0;
}

int RocksDBStore::_test_init(const string& dir)
{
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db;
  rocksdb::Status status = rocksdb::DB::Open(options, dir, &db);
  delete db;
  db = nullptr;
  return status.ok() ? 0 : -EIO;
}

RocksDBStore::~RocksDBStore()
{
  close();
  delete logger;

  // Ensure db is destroyed before dependent db_cache and filterpolicy
  delete db;
  db = nullptr;

  if (priv) {
    delete static_cast<rocksdb::Env*>(priv);
  }
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
  utime_t start = ceph_clock_now(g_ceph_context);
  RocksDBTransactionImpl * _t =
    static_cast<RocksDBTransactionImpl *>(t.get());
  rocksdb::WriteOptions woptions;
  woptions.disableWAL = disableWAL;
  rocksdb::Status s = db->Write(woptions, _t->bat);
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_rocksdb_txns);
  logger->tinc(l_rocksdb_submit_latency, lat);
  return s.ok() ? 0 : -1;
}

int RocksDBStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  utime_t start = ceph_clock_now(g_ceph_context);
  RocksDBTransactionImpl * _t =
    static_cast<RocksDBTransactionImpl *>(t.get());
  rocksdb::WriteOptions woptions;
  woptions.sync = true;
  woptions.disableWAL = disableWAL;
  rocksdb::Status s = db->Write(woptions, _t->bat);
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_rocksdb_txns);
  logger->tinc(l_rocksdb_submit_sync_latency, lat);
  return s.ok() ? 0 : -1;
}
int RocksDBStore::get_info_log_level(string info_log_level)
{
  if (info_log_level == "debug") {
    return 0;
  } else if (info_log_level == "info") {
    return 1;
  } else if (info_log_level == "warn") {
    return 2;
  } else if (info_log_level == "error") {
    return 3;
  } else if (info_log_level == "fatal") {
    return 4;
  } else {
    return 1;
  }
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
  string key = combine_strings(prefix, k);

  // bufferlist::c_str() is non-constant, so we can't call c_str()
  if (to_set_bl.is_contiguous() && to_set_bl.length() > 0) {
    bat->Put(rocksdb::Slice(key),
	     rocksdb::Slice(to_set_bl.buffers().front().c_str(),
			    to_set_bl.length()));
  } else {
    // make a copy
    bufferlist val = to_set_bl;
    bat->Put(rocksdb::Slice(key),
	     rocksdb::Slice(val.c_str(), val.length()));
  }
}

void RocksDBStore::RocksDBTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  bat->Delete(combine_strings(prefix, k));
}

void RocksDBStore::RocksDBTransactionImpl::rm_single_key(const string &prefix,
					                 const string &k)
{
  bat->SingleDelete(combine_strings(prefix, k));
}

void RocksDBStore::RocksDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    bat->Delete(combine_strings(prefix, it->key()));
  }
}

void RocksDBStore::RocksDBTransactionImpl::merge(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  string key = combine_strings(prefix, k);

  // bufferlist::c_str() is non-constant, so we can't call c_str()
  if (to_set_bl.is_contiguous() && to_set_bl.length() > 0) {
    bat->Merge(rocksdb::Slice(key),
	       rocksdb::Slice(to_set_bl.buffers().front().c_str(),
			    to_set_bl.length()));
  } else {
    // make a copy
    bufferlist val = to_set_bl;
    bat->Merge(rocksdb::Slice(key),
	     rocksdb::Slice(val.c_str(), val.length()));
  }
}

int RocksDBStore::get(
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
  logger->inc(l_rocksdb_gets);
  logger->tinc(l_rocksdb_get_latency, lat);
  return 0;
}

int RocksDBStore::get(
    const string &prefix,
    const string &key,
    bufferlist *out)
{
  assert(out && (out->length() == 0));
  utime_t start = ceph_clock_now(g_ceph_context);
  int r = 0;
  KeyValueDB::Iterator it = get_iterator(prefix);
  it->lower_bound(key);
  if (it->valid() && it->key() == key) {
    out->append(it->value_as_ptr());
  } else {
    r = -ENOENT;
  }
  utime_t lat = ceph_clock_now(g_ceph_context) - start;
  logger->inc(l_rocksdb_gets);
  logger->tinc(l_rocksdb_get_latency, lat);
  return r;
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
  size_t prefix_len = 0;
  
  // Find separator inside Slice
  char* separator = (char*) memchr(in.data(), 0, in.size());
  if (separator == NULL)
     return -EINVAL;
  prefix_len = size_t(separator - in.data());
  if (prefix_len >= in.size())
    return -EINVAL;

  // Fetch prefix and/or key directly from Slice
  if (prefix)
    *prefix = string(in.data(), prefix_len);
  if (key)
    *key = string(separator+1, in.size()-prefix_len-1);
  return 0;
}

void RocksDBStore::compact()
{
  logger->inc(l_rocksdb_compact);
  rocksdb::CompactRangeOptions options;
  db->CompactRange(options, nullptr, nullptr);
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
    compact_thread.create("rstore_commpact");
  }
}
bool RocksDBStore::check_omap_dir(string &omap_dir)
{
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db;
  rocksdb::Status status = rocksdb::DB::Open(options, omap_dir, &db);
  delete db;
  db = nullptr;
  return status.ok();
}
void RocksDBStore::compact_range(const string& start, const string& end)
{
  rocksdb::CompactRangeOptions options;
  rocksdb::Slice cstart(start);
  rocksdb::Slice cend(end);
  db->CompactRange(options, &cstart, &cend);
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

bool RocksDBStore::RocksDBWholeSpaceIteratorImpl::raw_key_is_prefixed(const string &prefix) {
  // Look for "prefix\0" right in rocksb::Slice
  rocksdb::Slice key = dbiter->key();
  if ((key.size() > prefix.length()) && (key[prefix.length()] == '\0')) {
    return memcmp(key.data(), prefix.c_str(), prefix.length()) == 0;
  } else {
    return false;
  }
}

bufferlist RocksDBStore::RocksDBWholeSpaceIteratorImpl::value()
{
  return to_bufferlist(dbiter->value());
}

bufferptr RocksDBStore::RocksDBWholeSpaceIteratorImpl::value_as_ptr()
{
  rocksdb::Slice val = dbiter->value();
  return bufferptr(val.data(), val.size());
}

int RocksDBStore::RocksDBWholeSpaceIteratorImpl::status()
{
  return dbiter->status().ok() ? 0 : -1;
}

string RocksDBStore::past_prefix(const string &prefix)
{
  string limit = prefix;
  limit.push_back(1);
  return limit;
}

RocksDBStore::WholeSpaceIterator RocksDBStore::_get_iterator()
{
  return std::make_shared<RocksDBWholeSpaceIteratorImpl>(
        db->NewIterator(rocksdb::ReadOptions()));
}

RocksDBStore::WholeSpaceIterator RocksDBStore::_get_snapshot_iterator()
{
  const rocksdb::Snapshot *snapshot;
  rocksdb::ReadOptions options;

  snapshot = db->GetSnapshot();
  options.snapshot = snapshot;

  return std::make_shared<RocksDBSnapshotIteratorImpl>(
          db, snapshot, db->NewIterator(options));
}

RocksDBStore::RocksDBSnapshotIteratorImpl::~RocksDBSnapshotIteratorImpl()
{
  db->ReleaseSnapshot(snapshot);
}
