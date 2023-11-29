// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Myoungwon Oh <ohmyoungwon@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "include/types.h"

#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.hpp"

#include "acconfig.h"

#include "common/Cond.h"
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/obj_bencher.h"
#include "global/global_init.h"

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <errno.h>
#include <dirent.h>
#include <stdexcept>
#include <climits>
#include <locale>
#include <memory>
#include <math.h>

#include "tools/RadosDump.h"
#include "cls/cas/cls_cas_client.h"
#include "cls/cas/cls_cas_internal.h"
#include "include/stringify.h"
#include "global/signal_handler.h"
#include "common/CDC.h"
#include "common/Preforker.h"

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

using namespace std;
namespace po = boost::program_options;

struct EstimateResult {
  std::unique_ptr<CDC> cdc;

  uint64_t chunk_size;

  ceph::mutex lock = ceph::make_mutex("EstimateResult::lock");

  // < key, <count, chunk_size> >
  map< string, pair <uint64_t, uint64_t> > chunk_statistics;
  uint64_t total_bytes = 0;
  std::atomic<uint64_t> total_objects = {0};

  EstimateResult(std::string alg, int chunk_size)
    : cdc(CDC::create(alg, chunk_size)),
      chunk_size(1ull << chunk_size) {}

  void add_chunk(bufferlist& chunk, const std::string& fp_algo) {
    string fp;
    if (fp_algo == "sha1") {
      sha1_digest_t sha1_val = crypto::digest<crypto::SHA1>(chunk);
      fp = sha1_val.to_str();
    } else if (fp_algo == "sha256") {
      sha256_digest_t sha256_val = crypto::digest<crypto::SHA256>(chunk);
      fp = sha256_val.to_str();
    } else if (fp_algo == "sha512") {
      sha512_digest_t sha512_val = crypto::digest<crypto::SHA512>(chunk);
      fp = sha512_val.to_str();
    } else {
      ceph_assert(0 == "no support fingerperint algorithm");
    }

    std::lock_guard l(lock);
    auto p = chunk_statistics.find(fp);
    if (p != chunk_statistics.end()) {
      p->second.first++;
      if (p->second.second != chunk.length()) {
	cerr << "warning: hash collision on " << fp
	     << ": was " << p->second.second
	     << " now " << chunk.length() << std::endl;
      }
    } else {
      chunk_statistics[fp] = make_pair(1, chunk.length());
    }
    total_bytes += chunk.length();
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("target_chunk_size", chunk_size);

    uint64_t dedup_bytes = 0;
    uint64_t dedup_objects = chunk_statistics.size();
    for (auto& j : chunk_statistics) {
      dedup_bytes += j.second.second;
    }
    //f->dump_unsigned("dedup_bytes", dedup_bytes);
    //f->dump_unsigned("original_bytes", total_bytes);
    f->dump_float("dedup_bytes_ratio",
		  (double)dedup_bytes / (double)total_bytes);
    f->dump_float("dedup_objects_ratio",
		  (double)dedup_objects / (double)total_objects);

    uint64_t avg = total_bytes / dedup_objects;
    uint64_t sqsum = 0;
    for (auto& j : chunk_statistics) {
      sqsum += (avg - j.second.second) * (avg - j.second.second);
    }
    uint64_t stddev = sqrt(sqsum / dedup_objects);
    f->dump_unsigned("chunk_size_average", avg);
    f->dump_unsigned("chunk_size_stddev", stddev);
  }
};

map<uint64_t, EstimateResult> dedup_estimates;  // chunk size -> result

using namespace librados;
unsigned default_op_size = 1 << 26;
ceph::mutex glock = ceph::make_mutex("glock");

po::options_description make_usage() {
  po::options_description desc("Usage");
  desc.add_options()
    ("help,h", ": produce help message")
    ("op estimate --pool <POOL> --chunk-size <CHUNK_SIZE> --chunk-algorithm <ALGO> --fingerprint-algorithm <FP_ALGO>", 
     ": estimate how many chunks are redundant")
    ("op chunk-scrub --chunk-pool <POOL>",
     ": perform chunk scrub")
    ("op chunk-get-ref --chunk-pool <POOL> --object <OID> --target-ref <OID> --target-ref-pool-id <POOL_ID>",
     ": get chunk object's reference")
    ("op chunk-put-ref --chunk-pool <POOL> --object <OID> --target-ref <OID> --target-ref-pool-id <POOL_ID>",
     ": put chunk object's reference")
    ("op chunk-repair --chunk-pool <POOL> --object <OID> --target-ref <OID> --target-ref-pool-id <POOL_ID>",
     ": fix mismatched references")
    ("op dump-chunk-refs --chunk-pool <POOL> --object <OID>",
     ": dump chunk object's references")
    ("op chunk-dedup --pool <POOL> --object <OID> --chunk-pool <POOL> --fingerprint-algorithm <FP> --source-off <OFFSET> --source-length <LENGTH>",
     ": perform a chunk dedup---deduplicate only a chunk, which is a part of object.")
    ("op object-dedup --pool <POOL> --object <OID> --chunk-pool <POOL> --fingerprint-algorithm <FP> --dedup-cdc-chunk-size <CHUNK_SIZE> [--snap]",
     ": perform a object dedup---deduplicate the entire object, not a chunk. Related snapshots are also deduplicated if --snap is given")
    ("op sample-dedup --pool <POOL> --chunk-pool <POOL> --chunk-algorithm <ALGO> --fingerprint-algorithm <FP> --daemon --loop",
     ": perform a sample dedup---make crawling threads which crawl objects in base pool and deduplicate them based on their deduplication efficiency")
    ;
  po::options_description op_desc("Opational arguments");
  op_desc.add_options()
    ("op", po::value<std::string>(), ": estimate|chunk-scrub|chunk-get-ref|chunk-put-ref|chunk-repair|dump-chunk-refs|chunk-dedup|object-dedup")
    ("target-ref", po::value<std::string>(), ": set target object")
    ("target-ref-pool-id", po::value<uint64_t>(), ": set target pool id")
    ("object", po::value<std::string>(), ": set object name")
    ("chunk-size", po::value<int>(), ": chunk size (byte)")
    ("chunk-algorithm", po::value<std::string>(), ": <fixed|fastcdc>, set chunk-algorithm")
    ("fingerprint-algorithm", po::value<std::string>(), ": <sha1|sha256|sha512>, set fingerprint-algorithm")
    ("chunk-pool", po::value<std::string>(), ": set chunk pool name")
    ("max-thread", po::value<int>()->default_value(2), ": set max thread")
    ("report-period", po::value<int>()->default_value(10), ": set report-period")
    ("max-seconds", po::value<int>(), ": set max runtime")
    ("max-read-size", po::value<int>(), ": set max read size")
    ("pool", po::value<std::string>(), ": set pool name")
    ("min-chunk-size", po::value<int>(), ": min chunk size (byte)")
    ("max-chunk-size", po::value<int>(), ": max chunk size (byte)")
    ("source-off", po::value<uint64_t>(), ": set source offset")
    ("source-length", po::value<uint64_t>(), ": set source length")
    ("dedup-cdc-chunk-size", po::value<unsigned int>(), ": set dedup chunk size for cdc")
    ("snap", ": deduplciate snapshotted object")
    ("debug", ": enable debug")
    ("pgid", ": set pgid")
    ("chunk-dedup-threshold", po::value<size_t>(), ": set the threshold for chunk dedup (number of duplication) ")
    ("sampling-ratio", po::value<int>(), ": set the sampling ratio (percentile)")
    ("daemon", ": execute sample dedup in daemon mode")
    ("loop", ": execute sample dedup in a loop until terminated. Sleeps 'wakeup-period' seconds between iterations")
    ("wakeup-period", po::value<int>(), ": set the wakeup period of crawler thread (sec)")
    ("fpstore-threshold", po::value<size_t>()->default_value(100_M), ": set max size of in-memory fingerprint store (bytes)")
  ;
  desc.add(op_desc);
  return desc;
}

template <typename I, typename T>
static int rados_sistrtoll(I &i, T *val) {
  std::string err;
  *val = strict_iecstrtoll(i->second, &err);
  if (err != "") {
    cerr << "Invalid value for " << i->first << ": " << err << std::endl;
    return -EINVAL;
  } else {
    return 0;
  }
}

class EstimateDedupRatio;
class ChunkScrub;
class CrawlerThread : public Thread
{
  IoCtx io_ctx;
  int n;
  int m;
  ObjectCursor begin;
  ObjectCursor end;
  ceph::mutex m_lock = ceph::make_mutex("CrawlerThread::Locker");
  ceph::condition_variable m_cond;
  int32_t report_period;
  bool m_stop = false;
  uint64_t total_bytes = 0;
  uint64_t total_objects = 0;
  uint64_t examined_objects = 0;
  uint64_t examined_bytes = 0;
  uint64_t max_read_size = 0;
  bool debug = false;
#define COND_WAIT_INTERVAL 10

public:
  CrawlerThread(IoCtx& io_ctx, int n, int m,
		ObjectCursor begin, ObjectCursor end, int32_t report_period,
		uint64_t num_objects, uint64_t max_read_size = default_op_size):
    io_ctx(io_ctx), n(n), m(m), begin(begin), end(end), 
    report_period(report_period), total_objects(num_objects), max_read_size(max_read_size)
  {}

  void signal(int signum) {
    std::lock_guard l{m_lock};
    m_stop = true;
    m_cond.notify_all();
  }
  virtual void print_status(Formatter *f, ostream &out) {}
  uint64_t get_examined_objects() { return examined_objects; }
  uint64_t get_examined_bytes() { return examined_bytes; }
  uint64_t get_total_bytes() { return total_bytes; }
  uint64_t get_total_objects() { return total_objects; }
  void set_debug(const bool debug_) { debug = debug_; }
  friend class EstimateDedupRatio;
  friend class ChunkScrub;
};

class EstimateDedupRatio : public CrawlerThread
{
  string chunk_algo;
  string fp_algo;
  uint64_t chunk_size;
  uint64_t max_seconds;

public:
  EstimateDedupRatio(
    IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end,
    string chunk_algo, string fp_algo, uint64_t chunk_size, int32_t report_period,
    uint64_t num_objects, uint64_t max_read_size,
    uint64_t max_seconds):
    CrawlerThread(io_ctx, n, m, begin, end, report_period, num_objects,
		  max_read_size),
    chunk_algo(chunk_algo),
    fp_algo(fp_algo),
    chunk_size(chunk_size),
    max_seconds(max_seconds) {
  }

  void* entry() {
    estimate_dedup_ratio();
    return NULL;
  }
  void estimate_dedup_ratio();
};

class ChunkScrub: public CrawlerThread
{
  IoCtx chunk_io_ctx;
  int damaged_objects = 0;

public:
  ChunkScrub(IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end, 
	     IoCtx& chunk_io_ctx, int32_t report_period, uint64_t num_objects):
    CrawlerThread(io_ctx, n, m, begin, end, report_period, num_objects), chunk_io_ctx(chunk_io_ctx)
    { }
  void* entry() {
    chunk_scrub_common();
    return NULL;
  }
  void chunk_scrub_common();
  int get_damaged_objects() { return damaged_objects; }
  void print_status(Formatter *f, ostream &out);
};

vector<std::unique_ptr<CrawlerThread>> estimate_threads;

static void print_dedup_estimate(std::ostream& out, std::string chunk_algo)
{
  /*
  uint64_t total_bytes = 0;
  uint64_t total_objects = 0;
  */
  uint64_t examined_objects = 0;
  uint64_t examined_bytes = 0;

  for (auto &et : estimate_threads) {
    examined_objects += et->get_examined_objects();
    examined_bytes += et->get_examined_bytes();
  }

  auto f = Formatter::create("json-pretty");
  f->open_object_section("results");
  f->dump_string("chunk_algo", chunk_algo);
  f->open_array_section("chunk_sizes");
  for (auto& i : dedup_estimates) {
    f->dump_object("chunker", i.second);
  }
  f->close_section();

  f->open_object_section("summary");
  f->dump_unsigned("examined_objects", examined_objects);
  f->dump_unsigned("examined_bytes", examined_bytes);
  /*
  f->dump_unsigned("total_objects", total_objects);
  f->dump_unsigned("total_bytes", total_bytes);
  f->dump_float("examined_ratio", (float)examined_bytes / (float)total_bytes);
  */
  f->close_section();
  f->close_section();
  f->flush(out);
}

static void handle_signal(int signum) 
{
  std::lock_guard l{glock};
  for (auto &p : estimate_threads) {
    p->signal(signum);
  }
}

void EstimateDedupRatio::estimate_dedup_ratio()
{
  ObjectCursor shard_start;
  ObjectCursor shard_end;

  io_ctx.object_list_slice(
    begin,
    end,
    n,
    m,
    &shard_start,
    &shard_end);

  utime_t start = ceph_clock_now();
  utime_t end;
  if (max_seconds) {
    end = start;
    end += max_seconds;
  }

  utime_t next_report;
  if (report_period) {
    next_report = start;
    next_report += report_period;
  }

  ObjectCursor c(shard_start);
  while (c < shard_end)
  {
    std::vector<ObjectItem> result;
    int r = io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
    if (r < 0 ){
      cerr << "error object_list : " << cpp_strerror(r) << std::endl;
      return;
    }

    unsigned op_size = max_read_size;

    for (const auto & i : result) {
      const auto &oid = i.oid;

      utime_t now = ceph_clock_now();
      if (max_seconds && now > end) {
	m_stop = true;
      }
      if (m_stop) {
	return;
      }

      if (n == 0 && // first thread only
	  next_report != utime_t() && now > next_report) {
	cerr << (int)(now - start) << "s : read "
	     << dedup_estimates.begin()->second.total_bytes << " bytes so far..."
	     << std::endl;
	print_dedup_estimate(cerr, chunk_algo);
	next_report = now;
	next_report += report_period;
      }

      // read entire object
      bufferlist bl;
      uint64_t offset = 0;
      while (true) {
	bufferlist t;
	int ret = io_ctx.read(oid, t, op_size, offset);
	if (ret <= 0) {
	  break;
	}
	offset += ret;
	bl.claim_append(t);
      }
      examined_objects++;
      examined_bytes += bl.length();

      // do the chunking
      for (auto& i : dedup_estimates) {
	vector<pair<uint64_t, uint64_t>> chunks;
	i.second.cdc->calc_chunks(bl, &chunks);
	for (auto& p : chunks) {
	  bufferlist chunk;
	  chunk.substr_of(bl, p.first, p.second);
	  i.second.add_chunk(chunk, fp_algo);
	  if (debug) {
	    cout << " " << oid <<  " " << p.first << "~" << p.second << std::endl;
	  }
	}
	++i.second.total_objects;
      }
    }
  }
}

static void print_chunk_scrub();
void ChunkScrub::chunk_scrub_common()
{
  ObjectCursor shard_start;
  ObjectCursor shard_end;
  int ret;
  Rados rados;

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     return;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     return;
  }

  chunk_io_ctx.object_list_slice(
    begin,
    end,
    n,
    m,
    &shard_start,
    &shard_end);

  const utime_t start = ceph_clock_now();
  utime_t next_report;
  if (report_period) {
    next_report = start;
    next_report += report_period;
  }

  ObjectCursor c(shard_start);
  while(c < shard_end)
  {
    std::vector<ObjectItem> result;
    int r = chunk_io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
    if (r < 0 ){
      cerr << "error object_list : " << cpp_strerror(r) << std::endl;
      return;
    }

    for (const auto & i : result) {
      std::unique_lock l{m_lock};
      if (m_stop) {
	Formatter *formatter = Formatter::create("json-pretty");
	print_status(formatter, cout);
	delete formatter;
	return;
      }

      utime_t now = ceph_clock_now();
      if (n == 0 && // first thread only
	  next_report != utime_t() && now > next_report) {
	cerr << (int)(now - start) << "s, interim findings is : "
	     << std::endl;
	print_chunk_scrub();
	next_report = now;
	next_report += report_period;
      }

      auto oid = i.oid;
      if (debug) {
	cout << oid << std::endl;
      }
      chunk_refs_t refs;
      {
	bufferlist t;
	ret = chunk_io_ctx.getxattr(oid, CHUNK_REFCOUNT_ATTR, t);
	if (ret < 0) {
	  continue;
	}
	auto p = t.cbegin();
	decode(refs, p);
      }

      examined_objects++;
      if (refs.get_type() != chunk_refs_t::TYPE_BY_OBJECT) {
	// we can't do anything here
	continue;
      }

      // check all objects
      chunk_refs_by_object_t *byo =
	static_cast<chunk_refs_by_object_t*>(refs.r.get());
      set<hobject_t> real_refs;

      uint64_t pool_missing = 0;
      uint64_t object_missing = 0;
      uint64_t does_not_ref = 0;
      for (auto& pp : byo->by_object) {
	IoCtx target_io_ctx;
	ret = rados.ioctx_create2(pp.pool, target_io_ctx);
	if (ret < 0) {
	  cerr << oid << " ref " << pp
	       << ": referencing pool does not exist" << std::endl;
	  ++pool_missing;
	  continue;
	}

	ret = cls_cas_references_chunk(target_io_ctx, pp.oid.name, oid);
	if (ret == -ENOENT) {
	  cerr << oid << " ref " << pp
	       << ": referencing object missing" << std::endl;
	  ++object_missing;
	} else if (ret == -ENOLINK) {
	  cerr << oid << " ref " << pp
	       << ": referencing object does not reference chunk"
	       << std::endl;
	  ++does_not_ref;
	}
      }
      if (pool_missing || object_missing || does_not_ref) {
	++damaged_objects;
      }
    }
  }
  cout << "--done--" << std::endl;
}

using AioCompRef = unique_ptr<AioCompletion>;

class SampleDedupWorkerThread : public Thread
{
public:
  struct chunk_t {
    string oid = "";
    size_t start = 0;
    size_t size = 0;
    string fingerprint = "";
    bufferlist data;
  };

  using dup_count_t = size_t;

  template <typename K, typename V>
  class FpMap {
    using map_t = std::unordered_map<K, V>;
  public:
    /// Represents a nullable reference into logical container
    class entry_t {
      /// Entry may be into one of two maps or NONE, indicates which
      enum entry_into_t {
	UNDER, OVER, NONE
      } entry_into = NONE;

      /// Valid iterator into map for UNDER|OVER, default for NONE
      typename map_t::iterator iter;

      entry_t(entry_into_t entry_into, typename map_t::iterator iter) :
	entry_into(entry_into), iter(iter) {
	ceph_assert(entry_into != NONE);
      }

    public:
      entry_t() = default;

      auto &operator*() {
	ceph_assert(entry_into != NONE);
	return *iter;
      }
      auto operator->() {
	ceph_assert(entry_into != NONE);
	return iter.operator->();
      }
      bool is_valid() const {
	return entry_into != NONE;
      }
      bool is_above_threshold() const {
	return entry_into == entry_t::OVER;
      }
      friend class FpMap;
    };

    /// inserts str, count into container, must not already be present
    entry_t insert(const K &str, V count) {
      std::pair<typename map_t::iterator, bool> r;
      typename entry_t::entry_into_t s;
      if (count < dedup_threshold) {
       r = under_threshold_fp_map.insert({str, count});
       s = entry_t::UNDER;
      } else {
       r = over_threshold_fp_map.insert({str, count});
       s = entry_t::OVER;
      }
      ceph_assert(r.second);
      return entry_t{s, r.first};
    }

    /// increments refcount for entry, promotes as necessary, entry must be valid
    entry_t increment_reference(entry_t entry) {
      ceph_assert(entry.is_valid());
      entry.iter->second++;
      if (entry.entry_into == entry_t::OVER ||
	  entry.iter->second < dedup_threshold) {
	return entry;
      } else {
	auto [over_iter, inserted] = over_threshold_fp_map.insert(
	  *entry);
	ceph_assert(inserted);
	under_threshold_fp_map.erase(entry.iter);
	return entry_t{entry_t::OVER, over_iter};
      }
    }

    /// returns entry for fp, return will be !is_valid() if not present
    auto find(const K &fp) {
      if (auto iter = under_threshold_fp_map.find(fp);
	  iter != under_threshold_fp_map.end()) {
	return entry_t{entry_t::UNDER, iter};
      } else if (auto iter = over_threshold_fp_map.find(fp);
		 iter != over_threshold_fp_map.end()) {
	return entry_t{entry_t::OVER, iter};
      }  else {
	return entry_t{};
      }
    }

    /// true if container contains fp
    bool contains(const K &fp) {
      return find(fp).is_valid();
    }

    /// returns number of items
    size_t get_num_items() const {
      return under_threshold_fp_map.size() + over_threshold_fp_map.size();
    }

    /// returns estimate of total in-memory size (bytes)
    size_t estimate_total_size() const {
      size_t total = 0;
      if (!under_threshold_fp_map.empty()) {
	total += under_threshold_fp_map.size() *
	  (under_threshold_fp_map.begin()->first.size() + sizeof(V));
      }
      if (!over_threshold_fp_map.empty()) {
	total += over_threshold_fp_map.size() *
	  (over_threshold_fp_map.begin()->first.size() + sizeof(V));
      }
      return total;
    }

    /// true if empty
    bool empty() const {
      return under_threshold_fp_map.empty() && over_threshold_fp_map.empty();
    }

    /// instructs container to drop entries with refcounts below threshold
    void drop_entries_below_threshold() {
      under_threshold_fp_map.clear();
    }

    FpMap(size_t dedup_threshold) : dedup_threshold(dedup_threshold) {}
    FpMap() = delete;
  private:
    map_t under_threshold_fp_map;
    map_t over_threshold_fp_map;
    const size_t dedup_threshold;
  };

  class FpStore {
  public:
    void maybe_print_status() {
      utime_t now = ceph_clock_now();
      if (next_report != utime_t() && now > next_report) {
	cerr << (int)(now - start) << "s : read "
	     << total_bytes << " bytes so far..."
	     << std::endl;
	next_report = now;
	next_report += report_period;
      }
    }

    bool contains(string& fp) {
      std::shared_lock lock(fingerprint_lock);
      return fp_map.contains(fp);
    }

    // return true if the chunk is duplicate
    bool add(chunk_t& chunk) {
      std::unique_lock lock(fingerprint_lock);
      auto entry = fp_map.find(chunk.fingerprint);
      total_bytes += chunk.size;
      if (!entry.is_valid()) {
	if (is_fpmap_full()) {
	  fp_map.drop_entries_below_threshold();
	  if (is_fpmap_full()) {
	    return false;
	  }
	}
	entry = fp_map.insert(chunk.fingerprint, 1);
      } else {
	entry = fp_map.increment_reference(entry);
      }
      return entry.is_above_threshold();
    }

    bool is_fpmap_full() const {
      return fp_map.estimate_total_size() >= memory_threshold;
    }

    FpStore(size_t chunk_threshold,
      uint32_t report_period,	
      size_t memory_threshold) :
      report_period(report_period),
      memory_threshold(memory_threshold),
      fp_map(chunk_threshold) { }
    FpStore() = delete;

  private:
    std::shared_mutex fingerprint_lock;
    const utime_t start = ceph_clock_now();
    utime_t next_report;
    const uint32_t report_period;
    size_t total_bytes = 0;
    const size_t memory_threshold;
    FpMap<std::string, dup_count_t> fp_map;
  };

  struct SampleDedupGlobal {
    FpStore fp_store;
    const double sampling_ratio = -1;
    SampleDedupGlobal(
      size_t chunk_threshold,
      int sampling_ratio,
      uint32_t report_period,
      size_t fpstore_threshold) :
      fp_store(chunk_threshold, report_period, fpstore_threshold),
      sampling_ratio(static_cast<double>(sampling_ratio) / 100) { }
  };

  SampleDedupWorkerThread(
    IoCtx &io_ctx,
    IoCtx &chunk_io_ctx,
    ObjectCursor begin,
    ObjectCursor end,
    size_t chunk_size,
    std::string &fp_algo,
    std::string &chunk_algo,
    SampleDedupGlobal &sample_dedup_global,
    bool snap) :
    chunk_io_ctx(chunk_io_ctx),
    chunk_size(chunk_size),
    fp_type(pg_pool_t::get_fingerprint_from_str(fp_algo)),
    chunk_algo(chunk_algo),
    sample_dedup_global(sample_dedup_global),
    begin(begin),
    end(end),
    snap(snap) {
      this->io_ctx.dup(io_ctx);
    }

  ~SampleDedupWorkerThread() { };

  size_t get_total_duplicated_size() const {
    return total_duplicated_size;
  }

  size_t get_total_object_size() const {
    return total_object_size;
  }

protected:
  void* entry() override {
    crawl();
    return nullptr;
  }

private:
  void crawl();
  std::tuple<std::vector<ObjectItem>, ObjectCursor> get_objects(
    ObjectCursor current,
    ObjectCursor end,
    size_t max_object_count);
  std::vector<size_t> sample_object(size_t count);
  void try_dedup_and_accumulate_result(ObjectItem &object, snap_t snap = 0);
  bool ok_to_dedup_all();
  int do_chunk_dedup(chunk_t &chunk, snap_t snap);
  bufferlist read_object(ObjectItem &object);
  std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> do_cdc(
    ObjectItem &object,
    bufferlist &data);
  std::string generate_fingerprint(bufferlist chunk_data);
  AioCompRef do_async_evict(string oid);

  IoCtx io_ctx;
  IoCtx chunk_io_ctx;
  size_t total_duplicated_size = 0;
  size_t total_object_size = 0;

  std::set<std::pair<std::string, snap_t>> oid_for_evict;
  const size_t chunk_size = 0;
  pg_pool_t::fingerprint_t fp_type = pg_pool_t::TYPE_FINGERPRINT_NONE;
  std::string chunk_algo;
  SampleDedupGlobal &sample_dedup_global;
  ObjectCursor begin;
  ObjectCursor end;
  bool snap;
};

void SampleDedupWorkerThread::crawl()
{
  cout << "new iteration" << std::endl;

  ObjectCursor current_object = begin;
  while (current_object < end) {
    std::vector<ObjectItem> objects;
    // Get the list of object IDs to deduplicate
    std::tie(objects, current_object) = get_objects(current_object, end, 100);

    // Pick few objects to be processed. Sampling ratio decides how many
    // objects to pick. Lower sampling ratio makes crawler have lower crawling
    // overhead but find less duplication.
    auto sampled_indexes = sample_object(objects.size());
    for (size_t index : sampled_indexes) {
      ObjectItem target = objects[index];
      if (snap) {
	io_ctx.snap_set_read(librados::SNAP_DIR);
	snap_set_t snap_set;
	int snap_ret;
	ObjectReadOperation op;
	op.list_snaps(&snap_set, &snap_ret);
	io_ctx.operate(target.oid, &op, NULL);

	for (vector<librados::clone_info_t>::const_iterator r = snap_set.clones.begin();
	  r != snap_set.clones.end();
	  ++r) {
	  io_ctx.snap_set_read(r->cloneid);
	  try_dedup_and_accumulate_result(target, r->cloneid);
	}
      } else {
	try_dedup_and_accumulate_result(target);
      }
    }
  }

  vector<AioCompRef> evict_completions(oid_for_evict.size());
  int i = 0;
  for (auto &oid : oid_for_evict) {
    if (snap) {
      io_ctx.snap_set_read(oid.second);
    }
    evict_completions[i] = do_async_evict(oid.first);
    i++;
  }
  for (auto &completion : evict_completions) {
    completion->wait_for_complete();
  }
  cout << "done iteration" << std::endl;
}

AioCompRef SampleDedupWorkerThread::do_async_evict(string oid)
{
  Rados rados;
  ObjectReadOperation op_tier;
  AioCompRef completion(rados.aio_create_completion());
  op_tier.tier_evict();
  io_ctx.aio_operate(
      oid,
      completion.get(),
      &op_tier,
      NULL);
  return completion;
}

std::tuple<std::vector<ObjectItem>, ObjectCursor> SampleDedupWorkerThread::get_objects(
  ObjectCursor current, ObjectCursor end, size_t max_object_count)
{
  std::vector<ObjectItem> objects;
  ObjectCursor next;
  int ret = io_ctx.object_list(
    current,
    end,
    max_object_count,
    {},
    &objects,
    &next);
  if (ret < 0 ) {
    cerr << "error object_list" << std::endl;
    objects.clear();
  }

  return std::make_tuple(objects, next);
}

std::vector<size_t> SampleDedupWorkerThread::sample_object(size_t count)
{
  std::vector<size_t> indexes(count);
  for (size_t i = 0 ; i < count ; i++) {
    indexes[i] = i;
  }
  default_random_engine generator;
  shuffle(indexes.begin(), indexes.end(), generator);
  size_t sampling_count = static_cast<double>(count) *
    sample_dedup_global.sampling_ratio;
  indexes.resize(sampling_count);

  return indexes;
}

void SampleDedupWorkerThread::try_dedup_and_accumulate_result(
  ObjectItem &object, snap_t snap)
{
  bufferlist data = read_object(object);
  if (data.length() == 0) {
    cerr << __func__ << " skip object " << object.oid
	 << " read returned size 0" << std::endl;
    return;
  }
  auto chunks = do_cdc(object, data);
  size_t chunk_total_amount = 0;

  // First, check total size of created chunks
  for (auto &chunk : chunks) {
    auto &chunk_data = std::get<0>(chunk);
    chunk_total_amount += chunk_data.length();
  }
  if (chunk_total_amount != data.length()) {
    cerr << __func__ << " sum of chunked length(" << chunk_total_amount
	 << ") is different from object data length(" << data.length() << ")"
	 << std::endl;
    return;
  }

  size_t duplicated_size = 0;
  list<chunk_t> redundant_chunks;
  for (auto &chunk : chunks) {
    auto &chunk_data = std::get<0>(chunk);
    std::string fingerprint = generate_fingerprint(chunk_data);
    std::pair<uint64_t, uint64_t> chunk_boundary = std::get<1>(chunk);
    chunk_t chunk_info = {
      .oid = object.oid,
      .start = chunk_boundary.first,
      .size = chunk_boundary.second,
      .fingerprint = fingerprint,
      .data = chunk_data
      };

    if (sample_dedup_global.fp_store.contains(fingerprint)) {
      duplicated_size += chunk_data.length();
    }
    if (sample_dedup_global.fp_store.add(chunk_info)) {
      redundant_chunks.push_back(chunk_info);
    }
  }

  size_t object_size = data.length();

  // perform chunk-dedup
  for (auto &p : redundant_chunks) {
    do_chunk_dedup(p, snap);
  }
  total_duplicated_size += duplicated_size;
  total_object_size += object_size;
}

bufferlist SampleDedupWorkerThread::read_object(ObjectItem &object)
{
  bufferlist whole_data;
  size_t offset = 0;
  int ret = -1;
  while (ret != 0) {
    bufferlist partial_data;
    ret = io_ctx.read(object.oid, partial_data, default_op_size, offset);
    if (ret < 0) {
      cerr << "read object error " << object.oid << " offset " << offset
        << " size " << default_op_size << " error(" << cpp_strerror(ret)
        << std::endl;
      bufferlist empty_buf;
      return empty_buf;
    }
    offset += ret;
    whole_data.claim_append(partial_data);
  }
  return whole_data;
}

std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> SampleDedupWorkerThread::do_cdc(
  ObjectItem &object,
  bufferlist &data)
{
  std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> ret;

  unique_ptr<CDC> cdc = CDC::create(chunk_algo, cbits(chunk_size) - 1);
  vector<pair<uint64_t, uint64_t>> chunks;
  cdc->calc_chunks(data, &chunks);
  for (auto &p : chunks) {
    bufferlist chunk;
    chunk.substr_of(data, p.first, p.second);
    ret.push_back(make_tuple(chunk, p));
  }

  return ret;
}

std::string SampleDedupWorkerThread::generate_fingerprint(bufferlist chunk_data)
{
  string ret;

  switch (fp_type) {
    case pg_pool_t::TYPE_FINGERPRINT_SHA1:
      ret = crypto::digest<crypto::SHA1>(chunk_data).to_str();
      break;

    case pg_pool_t::TYPE_FINGERPRINT_SHA256:
      ret = crypto::digest<crypto::SHA256>(chunk_data).to_str();
      break;

    case pg_pool_t::TYPE_FINGERPRINT_SHA512:
      ret = crypto::digest<crypto::SHA512>(chunk_data).to_str();
      break;
    default:
      ceph_assert(0 == "Invalid fp type");
      break;
  }
  return ret;
}

int SampleDedupWorkerThread::do_chunk_dedup(chunk_t &chunk, snap_t snap)
{
  uint64_t size;
  time_t mtime;

  int ret = chunk_io_ctx.stat(chunk.fingerprint, &size, &mtime);

  if (ret == -ENOENT) {
    bufferlist bl;
    bl.append(chunk.data);
    ObjectWriteOperation wop;
    wop.write_full(bl);
    chunk_io_ctx.operate(chunk.fingerprint, &wop);
  } else {
    ceph_assert(ret == 0);
  }

  ObjectReadOperation op;
  op.set_chunk(
      chunk.start,
      chunk.size,
      chunk_io_ctx,
      chunk.fingerprint,
      0,
      CEPH_OSD_OP_FLAG_WITH_REFERENCE);
  ret = io_ctx.operate(chunk.oid, &op, nullptr);
  oid_for_evict.insert(make_pair(chunk.oid, snap));
  return ret;
}

void ChunkScrub::print_status(Formatter *f, ostream &out)
{
  if (f) {
    f->open_array_section("chunk_scrub");
    f->dump_string("PID", stringify(get_pid()));
    f->open_object_section("Status");
    f->dump_string("Total object", stringify(total_objects));
    f->dump_string("Examined objects", stringify(examined_objects));
    f->dump_string("damaged objects", stringify(damaged_objects));
    f->close_section();
    f->flush(out);
    cout << std::endl;
  }
}

string get_opts_pool_name(const po::variables_map &opts) {
  if (opts.count("pool")) {
    return opts["pool"].as<string>();
  }
  cerr << "must specify pool name" << std::endl;
  exit(1);
}

string get_opts_chunk_algo(const po::variables_map &opts) {
  if (opts.count("chunk-algorithm")) {
    string chunk_algo = opts["chunk-algorithm"].as<string>();
    if (!CDC::create(chunk_algo, 12)) {
      cerr << "unrecognized chunk-algorithm " << chunk_algo << std::endl;
      exit(1);
    }
    return chunk_algo;
  }
  cerr << "must specify chunk-algorithm" << std::endl;
  exit(1);
}

string get_opts_fp_algo(const po::variables_map &opts) {
  if (opts.count("fingerprint-algorithm")) {
    string fp_algo = opts["fingerprint-algorithm"].as<string>();
    if (fp_algo != "sha1"
	&& fp_algo != "sha256" && fp_algo != "sha512") {
      cerr << "unrecognized fingerprint-algorithm " << fp_algo << std::endl;
      exit(1);
    }
    return fp_algo;
  }
  cout << "SHA1 is set as fingerprint algorithm by default" << std::endl;
  return string("sha1");
}

string get_opts_op_name(const po::variables_map &opts) {
  if (opts.count("op")) {
    return opts["op"].as<string>();
  } else {
    cerr << "must specify op" << std::endl;
    exit(1);
  }
}

string get_opts_chunk_pool(const po::variables_map &opts) {
  if (opts.count("chunk-pool")) {
    return opts["chunk-pool"].as<string>();
  } else {
    cerr << "must specify --chunk-pool" << std::endl;
    exit(1);
  }
}

string get_opts_object_name(const po::variables_map &opts) {
  if (opts.count("object")) {
    return opts["object"].as<string>();
  } else {
    cerr << "must specify object" << std::endl;
    exit(1);
  }
}

int get_opts_max_thread(const po::variables_map &opts) {
  if (opts.count("max-thread")) {
    return opts["max-thread"].as<int>();
  } else {
    cout << "2 is set as the number of threads by default" << std::endl;
    return 2;
  }
}

int get_opts_report_period(const po::variables_map &opts) {
  if (opts.count("report-period")) {
    return opts["report-period"].as<int>();
  } else {
    cout << "10 seconds is set as report period by default" << std::endl;
    return 10;
  }
}

int estimate_dedup_ratio(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx;
  std::string chunk_algo = "fastcdc";
  string fp_algo = "sha1";
  string pool_name;
  uint64_t chunk_size = 8192;
  uint64_t min_chunk_size = 8192;
  uint64_t max_chunk_size = 4*1024*1024;
  unsigned max_thread = get_opts_max_thread(opts);
  uint32_t report_period = get_opts_report_period(opts);
  uint64_t max_read_size = default_op_size;
  uint64_t max_seconds = 0;
  int ret;
  std::map<std::string, std::string>::const_iterator i;
  bool debug = false;
  ObjectCursor begin;
  ObjectCursor end;
  librados::pool_stat_t s; 
  list<string> pool_names;
  map<string, librados::pool_stat_t> stats;

  pool_name = get_opts_pool_name(opts);
  if (opts.count("chunk-algorithm")) {
    chunk_algo = opts["chunk-algorithm"].as<string>();
    if (!CDC::create(chunk_algo, 12)) {
      cerr << "unrecognized chunk-algorithm " << chunk_algo << std::endl;
      exit(1);
    }
  } else {
    cerr << "must specify chunk-algorithm" << std::endl;
    exit(1);
  }
  fp_algo = get_opts_fp_algo(opts);
  if (opts.count("chunk-size")) {
    chunk_size = opts["chunk-size"].as<int>();
  } else {
    cout << "8192 is set as chunk size by default" << std::endl;
  }
  if (opts.count("min-chunk-size")) {
    chunk_size = opts["min-chunk-size"].as<int>();
  } else {
    cout << "8192 is set as min chunk size by default" << std::endl;
  }
  if (opts.count("max-chunk-size")) {
    chunk_size = opts["max-chunk-size"].as<int>();
  } else {
    cout << "4MB is set as max chunk size by default" << std::endl;
  }
  if (opts.count("max-seconds")) {
    max_seconds = opts["max-seconds"].as<int>();
  } else {
    cout << "max seconds is not set" << std::endl;
  }
  if (opts.count("max-read-size")) {
    max_read_size = opts["max-read-size"].as<int>();
  } else {
    cout << default_op_size << " is set as max-read-size by default" << std::endl;
  }
  if (opts.count("debug")) {
    debug = true;
  }
  boost::optional<pg_t> pgid(opts.count("pgid"), pg_t());

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  if (pool_name.empty()) {
    cerr << "--create-pool requested but pool_name was not specified!" << std::endl;
    exit(1);
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  // set up chunkers
  if (chunk_size) {
    dedup_estimates.emplace(std::piecewise_construct,
			    std::forward_as_tuple(chunk_size),
			    std::forward_as_tuple(chunk_algo, cbits(chunk_size)-1));
  } else {
    for (size_t cs = min_chunk_size; cs <= max_chunk_size; cs *= 2) {
      dedup_estimates.emplace(std::piecewise_construct,
			      std::forward_as_tuple(cs),
			      std::forward_as_tuple(chunk_algo, cbits(cs)-1));
    }
  }

  glock.lock();
  begin = io_ctx.object_list_begin();
  end = io_ctx.object_list_end();
  pool_names.push_back(pool_name);
  ret = rados.get_pool_stats(pool_names, stats);
  if (ret < 0) {
    cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
    glock.unlock();
    return ret;
  }
  if (stats.find(pool_name) == stats.end()) {
    cerr << "stats can not find pool name: " << pool_name << std::endl;
    glock.unlock();
    return ret;
  }
  s = stats[pool_name];

  for (unsigned i = 0; i < max_thread; i++) {
    std::unique_ptr<CrawlerThread> ptr (
      new EstimateDedupRatio(io_ctx, i, max_thread, begin, end,
			     chunk_algo, fp_algo, chunk_size,
			     report_period, s.num_objects, max_read_size,
			     max_seconds));
    ptr->create("estimate_thread");
    ptr->set_debug(debug);
    estimate_threads.push_back(std::move(ptr));
  }
  glock.unlock();

  for (auto &p : estimate_threads) {
    p->join();
  }

  print_dedup_estimate(cout, chunk_algo);

 out:
  return (ret < 0) ? 1 : 0;
}

static void print_chunk_scrub()
{
  uint64_t total_objects = 0;
  uint64_t examined_objects = 0;
  int damaged_objects = 0;

  for (auto &et : estimate_threads) {
    if (!total_objects) {
      total_objects = et->get_total_objects();
    }
    examined_objects += et->get_examined_objects();
    ChunkScrub *ptr = static_cast<ChunkScrub*>(et.get());
    damaged_objects += ptr->get_damaged_objects();
  }

  cout << " Total object : " << total_objects << std::endl;
  cout << " Examined object : " << examined_objects << std::endl;
  cout << " Damaged object : " << damaged_objects << std::endl;
}

int chunk_scrub_common(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx, chunk_io_ctx;
  std::string object_name, target_object_name;
  string chunk_pool_name, op_name;
  int ret;
  unsigned max_thread = get_opts_max_thread(opts);
  std::map<std::string, std::string>::const_iterator i;
  uint32_t report_period = get_opts_report_period(opts);
  ObjectCursor begin;
  ObjectCursor end;
  librados::pool_stat_t s; 
  list<string> pool_names;
  map<string, librados::pool_stat_t> stats;

  op_name = get_opts_op_name(opts);
  chunk_pool_name = get_opts_chunk_pool(opts);
  boost::optional<pg_t> pgid(opts.count("pgid"), pg_t());

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << chunk_pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  if (op_name == "chunk-get-ref" ||
      op_name == "chunk-put-ref" ||
      op_name == "chunk-repair") {
    string target_object_name;
    uint64_t pool_id;
    object_name = get_opts_object_name(opts);
    if (opts.count("target-ref")) {
      target_object_name = opts["target-ref"].as<string>();
    } else {
      cerr << "must specify target ref" << std::endl;
      exit(1);
    }
    if (opts.count("target-ref-pool-id")) {
      pool_id = opts["target-ref-pool-id"].as<uint64_t>();
    } else {
      cerr << "must specify target-ref-pool-id" << std::endl;
      exit(1);
    }

    uint32_t hash;
    ret = chunk_io_ctx.get_object_hash_position2(object_name, &hash);
    if (ret < 0) {
      return ret;
    }
    hobject_t oid(sobject_t(target_object_name, CEPH_NOSNAP), "", hash, pool_id, "");

    auto run_op = [] (ObjectWriteOperation& op, hobject_t& oid,
      string& object_name, IoCtx& chunk_io_ctx) -> int {
      int ret = chunk_io_ctx.operate(object_name, &op);
      if (ret < 0) {
	cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      }
      return ret;
    };

    ObjectWriteOperation op;
    if (op_name == "chunk-get-ref") {
      cls_cas_chunk_get_ref(op, oid);
      ret = run_op(op, oid, object_name, chunk_io_ctx);
    } else if (op_name == "chunk-put-ref") {
      cls_cas_chunk_put_ref(op, oid);
      ret = run_op(op, oid, object_name, chunk_io_ctx);
    } else if (op_name == "chunk-repair") {
      ret = rados.ioctx_create2(pool_id, io_ctx);
      if (ret < 0) {
	cerr << oid << " ref " << pool_id
	     << ": referencing pool does not exist" << std::endl;
	return ret;
      }
      int chunk_ref = -1, base_ref = -1;
      // read object on chunk pool to know how many reference the object has
      bufferlist t;
      ret = chunk_io_ctx.getxattr(object_name, CHUNK_REFCOUNT_ATTR, t);
      if (ret < 0) {
	return ret;
      }
      chunk_refs_t refs;
      auto p = t.cbegin();
      decode(refs, p);
      if (refs.get_type() != chunk_refs_t::TYPE_BY_OBJECT) {
	cerr << " does not supported chunk type " << std::endl;
	return -1;
      }
      chunk_ref =
	static_cast<chunk_refs_by_object_t*>(refs.r.get())->by_object.count(oid);
      if (chunk_ref < 0) {
	cerr << object_name << " has no reference of " << target_object_name
	     << std::endl;
	return chunk_ref;
      }
      cout << object_name << " has " << chunk_ref << " references for "
	   << target_object_name << std::endl;

      // read object on base pool to know the number of chunk object's references
      base_ref = cls_cas_references_chunk(io_ctx, target_object_name, object_name);
      if (base_ref < 0) {
	if (base_ref == -ENOENT || base_ref == -ENOLINK) {
	  base_ref = 0;
	} else {
	  return base_ref;
	}
      }
      cout << target_object_name << " has " << base_ref << " references for "
	   << object_name << std::endl;
      if (chunk_ref != base_ref) {
	if (base_ref > chunk_ref) {
	  cerr << "error : " << target_object_name << "'s ref. < " << object_name
	       << "' ref. " << std::endl;
	  return -EINVAL;
	}
	cout << " fix dangling reference from " << chunk_ref << " to " << base_ref
	     << std::endl;
	while (base_ref != chunk_ref) {
	  ObjectWriteOperation op;
	  cls_cas_chunk_put_ref(op, oid);
	  chunk_ref--;
	  ret = run_op(op, oid, object_name, chunk_io_ctx);
	  if (ret < 0) {
	    return ret;
	  }
	}
      }
    }
    return ret;

  } else if (op_name == "dump-chunk-refs") {
    object_name = get_opts_object_name(opts);
    bufferlist t;
    ret = chunk_io_ctx.getxattr(object_name, CHUNK_REFCOUNT_ATTR, t);
    if (ret < 0) {
      return ret;
    }
    chunk_refs_t refs;
    auto p = t.cbegin();
    decode(refs, p);
    auto f = Formatter::create("json-pretty");
    f->dump_object("refs", refs);
    f->flush(cout);
    return 0;
  }

  glock.lock();
  begin = chunk_io_ctx.object_list_begin();
  end = chunk_io_ctx.object_list_end();
  pool_names.push_back(chunk_pool_name);
  ret = rados.get_pool_stats(pool_names, stats);
  if (ret < 0) {
    cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
    glock.unlock();
    return ret;
  }
  if (stats.find(chunk_pool_name) == stats.end()) {
    cerr << "stats can not find pool name: " << chunk_pool_name << std::endl;
    glock.unlock();
    return ret;
  }
  s = stats[chunk_pool_name];

  for (unsigned i = 0; i < max_thread; i++) {
    std::unique_ptr<CrawlerThread> ptr (
      new ChunkScrub(io_ctx, i, max_thread, begin, end, chunk_io_ctx,
		     report_period, s.num_objects));
    ptr->create("estimate_thread");
    estimate_threads.push_back(std::move(ptr));
  }
  glock.unlock();

  for (auto &p : estimate_threads) {
    cout << "join " << std::endl;
    p->join();
    cout << "joined " << std::endl;
  }

  print_chunk_scrub();

out:
  return (ret < 0) ? 1 : 0;
}

string make_pool_str(string pool, string var, string val)
{
  return string("{\"prefix\": \"osd pool set\",\"pool\":\"") + pool
    + string("\",\"var\": \"") + var + string("\",\"val\": \"")
    + val + string("\"}");
}

string make_pool_str(string pool, string var, int val)
{
  return make_pool_str(pool, var, stringify(val));
}

int make_dedup_object(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx, chunk_io_ctx;
  std::string object_name, chunk_pool_name, op_name, pool_name, fp_algo;
  int ret;
  std::map<std::string, std::string>::const_iterator i;

  op_name = get_opts_op_name(opts);
  pool_name = get_opts_pool_name(opts);
  object_name = get_opts_object_name(opts);
  chunk_pool_name = get_opts_chunk_pool(opts);
  boost::optional<pg_t> pgid(opts.count("pgid"), pg_t());

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << chunk_pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }
  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << chunk_pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }
  fp_algo = get_opts_fp_algo(opts);

  if (op_name == "chunk-dedup") {
    uint64_t offset, length;
    string chunk_object;
    if (opts.count("source-off")) {
      offset = opts["source-off"].as<uint64_t>();
    } else {
      cerr << "must specify --source-off" << std::endl;
      exit(1);
    }
    if (opts.count("source-length")) {
      length = opts["source-length"].as<uint64_t>();
    } else {
      cerr << "must specify --source-length" << std::endl;
      exit(1);
    }
    // 1. make a copy from manifest object to chunk object
    bufferlist bl;
    ret = io_ctx.read(object_name, bl, length, offset);
    if (ret < 0) {
      cerr << " reading object in base pool fails : " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    chunk_object = [&fp_algo, &bl]() -> string {
      if (fp_algo == "sha1") {
        return ceph::crypto::digest<ceph::crypto::SHA1>(bl).to_str();
      } else if (fp_algo == "sha256") {
        return ceph::crypto::digest<ceph::crypto::SHA256>(bl).to_str();
      } else if (fp_algo == "sha512") {
        return ceph::crypto::digest<ceph::crypto::SHA512>(bl).to_str();
      } else {
        assert(0 == "unrecognized fingerprint type");
        return {};
      }
    }();
    ret = chunk_io_ctx.write(chunk_object, bl, length, offset);
    if (ret < 0) {
      cerr << " writing object in chunk pool fails : " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    // 2. call set_chunk
    ObjectReadOperation op;
    op.set_chunk(offset, length, chunk_io_ctx, chunk_object, 0,
	CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    ret = io_ctx.operate(object_name, &op, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      goto out;
    }
  } else if (op_name == "object-dedup") {
    unsigned chunk_size = 0;
    bool snap = false;
    if (opts.count("dedup-cdc-chunk-size")) {
      chunk_size = opts["dedup-cdc-chunk-size"].as<unsigned int>();
    } else {
      cerr << "must specify --dedup-cdc-chunk-size" << std::endl;
      exit(1);
    }
    if (opts.count("snap")) {
      snap = true;
    }

    bufferlist inbl;
    ret = rados.mon_command(
	make_pool_str(pool_name, "fingerprint_algorithm", fp_algo),
	inbl, NULL, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    ret = rados.mon_command(
	make_pool_str(pool_name, "dedup_tier", chunk_pool_name),
	inbl, NULL, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    ret = rados.mon_command(
	make_pool_str(pool_name, "dedup_chunk_algorithm", "fastcdc"),
	inbl, NULL, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    ret = rados.mon_command(
	make_pool_str(pool_name, "dedup_cdc_chunk_size", chunk_size),
	inbl, NULL, NULL);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }

    auto create_new_deduped_object =
      [&io_ctx](string object_name) -> int {

      // tier-flush to perform deduplication
      ObjectReadOperation flush_op;
      flush_op.tier_flush();
      int ret = io_ctx.operate(object_name, &flush_op, NULL);
      if (ret < 0) {
	cerr << " tier_flush fail : " << cpp_strerror(ret) << std::endl;
	return ret;
      }
      // tier-evict
      ObjectReadOperation evict_op;
      evict_op.tier_evict();
      ret = io_ctx.operate(object_name, &evict_op, NULL);
      if (ret < 0) {
	cerr << " tier_evict fail : " << cpp_strerror(ret) << std::endl;
	return ret;
      }
      return ret;
    };

    if (snap) {
      io_ctx.snap_set_read(librados::SNAP_DIR);
      snap_set_t snap_set;
      int snap_ret;
      ObjectReadOperation op;
      op.list_snaps(&snap_set, &snap_ret);
      io_ctx.operate(object_name, &op, NULL);

      for (vector<librados::clone_info_t>::const_iterator r = snap_set.clones.begin();
	r != snap_set.clones.end();
	++r) {
	io_ctx.snap_set_read(r->cloneid);
	ret = create_new_deduped_object(object_name);
	if (ret < 0) {
	  goto out;
	}
      }
    } else {
      ret = create_new_deduped_object(object_name);
    }
  }

out:
  return (ret < 0) ? 1 : 0;
}

int make_crawling_daemon(const po::variables_map &opts)
{
  string base_pool_name = get_opts_pool_name(opts);
  string chunk_pool_name = get_opts_chunk_pool(opts);
  unsigned max_thread = get_opts_max_thread(opts);
  uint32_t report_period = get_opts_report_period(opts);

  bool loop = false;
  if (opts.count("loop")) {
    loop = true;
  }

  int sampling_ratio = -1;
  if (opts.count("sampling-ratio")) {
    sampling_ratio = opts["sampling-ratio"].as<int>();
  }
  size_t chunk_size = 8192;
  if (opts.count("chunk-size")) {
    chunk_size = opts["chunk-size"].as<int>();
  } else {
    cout << "8192 is set as chunk size by default" << std::endl;
  }
  bool snap = false;
  if (opts.count("snap")) {
    snap = true;
  }

  uint32_t chunk_dedup_threshold = -1;
  if (opts.count("chunk-dedup-threshold")) {
    chunk_dedup_threshold = opts["chunk-dedup-threshold"].as<size_t>();
  }

  std::string chunk_algo = get_opts_chunk_algo(opts);

  Rados rados;
  int ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
    cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  ret = rados.connect();
  if (ret) {
    cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  int wakeup_period = 100;
  if (opts.count("wakeup-period")) {
    wakeup_period = opts["wakeup-period"].as<int>();
  } else {
    cout << "100 second is set as wakeup period by default" << std::endl;
  }

  const size_t fp_threshold = opts["fpstore-threshold"].as<size_t>();

  std::string fp_algo = get_opts_fp_algo(opts);

  list<string> pool_names;
  IoCtx io_ctx, chunk_io_ctx;
  pool_names.push_back(base_pool_name);
  ret = rados.ioctx_create(base_pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening base pool "
      << base_pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }

  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_io_ctx);
  if (ret < 0) {
    cerr << "error opening chunk pool "
      << chunk_pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  bufferlist inbl;
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "fingerprint_algorithm", fp_algo),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "dedup_chunk_algorithm", "fastcdc"),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "dedup_cdc_chunk_size", chunk_size),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "dedup_tier", chunk_pool_name),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }

  cout << "SampleRatio : " << sampling_ratio << std::endl
    << "Chunk Dedup Threshold : " << chunk_dedup_threshold << std::endl
    << "Chunk Size : " << chunk_size << std::endl
    << std::endl;

  while (true) {
    lock_guard lock(glock);
    ObjectCursor begin = io_ctx.object_list_begin();
    ObjectCursor end = io_ctx.object_list_end();
    map<string, librados::pool_stat_t> stats;
    ret = rados.get_pool_stats(pool_names, stats);
    if (ret < 0) {
      cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
      return -EINVAL;
    }
    if (stats.find(base_pool_name) == stats.end()) {
      cerr << "stats can not find pool name: " << base_pool_name << std::endl;
      return -EINVAL;
    }

    SampleDedupWorkerThread::SampleDedupGlobal sample_dedup_global(
      chunk_dedup_threshold, sampling_ratio, report_period, fp_threshold);

    std::list<SampleDedupWorkerThread> threads;
    size_t total_size = 0;
    size_t total_duplicate_size = 0;
    for (unsigned i = 0; i < max_thread; i++) {
      cout << " add thread.. " << std::endl;
      ObjectCursor shard_start;
      ObjectCursor shard_end;
      io_ctx.object_list_slice(
        begin,
        end,
        i,
        max_thread,
        &shard_start,
        &shard_end);

      threads.emplace_back(
	io_ctx,
	chunk_io_ctx,
	shard_start,
	shard_end,
	chunk_size,
	fp_algo,
	chunk_algo,
	sample_dedup_global,
	snap);
      threads.back().create("sample_dedup");
    }

    for (auto &p : threads) {
      total_size += p.get_total_object_size();
      total_duplicate_size += p.get_total_duplicated_size();
      p.join();
    }

    cerr << "Summary: read "
	 << total_size << " bytes so far and found saveable space ("
	 << total_duplicate_size << " bytes)."
	 << std::endl;

    if (loop) {
      sleep(wakeup_period);
    } else {
      break;
    }
  }

  return 0;
}

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }

  po::variables_map opts;
  po::positional_options_description p;
  p.add("command", 1);
  po::options_description desc = make_usage();
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(desc).positional(p).allow_unregistered().run();
    po::store(parsed, opts);
    po::notify(opts);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  if (opts.count("help") || opts.count("h")) {
    cout<< desc << std::endl;
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			CODE_ENVIRONMENT_DAEMON,
			CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  Preforker forker;
  if (global_init_prefork(g_ceph_context) >= 0) {
    std::string err;
    int r = forker.prefork(err);
    if (r < 0) {
      cerr << err << std::endl;
      return r;
    }
    if (forker.is_parent()) {
      g_ceph_context->_log->start();
      if (forker.parent_wait(err) != 0) {
        return -ENXIO;
      }
      return 0;
    }
    global_init_postfork_start(g_ceph_context);
  }
  common_init_finish(g_ceph_context);
  if (opts.count("daemon")) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }
  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  string op_name = get_opts_op_name(opts);
  int ret = 0;
  if (op_name == "estimate") {
    ret = estimate_dedup_ratio(opts);
  } else if (op_name == "chunk-scrub" ||
	     op_name == "chunk-get-ref" ||
	     op_name == "chunk-put-ref" ||
	     op_name == "chunk-repair" ||
	     op_name == "dump-chunk-refs") {
    ret = chunk_scrub_common(opts);
  } else if (op_name == "chunk-dedup" ||
	     op_name == "object-dedup") {
    /*
     * chunk-dedup:
     * using a chunk generated by given source,
     * create a new object in the chunk pool or increase the reference 
     * if the object exists
     * 
     * object-dedup:
     * perform deduplication on the entire object, not a chunk.
     *
     */
    ret = make_dedup_object(opts);
  } else if (op_name == "sample-dedup") {
    ret = make_crawling_daemon(opts);
  } else {
    cerr << "unrecognized op " << op_name << std::endl;
    exit(1);
  }

  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  
  return forker.signal_exit(ret);
}
