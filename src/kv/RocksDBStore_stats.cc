// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <errno.h>
#include "port/port_posix.h"
#include "db/db_impl/db_impl.h"
#include "KeyValueDB.h"
#include "RocksDBStore.h"
#include "common/admin_socket.h"
#include "common/debug.h"

using ceph::bufferlist;
using ceph::Formatter;

static constexpr uint32_t deb = 1; //< db_stats_debug
static constexpr uint32_t obs = 2; //< db_stats_objectstore
static constexpr uint32_t tel = 4; //< db_stats_telemetry
static constexpr uint32_t alw = deb + obs + tel; //< always capture

struct db_ticker {
  uint32_t mode;              //< filter that determines which mode includes this counter
  const char* name;           //< name associated with performace counter
  uint32_t ticker_index = 0;  //< RocksDB id to retrieve data
};

static db_ticker tickers[] = {
{0,        "rocksdb.blobdb.blob.file.bytes.read"},
{0,        "rocksdb.blobdb.blob.file.bytes.written"},
{0,        "rocksdb.blobdb.blob.file.synced"},
{0,        "rocksdb.blobdb.blob.index.evicted.count"},
{0,        "rocksdb.blobdb.blob.index.evicted.size"},
{0,        "rocksdb.blobdb.blob.index.expired.count"},
{0,        "rocksdb.blobdb.blob.index.expired.size"},
{0,        "rocksdb.blobdb.bytes.read"},
{0,        "rocksdb.blobdb.bytes.written"},
{0,        "rocksdb.blobdb.fifo.bytes.evicted"},
{0,        "rocksdb.blobdb.fifo.num.files.evicted"},
{0,        "rocksdb.blobdb.fifo.num.keys.evicted"},
{0,        "rocksdb.blobdb.gc.bytes.expired"},
{0,        "rocksdb.blobdb.gc.bytes.overwritten"},
{0,        "rocksdb.blobdb.gc.bytes.relocated"},
{0,        "rocksdb.blobdb.gc.failures"},
{0,        "rocksdb.blobdb.gc.num.files"},
{0,        "rocksdb.blobdb.gc.num.keys.expired"},
{0,        "rocksdb.blobdb.gc.num.keys.overwritten"},
{0,        "rocksdb.blobdb.gc.num.keys.relocated"},
{0,        "rocksdb.blobdb.gc.num.new.files"},
{alw,      "rocksdb.blobdb.num.get"},
{alw,      "rocksdb.blobdb.num.keys.read"},
{alw,      "rocksdb.blobdb.num.keys.written"},
{alw,      "rocksdb.blobdb.num.multiget"},
{alw,      "rocksdb.blobdb.num.next"},
{alw,      "rocksdb.blobdb.num.prev"},
{alw,      "rocksdb.blobdb.num.put"},
{alw,      "rocksdb.blobdb.num.seek"},
{alw,      "rocksdb.blobdb.num.write"},
{alw,      "rocksdb.blobdb.write.blob"},
{0,        "rocksdb.blobdb.write.blob.ttl"},
{0,        "rocksdb.blobdb.write.inlined"},
{0,        "rocksdb.blobdb.write.inlined.ttl"},
{0,        "rocksdb.block.cache.add"},
{0,        "rocksdb.block.cache.add.failures"},
{0,        "rocksdb.block.cache.add.redundant"},
{deb,      "rocksdb.block.cache.bytes.read"},
{deb,      "rocksdb.block.cache.bytes.write"},
{0,        "rocksdb.block.cachecompressed.add"},
{0,        "rocksdb.block.cachecompressed.add.failures"},
{0,        "rocksdb.block.cachecompressed.hit"},
{0,        "rocksdb.block.cachecompressed.miss"},
{0,        "rocksdb.block.cache.compression.dict.add"},
{0,        "rocksdb.block.cache.compression.dict.add.redundant"},
{0,        "rocksdb.block.cache.compression.dict.bytes.evict"},
{0,        "rocksdb.block.cache.compression.dict.bytes.insert"},
{0,        "rocksdb.block.cache.compression.dict.hit"},
{0,        "rocksdb.block.cache.compression.dict.miss"},
{deb,      "rocksdb.block.cache.data.add"},
{deb,      "rocksdb.block.cache.data.add.redundant"},
{deb,      "rocksdb.block.cache.data.bytes.insert"},
{alw,      "rocksdb.block.cache.data.hit"},
{alw,      "rocksdb.block.cache.data.miss"},
{alw,      "rocksdb.block.cache.filter.add"},
{deb,      "rocksdb.block.cache.filter.add.redundant"},
{deb,      "rocksdb.block.cache.filter.bytes.evict"},
{deb,      "rocksdb.block.cache.filter.bytes.insert"},
{deb,      "rocksdb.block.cache.filter.hit"},
{alw,      "rocksdb.block.cache.filter.miss"},
{alw,      "rocksdb.block.cache.hit"},
{deb,      "rocksdb.block.cache.index.add"},
{deb,      "rocksdb.block.cache.index.add.redundant"},
{deb,      "rocksdb.block.cache.index.bytes.evict"},
{deb,      "rocksdb.block.cache.index.bytes.insert"},
{deb,      "rocksdb.block.cache.index.hit"},
{deb,      "rocksdb.block.cache.index.miss"},
{alw,      "rocksdb.block.cache.miss"},
{deb,      "rocksdb.bloom.filter.full.positive"},
{deb,      "rocksdb.bloom.filter.full.true.positive"},
{deb,      "rocksdb.bloom.filter.micros"},
{deb,      "rocksdb.bloom.filter.prefix.checked"},
{deb,      "rocksdb.bloom.filter.prefix.useful"},
{deb,      "rocksdb.bloom.filter.useful"},
{alw,      "rocksdb.bytes.read"},
{alw,      "rocksdb.bytes.written"},
{deb,      "rocksdb.compaction.cancelled"},
{deb,      "rocksdb.compaction.key.drop.new"},
{deb,      "rocksdb.compaction.key.drop.obsolete"},
{deb,      "rocksdb.compaction.key.drop.range_del"},
{deb,      "rocksdb.compaction.key.drop.user"},
{deb,      "rocksdb.compaction.optimized.del.drop.obsolete"},
{deb,      "rocksdb.compaction.range_del.drop.obsolete"},
{deb,      "rocksdb.compact.read.bytes"},
{deb,      "rocksdb.compact.read.marked.bytes"},
{deb,      "rocksdb.compact.read.periodic.bytes"},
{deb,      "rocksdb.compact.read.ttl.bytes"},
{deb,      "rocksdb.compact.write.bytes"},
{deb,      "rocksdb.compact.write.marked.bytes"},
{deb,      "rocksdb.compact.write.periodic.bytes"},
{deb,      "rocksdb.compact.write.ttl.bytes"},
{0,        "rocksdb.db.iter.bytes.read"},
{0,        "rocksdb.db.mutex.wait.micros"},
{0,        "rocksdb.files.deleted.immediately"},
{0,        "rocksdb.files.marked.trash"},
{0,        "rocksdb.filter.operation.time.nanos"},
{0,        "rocksdb.flush.write.bytes"},
{0,        "rocksdb.getupdatessince.calls"},
{deb,      "rocksdb.l0.hit"},
{deb,      "rocksdb.l0.num.files.stall.micros"},
{deb,      "rocksdb.l0.slowdown.micros"},
{deb,      "rocksdb.l1.hit"},
{deb,      "rocksdb.l2andup.hit"},
{deb,      "rocksdb.memtable.compaction.micros"},
{alw,      "rocksdb.memtable.hit"},
{alw,      "rocksdb.memtable.miss"},
{deb,      "rocksdb.merge.operation.time.nanos"},
{deb,      "rocksdb.no.file.closes"},
{deb,      "rocksdb.no.file.errors"},
{deb,      "rocksdb.no.file.opens"},
{0,        "rocksdb.number.block.compressed"},
{0,        "rocksdb.number.block.decompressed"},
{0,        "rocksdb.number.block.not_compressed"},
{0,        "rocksdb.number.db.next"},
{0,        "rocksdb.number.db.next.found"},
{0,        "rocksdb.number.db.prev"},
{0,        "rocksdb.number.db.prev.found"},
{0,        "rocksdb.number.db.seek"},
{0,        "rocksdb.number.db.seek.found"},
{0,        "rocksdb.number.deletes.filtered"},
{0,        "rocksdb.number.direct.load.table.properties"},
{0,        "rocksdb.number.iter.skip"},
{0,        "rocksdb.number.keys.read"},
{0,        "rocksdb.number.keys.updated"},
{0,        "rocksdb.number.keys.written"},
{0,        "rocksdb.number.merge.failures"},
{0,        "rocksdb.number.multiget.bytes.read"},
{0,        "rocksdb.number.multiget.get"},
{0,        "rocksdb.number.multiget.keys.found"},
{0,        "rocksdb.number.multiget.keys.read"},
{0,        "rocksdb.number.rate_limiter.drains"},
{0,        "rocksdb.number.reseeks.iteration"},
{0,        "rocksdb.number.superversion_acquires"},
{0,        "rocksdb.number.superversion_cleanups"},
{0,        "rocksdb.number.superversion_releases"},
{0,        "rocksdb.num.iterator.created"},
{0,        "rocksdb.num.iterator.deleted"},
{0,        "rocksdb.num.iterators"},
{0,        "rocksdb.persistent.cache.hit"},
{0,        "rocksdb.persistent.cache.miss"},
{0,        "rocksdb.rate.limit.delay.millis"},
{0,        "rocksdb.read.amp.estimate.useful.bytes"},
{0,        "rocksdb.read.amp.total.read.bytes"},
{0,        "rocksdb.row.cache.hit"},
{0,        "rocksdb.row.cache.miss"},
{0,        "rocksdb.sim.block.cache.hit"},
{0,        "rocksdb.sim.block.cache.miss"},
{0,        "rocksdb.stall.micros"},
{0,        "rocksdb.txn.get.tryagain"},
{0,        "rocksdb.txn.overhead.duplicate.key"},
{0,        "rocksdb.txn.overhead.mutex.old.commit.map"},
{0,        "rocksdb.txn.overhead.mutex.prepare"},
{0,        "rocksdb.txn.overhead.mutex.snapshot"},
{deb,      "rocksdb.wal.bytes"},
{deb,      "rocksdb.wal.synced"},
{0,        "rocksdb.write.other"},
{0,        "rocksdb.write.self"},
{0,        "rocksdb.write.timeout"},
{0,        "rocksdb.write.wal"}
};

static db_ticker histograms[] = {
{0,        "rocksdb.blobdb.blob.file.read.micros"},
{0,        "rocksdb.blobdb.blob.file.sync.micros"},
{0,        "rocksdb.blobdb.blob.file.write.micros"},
{0,        "rocksdb.blobdb.compression.micros"},
{0,        "rocksdb.blobdb.decompression.micros"},
{0,        "rocksdb.blobdb.gc.micros"},
{0,        "rocksdb.blobdb.get.micros"},
{0,        "rocksdb.blobdb.key.size"},
{0,        "rocksdb.blobdb.multiget.micros"},
{0,        "rocksdb.blobdb.next.micros"},
{0,        "rocksdb.blobdb.prev.micros"},
{0,        "rocksdb.blobdb.seek.micros"},
{0,        "rocksdb.blobdb.value.size"},
{0,        "rocksdb.blobdb.write.micros"},
{0,        "rocksdb.bytes.compressed"},
{0,        "rocksdb.bytes.decompressed"},
{0,        "rocksdb.bytes.per.multiget"},
{0,        "rocksdb.bytes.per.read"},
{0,        "rocksdb.bytes.per.write"},
{0,        "rocksdb.compaction.outfile.sync.micros"},
{alw,      "rocksdb.compaction.times.cpu_micros"},
{alw,      "rocksdb.compaction.times.micros"},
{0,        "rocksdb.compression.times.nanos"},
{deb,      "rocksdb.db.flush.micros"},
{alw,      "rocksdb.db.get.micros"},
{deb,      "rocksdb.db.multiget.micros"},
{deb,      "rocksdb.db.seek.micros"},
{alw,      "rocksdb.db.write.micros"},
{deb,      "rocksdb.db.write.stall"},
{0,        "rocksdb.decompression.times.nanos"},
{0,        "rocksdb.hard.rate.limit.delay.count"},
{0,        "rocksdb.l0.slowdown.count"},
{0,        "rocksdb.manifest.file.sync.micros"},
{0,        "rocksdb.memtable.compaction.count"},
{0,        "rocksdb.num.data.blocks.read.per.level"},
{0,        "rocksdb.numfiles.in.singlecompaction"},
{0,        "rocksdb.num.files.stall.count"},
{0,        "rocksdb.num.index.and.filter.blocks.read.per.level"},
{0,        "rocksdb.num.sst.read.per.level"},
{0,        "rocksdb.num.subcompactions.scheduled"},
{0,        "rocksdb.read.block.compaction.micros"},
{0,        "rocksdb.read.block.get.micros"},
{0,        "rocksdb.read.num.merge_operands"},
{0,        "rocksdb.soft.rate.limit.delay.count"},
{deb,      "rocksdb.sst.batch.size"},
{alw,      "rocksdb.sst.read.micros"},
{0,        "rocksdb.subcompaction.setup.times.micros"},
{0,        "rocksdb.table.open.io.micros"},
{0,        "rocksdb.table.sync.micros"},
{alw,      "rocksdb.wal.file.sync.micros"},
{0,        "rocksdb.write.raw.block.micros"},
};

#undef dout_context
#define dout_context cct
#undef dout_subsys
#define dout_subsys ceph_subsys_rocksdb
#undef dout_prefix
#define dout_prefix *_dout << "rocksdb: "

class RocksDBStore::db_stats {
public:
  enum db_stats_mode : uint32_t {
    all = 0,
    debug = 1,
    objectstore = 2,
    telemetry = 3,
    stats_max
  };

  db_stats(RocksDBStore* db) : db(db), cct(db->cct) {
    init_assign_ticker_index(cct);
    init_assign_histogram_index(cct);
  }

  void print(ceph::Formatter *f, db_stats_mode mode) {
    f->open_object_section("stats");
    print_main_stats(f, mode);
    print_histograms(f, mode);
    print_cfstats(f, mode);
    f->close_section();
  }
private:
  RocksDBStore* db;
  CephContext* cct;

  struct counter {
    uint32_t mode;
    const char* name;
  };

  enum cf_sum_e {
    sum_everything,
    sum_within_shard,
    show_everything
  };

  enum comp_cnt_e {
    AvgSec = 0,
    CompCount,
    CompMergeCPU,
    CompSec,
    CompactedFiles,
    KeyDrop,
    KeyIn,
    MovedGB,
    NumFiles,
    ReadGB,
    ReadMBps,
    RnGB,
    Rnp1GB,
    Score,
    SizeBytes,
    WnewGB,
    WriteAmp,
    WriteGB,
    WriteMBps,
    CC_MAX
  };
  
  static constexpr counter comp_cnt[CC_MAX] =
    {
      {0,       "AvgSec"},
      {deb,     "CompCount"},
      {deb,     "CompMergeCPU"},
      {0,       "CompSec"},
      {0,       "CompactedFiles"},
      {alw,     "KeyDrop"},
      {alw,     "KeyIn"},
      {0,       "MovedGB"},
      {alw,     "NumFiles"},
      {deb,     "ReadGB"},
      {deb,     "ReadMBps"},
      {0,       "RnGB"},
      {0,       "Rnp1GB"},
      {0,       "Score"},
      {0,       "SizeBytes"},
      {0,       "WnewGB"},
      {deb,     "WriteAmp"},
      {deb,     "WriteGB"},
      {deb,     "WriteMBps"}
    };

  struct stats_tab_t {
    double tab[CC_MAX] = {0};
  };

  enum io_stalls_e {
    level0_numfiles = 0,
    level0_numfiles_with_compaction,
    level0_slowdown,
    level0_slowdown_with_compaction,
    memtable_compaction,
    memtable_slowdown,
    slowdown_for_pending_compaction_bytes,
    stop_for_pending_compaction_bytes,
    total_slowdown,
    total_stop,
    IO_STALLS_MAX
  };

  static constexpr counter io_stalls[IO_STALLS_MAX] =
    {
      {deb,     "level0_numfiles"},
      {deb,     "level0_numfiles_with_compaction"},
      {deb,     "level0_slowdown"},
      {deb,     "level0_slowdown_with_compaction"},
      {0,       "memtable_compaction"},
      {0,       "memtable_slowdown"},
      {deb,     "slowdown_for_pending_compaction_bytes"},
      {deb,     "stop_for_pending_compaction_bytes"},
      {alw,     "total_slowdown"},
      {alw,     "total_stop"}
    };

  struct cfstats_t {
    stats_tab_t sum;
    std::vector<stats_tab_t> l;
    double iostall[IO_STALLS_MAX] = {0};
  };

  static void init_assign_ticker_index(CephContext* cct)
  {
    static bool done = false;
    if (done) {
      return;
    }
    dout(5) << __func__ << " start" << dendl;
    std::map<std::string, uint32_t> tickers_map;
    for (auto& t: rocksdb::TickersNameMap) {
      tickers_map.emplace(t.second, t.first);
    }
    for (auto& c: tickers) {
      auto it = tickers_map.find(c.name);
      if (it != tickers_map.end()) {
	c.ticker_index = it->second;
      } else {
	dout(10) << "ticker " << c.name << " not found in rocksdb" << dendl;
      }
    }
    dout(5) << __func__ << " finish" << dendl;
    done = true;
  }

  static void init_assign_histogram_index(CephContext* cct)
  {
    static bool done = false;
    if (done) {
      return;
    }
    dout(5) << __func__ << " start" << dendl;
    std::map<std::string, uint32_t> histograms_map;
    for (auto& t: rocksdb::HistogramsNameMap) {
      histograms_map.emplace(t.second, t.first);
    }
    for (auto& c: histograms) {
      auto it = histograms_map.find(c.name);
      if (it != histograms_map.end()) {
	c.ticker_index = it->second;
      } else {
	dout(10) << "histogram " << c.name << " not found in rocksdb" << dendl;
      }
    }
    dout(5) << __func__ << " finish" << dendl;
    done = true;
  }

  void dump_float(ceph::Formatter *f, std::string_view name, double v, int prec = 6) {
    if (trunc(v) != v) {
      f->dump_format_unquoted(name, "%.*f", prec, v);
    } else {
      f->dump_int(name, trunc(v));
    }
  }

  void print_main_stats(ceph::Formatter *f, db_stats_mode mode) {
    uint32_t mode_bits = get_mode_bits(mode);
    f->open_object_section("main");
    for (auto &c : tickers) {
      if (mode == all || (mode_bits & c.mode) != 0) {
	if (db->dbstats) {
	  uint64_t val = db->dbstats->getTickerCount(c.ticker_index);
	  f->dump_int(c.name, val);
	} else {
	  f->dump_int(c.name, -1);
	}
      }
    }
    f->close_section();
  }

  void print_histograms(ceph::Formatter *f, db_stats_mode mode) {
    uint32_t mode_bits = get_mode_bits(mode);
    f->open_object_section("histogram");
    for (auto &c : histograms) {
      if (mode == all || (mode_bits & c.mode) != 0) {
	rocksdb::HistogramData data;
	f->open_object_section(c.name);
	if (db->dbstats) {
	  db->dbstats->histogramData(c.ticker_index, &data);
	  dump_float(f, "p50", data.median);
	  dump_float(f, "p95", data.percentile95);
	  dump_float(f, "p99", data.percentile99);
	  dump_float(f, "avg", data.average);
	  dump_float(f, "std", data.standard_deviation);
	  dump_float(f, "min", data.min);
	  dump_float(f, "max", data.max);
	  dump_float(f, "count", data.count);
	  dump_float(f, "sum", data.sum);
	} else {
	  f->dump_float("p50", -1);
	  f->dump_float("p95", -1);
	  f->dump_float("p99", -1);
	  f->dump_float("avg", -1);
	  f->dump_float("std", -1);
	  f->dump_float("min", -1);
	  f->dump_float("max", -1);
	  f->dump_float("count", -1);
	  f->dump_float("sum", -1);
	}
	f->close_section();
      }
    }
    f->close_section();
  }

  void print_cfstats(ceph::Formatter *f, db_stats_mode mode) {
    f->open_object_section("columns");
    switch (mode) {
    case all:
      cfstats_show_everything(f, mode);
      break;
    case debug:
    case objectstore:
      cfstats_sum_withing_shard(f, mode);
      break;
    case telemetry:
      cfstats_sum_everything(f, mode);
      break;
    default:
      ceph_assert(false && "cannot happen");
    }
    f->close_section();
  }

  double sum(const std::vector<stats_tab_t*>& tabs, uint32_t index) {
    double r = 0;
    for (auto& c : tabs) {
      r += c->tab[index];
    }
    return r;
  }

  double avg(const std::vector<stats_tab_t*>& tabs, uint32_t index) {
    double r = 0;
    for (auto& c : tabs) {
      r += c->tab[index];
    }
    return r / tabs.size();
  }
  
  double weighted(const std::vector<stats_tab_t*>& tabs,
		  uint32_t avg_index,
		  uint32_t cnt_index) {
    size_t elems = 0;
    double s = 0;
    double d = 0;
    for (auto& t : tabs) {
      // avg = cnt / denominator
      if (t->tab[avg_index] > 0) {
	// reconstruct original denominator
	double denom = t->tab[cnt_index] / t->tab[avg_index];
	d += denom;
	s += t->tab[cnt_index];
	elems++;
      }
    }
    if (elems == 0)
      return 0;
    return s / d;
  }
  
  stats_tab_t sum_tabs(const std::vector<stats_tab_t*>& tabs) {
    stats_tab_t result;
    if (tabs.size() > 0) {
      result.tab[AvgSec]         = weighted(tabs, AvgSec, CompCount);
      result.tab[CompCount]      = sum(tabs, CompCount);
      result.tab[CompMergeCPU]   = sum(tabs, CompMergeCPU);
      result.tab[CompSec]        = sum(tabs, CompSec);
      result.tab[CompactedFiles] = sum(tabs, CompactedFiles);
      result.tab[KeyDrop]        = sum(tabs, KeyDrop);
      result.tab[KeyIn]          = sum(tabs, KeyIn);
      result.tab[MovedGB]        = sum(tabs, MovedGB);
      result.tab[NumFiles]       = sum(tabs, NumFiles);
      result.tab[ReadGB]         = sum(tabs, ReadGB);
      result.tab[ReadMBps]       = weighted(tabs, ReadMBps, ReadGB);
      result.tab[RnGB]           = sum(tabs, RnGB);
      result.tab[Rnp1GB]         = sum(tabs, Rnp1GB);
      result.tab[Score]          = avg(tabs, Score);
      result.tab[SizeBytes]      = sum(tabs, SizeBytes);
      result.tab[WnewGB]         = sum(tabs, WnewGB);
      result.tab[WriteAmp]       = sum(tabs, WriteAmp);
      result.tab[WriteGB]        = sum(tabs, WriteGB);
      result.tab[WriteMBps]      = weighted(tabs, WriteMBps, WriteGB);
    }
    return result;
  };

  cfstats_t sum_stats(std::vector<cfstats_t>& cf_stats_batch) {
    cfstats_t result;
    std::vector<stats_tab_t*> tabs;
    size_t max_l = 0; //the deepest L level
    for (auto& c : cf_stats_batch) {
      max_l = std::max(max_l, c.l.size());
      tabs.push_back(&c.sum);
    }
    result.sum = sum_tabs(tabs);
    
    result.l.resize(max_l);
    for (size_t m = 0; m < max_l; m++) {
      // for level m select only CFs that reached this level
      tabs.clear();
      for (auto& c : cf_stats_batch) {
	if (m < c.l.size()) {
	  tabs.push_back(&c.l[m]);
	}
      }
      // ... and sum/average it
      result.l[m] = sum_tabs(tabs);
    }
    return result;
  }

  void capture_cfstats(rocksdb::ColumnFamilyHandle *cf, cfstats_t& cf_stats) {
    rocksdb::DBImpl* dbi = dynamic_cast<rocksdb::DBImpl*>(db->db);
    ceph_assert(dbi);
    std::map<std::string, std::string> valuemap;
    bool mapok = dbi->GetMapProperty(cf, "rocksdb.cfstats", &valuemap);
    if (mapok) {
      for (auto& v: valuemap) {
	dout(20) << __func__ << " value=" << v.first << dendl;
	const std::string& name = v.first;
	if (name.substr(0, strlen("compaction.")) == "compaction.") {
	  std::string name_c = name.substr(strlen("compaction."));
	  size_t pos = name_c.find(".");
	  if (pos != std::string::npos) {
	    std::string level = name_c.substr(0, pos);
	    stats_tab_t* tab;
	    if (level == "Sum") {
	      tab = &cf_stats.sum;
	    } else if (level.size() > 1 && level[0] == 'L') {
	      unsigned int level_int = std::stoi(level.substr(1));
	      if (level_int >= cf_stats.l.size()) {
		cf_stats.l.resize(level_int + 1);
	      }
	      tab = &cf_stats.l[level_int];
	    } else {
	      derr << __func__ << " bad level " << level << dendl;
	      continue;
	    }
	    std::string param = name_c.substr(pos + 1);
	    dout(20) << "level=" << level << " param=" << param << " value=" << v.second << dendl;
	    size_t i;
	    for (i = 0; i < CC_MAX; i++) {
	      if (param == comp_cnt[i].name) {
		tab->tab[i] = std::stod(v.second);
		break;
	      }
	    }
	    if (i == CC_MAX) {
	      dout(10) << __func__ << " unrecognized " << param << dendl;
	    }
	  } else {
	    dout(20) << "cant split " << name_c << dendl;
	  }
	} else if (name.substr(0, strlen("io_stalls.")) == "io_stalls.") {
	  std::string stall = name.substr(strlen("io_stalls."));
	  size_t i;
	  for (i = 0; i < IO_STALLS_MAX; i++) {
	    if (stall == io_stalls[i].name) {
	      cf_stats.iostall[i] = std::stod(v.second);
	      break;
	    }
	  }
	  if (i == IO_STALLS_MAX) {
	    dout(10) << __func__ << " unrecognized " << stall << dendl;
	  }
	}      
      }
    }
  }

  uint32_t get_mode_bits(db_stats_mode mode)
  {
    switch (mode) {
    case all:
      return alw;
    case debug:
      return deb;
    case objectstore:
      return obs;
    case telemetry:
      return tel;
    default:
      ceph_assert(false);
    }
  }
  
  void dump_cfstats(ceph::Formatter *f, const cfstats_t& cf_stats, db_stats_mode mode) {
    uint32_t mode_bits = get_mode_bits(mode);
    f->open_object_section("sum");
    for (size_t i = 0; i < CC_MAX; i++) {
      if (mode == all || (mode_bits & comp_cnt[i].mode) != 0) {
	dump_float(f, comp_cnt[i].name, cf_stats.sum.tab[i]);
      }
    }
    f->close_section();
    if (mode != telemetry) {
      for (size_t l = 0; l < cf_stats.l.size(); l++) {
	f->open_object_section("l" + std::to_string(l));
	for (size_t i = 0; i < CC_MAX; i++) {
	  if (mode == all || (mode_bits & comp_cnt[i].mode) != 0) {
	    dump_float(f, comp_cnt[i].name, cf_stats.l[l].tab[i]);
	  }
	}
	f->close_section();
      }
    }
    for (size_t i = 0; i < IO_STALLS_MAX; i++) {
      if (mode == all || (mode_bits & io_stalls[i].mode) != 0) {
	dump_float(f, io_stalls[i].name, cf_stats.iostall[i]);
      }
    }
  };

  void cfstats_sum_everything(ceph::Formatter *f, db_stats_mode mode) {
    std::vector<cfstats_t> cf_stats_batch;
    cfstats_t cf_stats;
    capture_cfstats(db->default_cf, cf_stats);
    cf_stats_batch.push_back(cf_stats);
    for (auto& cfh : db->cf_handles) {
      for (size_t s = 0; s < cfh.second.handles.size(); s++) {
	cfstats_t cf_stats;
	capture_cfstats(cfh.second.handles[s], cf_stats);
	cf_stats_batch.push_back(cf_stats);
      }
    }
    cf_stats = sum_stats(cf_stats_batch);
    f->open_object_section("all_columns");
    dump_cfstats(f, cf_stats, mode);
    f->close_section();
  }

  void cfstats_sum_withing_shard(ceph::Formatter *f, db_stats_mode mode) {
    cfstats_t cf_stats;
    f->open_object_section("default");
    capture_cfstats(db->default_cf, cf_stats);
    dump_cfstats(f, cf_stats, mode);
    f->close_section();

    for (auto& cfh : db->cf_handles) {
      cfstats_t cf_stats;
      std::vector<cfstats_t> cf_stats_batch;
      for (size_t s = 0; s < cfh.second.handles.size(); s++) {
	capture_cfstats(cfh.second.handles[s], cf_stats);
	cf_stats_batch.push_back(cf_stats);
      }
      cf_stats = sum_stats(cf_stats_batch);
      f->open_object_section(cfh.first);
      dump_cfstats(f, cf_stats, mode);
      f->close_section();
    }
  }

  void cfstats_show_everything(ceph::Formatter *f, db_stats_mode mode) {
    cfstats_t cf_stats;
    f->open_object_section("default");
    capture_cfstats(db->default_cf, cf_stats);
    dump_cfstats(f, cf_stats, mode);
    f->close_section();
    
    for (auto& cfh : db->cf_handles) {
      for (size_t s = 0; s < cfh.second.handles.size(); s++) {
        cfstats_t cf_stats;
	capture_cfstats(cfh.second.handles[s], cf_stats);
	if (cfh.second.handles.size() == 1) {
	  f->open_object_section(cfh.first);
	} else {
	  f->open_object_section(cfh.first + "-" + std::to_string(s));
	}
	dump_cfstats(f, cf_stats, mode);
	f->close_section();
      }
    }
  }  
};

class RocksDBStore::SocketHook : public AdminSocketHook {
  RocksDBStore* db;
public:
  SocketHook(RocksDBStore* db)
    : db(db) {};
  ~SocketHook() {
    AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
    if (admin_socket && db->hook) {
      db->hook = nullptr;
      admin_socket->unregister_commands(this);
    }
  }
  static int create(RocksDBStore* db) {
    int r = -1;
    AdminSocket *admin_socket = db->cct->get_admin_socket();
    if (admin_socket && db->hook == nullptr) {
      SocketHook* hook = new SocketHook(db);
      r = admin_socket->register_command(
	"dump_rocksdb_stats name=level,type=CephString",       
	hook,
	"dump_rocksdb_stats <'telemetry'/'objectstore'/'debug'/'all'> ; dump RocksDB stats");
      if (r == 0) {
	db->hook = hook;
      }
    }
    return r;
  }

  int call(std::string_view command,
	   const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& ss,
	   bufferlist& out) override {
    int r = 0;
    if (command == "dump_rocksdb_stats") {
      std::string level;
      db_stats::db_stats_mode mode = db_stats::telemetry;
      if (ceph::common::cmd_getval(cmdmap, "level", level)) {
	if (level == "telemetry") mode = db_stats::telemetry;
	else if (level == "objectstore") mode = db_stats::objectstore;
	else if (level == "debug") mode = db_stats::debug;
	else if (level == "all") mode = db_stats::all;
	else return -EINVAL;
      }
      db_stats(db).print(f, mode);
      r = 0;
    } else {
      ss << "Invalid command" << std::endl;
      r = -ENOSYS;
    }
    return r;
  }
};

int RocksDBStore::register_stats_hook() {
  int r;
  r = SocketHook::create(this);
  return r;
}

void RocksDBStore::unregister_stats_hook() {
  delete hook;
  hook = nullptr;
}
