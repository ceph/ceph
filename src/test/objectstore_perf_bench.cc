// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Random read write performance benchmark for objectstore.
 *
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#include <chrono>
#include <cassert>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "os/ObjectStore.h"

#include "global/global_init.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/perf_counters.h"

#define OBJ_SIZE (4 * 1024 * 1024)

#define dout_subsys ceph_subsys_tests

static void usage()
{
  derr << "usage: ceph_objectstore_bench [options]\n"
      "   --conf\n"
      "         ceph.conf file location.\n"
      "   --size\n"
      "         total size in bytes\n"
      "   --block-size\n"
      "         block size in bytes for each write\n"
      "   --threads\n"
      "         number of threads to carry out this workload\n"
      "   --write_pct\n"
      "         % of writes in read + write workloads. Default is 100%.\n"
      "   --time\n"
      "         total time in seconds this test should run..\n"
      "   --stats_interval\n"
      "         time interval in second for stats print.\n"
      "   --fill\n"
      "         fill device first\n" << dendl;
  generic_server_usage();
}

static int64_t total_ops = 0;
static int64_t total_read_ops = 0;
static int64_t total_write_ops = 0;
static int64_t total_time = 1;
static bool _test_done = false;

// helper class for bytes with units
struct byte_units {
  size_t v;
  // cppcheck-suppress noExplicitConstructor
  byte_units(size_t v) : v(v) {}

  bool parse(const std::string &val, std::string *err);

  operator size_t() const { return v; }
};

bool byte_units::parse(const std::string &val, std::string *err)
{
  v = strict_sistrtoll(val.c_str(), err);
  return err->empty();
}

std::ostream& operator<<(std::ostream &out, const byte_units &amount)
{
  static const char* units[] = { "B", "KB", "MB", "GB", "TB", "PB", "EB" };
  static const int max_units = sizeof(units)/sizeof(*units);

  int unit = 0;
  auto v = amount.v;
  while (v >= 1024 && unit < max_units) {
    // preserve significant bytes
    if (v < 1048576 && (v % 1024 != 0))
      break;
    v >>= 10;
    unit++;
  }
  return out << v << ' ' << units[unit];
}

struct Config {
  byte_units size;
  byte_units block_size;
  int threads;
  int time_secs;
  bool fill;
  int write_pct;
  int stats_interval;
  ObjectStore *os;
  
  Config()
    : size(OBJ_SIZE), block_size(4096), threads(1), stats_interval(5) {}
};
uint64_t total_test_time = 900;

inline uint64_t
get_time_usecs(void)
{
  struct timeval tv = { 0, 0};
  gettimeofday(&tv, NULL);
  return ((tv.tv_sec * 1000 * 1000) + tv.tv_usec);
}

std::atomic_int _tid_count;
thread_local int _my_tid = 0;
std::atomic<uint64_t> _total_data_written;
std::atomic_int _not_filled;

void
stats_thd(const Config *cfg)
{
  int64_t last_ops = 0;
  int64_t last_time = get_time_usecs();
  int count = 100;
  int stats_interval = cfg->stats_interval;

//  cfg->os->set_flag("skip_aio_writes", "1");
  while (_not_filled) {
    sleep(5);
    dout(10) << "Stats: written " << byte_units(_total_data_written) <<" of "<< cfg->size <<"." <<dendl;
  }

//  cfg->os->set_flag("skip_aio_writes", "0");
  total_time = get_time_usecs();
  while(!_test_done) {
    sleep(stats_interval);
    int64_t divisor = ((get_time_usecs() - total_time) /1000000);
    int64_t time_i = get_time_usecs();
    int64_t divisor_i = ((time_i - last_time) /1000000);
    if (count++ > 20) {
      count = 0;
      PerfCounters *pf = cfg->os->get_perf_counter();
      bufferlist bl;
      Formatter *f = Formatter::create("json-pretty");
      pf->dump_formatted(f, 0);
      f->flush(bl);
      delete f;
      bl.append('\0');
      dout(15) << "Perf Counters = " << bl.c_str() << dendl;
    }
    last_time = time_i;
    dout(10) <<"Stats: tput = "<< total_ops / divisor << ", write ops = " << total_write_ops / divisor  <<
                             ", read ops = " << total_read_ops / divisor  <<" , total_ops = "<< total_ops << 
                              ", Itput = " << (total_ops - last_ops) / divisor_i << "." << dendl;
    last_ops = total_ops;

  }
  total_time = (get_time_usecs() - total_time) / 1000000;
  dout(0) << "Stats : Total Ops = " << total_ops << ", Total time = " << total_time << 
             ", Read:Write = " << total_read_ops << ":" << total_write_ops << dendl;
}


class C_NotifyCond : public Context {
  std::mutex *mutex;
  std::condition_variable *cond;
  bool *done;
public:
  C_NotifyCond(std::mutex *mutex, std::condition_variable *cond, bool *done)
    : mutex(mutex), cond(cond), done(done) {}
  void finish(int r) {
    std::lock_guard<std::mutex> lock(*mutex);
    *done = true;
    cond->notify_one();
  }
};

void osbench_worker(ObjectStore *os, const Config &cfg)
{

  _my_tid = _tid_count.fetch_add(1);

  int obj_size = OBJ_SIZE; 
  uint32_t block_size = cfg.block_size;
  int64_t my_size = cfg.size / cfg.threads;
  int num_objs = my_size / obj_size;
  spg_t pg(pg_t(rand(), 0, 0), shard_id_t(rand()));
  std::stringstream oss;

  const coll_t cid(pg);
  {
    ObjectStore::Sequencer osr(__func__);
    ObjectStore::Transaction t;
    t.create_collection(cid, 0xff);
    os->apply_transaction(&osr, std::move(t));
  }

  dout(10) << "Thread id " << _my_tid << ", objs = " << num_objs << ", size = "<<
      obj_size << ", total = " << my_size << dendl;

  oss << "osbench-thread-" << rand();
  std::vector<ghobject_t *> *oids = new std::vector<ghobject_t *>(num_objs);
  for (auto p = oids->begin(); p != oids->end(); p++) {
    std::string s = oss.str();
    s.append(std::to_string(rand()));;
    (*p) = new ghobject_t(pg.make_temp_hobject(s.c_str()));
  }

  dout(10) << "Created " << oids->size() << " oids" << dendl;

  ObjectStore::Sequencer osr(__func__);
  {
    int count = 0;
    for (auto p = oids->begin(); p != oids->end(); p++) {
      ObjectStore::Transaction t;
      ghobject_t *c_oid = (*oids)[count];
      t.touch(cid, *c_oid);
      int r = os->apply_transaction(&osr, std::move(t));
      assert(r == 0);
      count++;
    }
  }

  if (cfg.fill) {
    uint64_t data_filled = 0;
    int64_t count = 0;
    bufferlist data;
    data.append(buffer::create(obj_size));

    dout(10) << "Thread id " << _my_tid << " doing " << my_size << " data fill" << dendl;
    
    for (auto p = oids->begin(); p != oids->end(); p++) {

      ObjectStore::Transaction t;
      t.write(cid, **p, 0, obj_size, data);
      int r = os->apply_transaction(&osr, std::move(t));
      assert(r == 0);
      data_filled += obj_size;

      if (count++ > 100) {
        _total_data_written.fetch_add(data_filled);
        count = 0;
        data_filled = 0;
      }
    }
    dout(10) << "Thread id " << _my_tid << " done " << byte_units(my_size) << " data fill" << dendl;
  }


  sleep(10);
  _not_filled.fetch_sub(1);

  bufferlist data;
  data.append(buffer::create(cfg.block_size));
  int count = 0;
  int read_ops = (100 - cfg.write_pct) / 10;
  int write_ops = cfg.write_pct / 10;

  while (!_test_done) {
    for (int i = 0; i < write_ops; i++) {
      ObjectStore::Transaction t;
      ghobject_t *oidp = (*oids)[rand() % num_objs];

      uint64_t offset = ((rand() % obj_size) / block_size) * block_size;
      uint32_t length = block_size;
      t.write(cid, *oidp, offset, length, data);
      int r = os->apply_transaction(&osr, std::move(t));
      assert(r == 0);
    }

    for (int i = 0; i < read_ops; i++) {
      ghobject_t *oidp = (*oids)[rand() % num_objs];

      uint64_t offset = ((rand() % obj_size) / block_size) * block_size;
      int length = block_size;
     
      int r = os->read(cid, *oidp, offset, length, data); 
      assert(r > 0 && r == length);
    }

    count += read_ops + write_ops;
    if ((count > 1000) == 0) {
      __sync_fetch_and_add(&total_ops, count);
      __sync_fetch_and_add(&total_read_ops, read_ops);
      __sync_fetch_and_add(&total_write_ops, write_ops);
      count = 0;
    }
  }

   dout(10) << "Thread id " << _my_tid << " done. exiting. " << dendl; 
}

int main(int argc, const char *argv[])
{

  // command-line arguments
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  Config cfg;

  _tid_count = 0;
  cfg.time_secs = 900;
  cfg.write_pct = 100;
  _total_data_written = 0;

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY, 0);

  std::string val;
  vector<const char*>::iterator i = args.begin();
  while (i != args.end()) {
    if (ceph_argparse_double_dash(args, i))
      break;

    if (ceph_argparse_witharg(args, i, &val, "--size", (char*)nullptr)) {
      std::string err;
      if (!cfg.size.parse(val, &err)) {
        derr << "error parsing size: " << err << dendl;
        usage();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--block-size", (char*)nullptr)) {
      std::string err;
      if (!cfg.block_size.parse(val, &err)) {
        derr << "error parsing block-size: " << err << dendl;
        usage();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--time", (char*)nullptr)) {
      cfg.time_secs = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--write_pct", (char*)nullptr)) {
      cfg.write_pct = atoi(val.c_str());
      if (cfg.write_pct > 100) {
        derr << "Invalid write precentage ratio. "<< dendl;
        return -1;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--threads", (char*)nullptr)) {
      cfg.threads = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--stats_interval", (char*)nullptr)) {
      cfg.stats_interval = atoi(val.c_str());
    } else if (ceph_argparse_flag(args, i, "--fill", (char*)nullptr)) {
      cfg.fill = true;
    } else {
      derr << "Error: can't understand argument: " << *i << "\n" << dendl;
      usage();
    }
  }

  if (cfg.write_pct < 100) {
    cfg.fill = true;
  }

  common_init_finish(g_ceph_context);

  // create object store
  dout(0) << "objectstore " << g_conf->osd_objectstore << dendl;
  dout(0) << "data " << g_conf->osd_data << dendl;
  dout(0) << "journal " << g_conf->osd_journal << dendl;
  dout(0) << "size " << cfg.size << dendl;
  dout(0) << "block-size " << cfg.block_size << dendl;
  dout(0) << "write% " << cfg.write_pct<< dendl;
  dout(0) << "fill" << cfg.fill<< dendl;
  dout(0) << "threads " << cfg.threads << dendl;
  dout(0) << "stats-interval " << cfg.stats_interval<< dendl;
  dout(0) << "time " << cfg.time_secs << dendl;

  auto os = std::unique_ptr<ObjectStore>(
      ObjectStore::create(g_ceph_context,
                          g_conf->osd_objectstore,
                          g_conf->osd_data,
                          g_conf->osd_journal));

  os->set_cache_shards(g_conf->osd_op_num_shards);

  _not_filled = 0;
  if (cfg.fill) {
    _not_filled = cfg.threads;
  }
  cfg.os = os.get();

  //Checking data folder: create if needed or error if it's not empty
  DIR *dir = ::opendir(g_conf->osd_data.c_str());
  if (!dir) {
    std::string cmd("mkdir -p ");
    cmd+=g_conf->osd_data;
    int r = ::system( cmd.c_str() );
    if( r<0 ){
      derr << "Failed to create data directory, ret = " << r << dendl;
      return 1;
    }
  }
  else {
     bool non_empty = readdir(dir) != NULL && readdir(dir) != NULL && readdir(dir) != NULL;
     if( non_empty ){
       derr << "Data directory '"<<g_conf->osd_data<<"' isn't empty, please clean it first."<< dendl;
       return 1;
     }
  }
  ::closedir(dir);

  //Create folders for journal if needed
  string journal_base = g_conf->osd_journal.substr(0, g_conf->osd_journal.rfind('/'));
  struct stat sb;
  if (stat(journal_base.c_str(), &sb) != 0 ){
    std::string cmd("mkdir -p ");
    cmd+=journal_base;
    int r = ::system( cmd.c_str() );
    if( r<0 ){
      derr << "Failed to create journal directory, ret = " << r << dendl;
      return 1;
    }
  }

  if (!os) {
    derr << "bad objectstore type " << g_conf->osd_objectstore << dendl;
    return 1;
  }
  if (os->mkfs() < 0) {
    derr << "mkfs failed" << dendl;
    return 1;
  }
  if (os->mount() < 0) {
    derr << "mount failed" << dendl;
    return 1;
  }

  dout(10) << "created objectstore " << os.get() << dendl;

  // create a collection
  spg_t pg;
  const coll_t cid(pg);
  {
    ObjectStore::Sequencer osr(__func__);
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    os->apply_transaction(&osr, std::move(t));
  }


  // run the worker threads
  std::vector<std::thread> workers;
  workers.reserve(cfg.threads);


  std::thread stats_thd1(stats_thd, &cfg); 

  using namespace std::chrono;
  for (int i = 0; i < cfg.threads; i++) {
    workers.emplace_back(osbench_worker, os.get(), std::ref(cfg));
  }

   while (_not_filled) {
     sleep(10);
    }
   sleep(cfg.time_secs);
   _test_done = true;

  for (auto &worker : workers)
    worker.join();
  workers.clear();

  os->umount();
  stats_thd1.join();

  dout(0) << " Finished test.\n" << dendl;;
  return 0;
}
