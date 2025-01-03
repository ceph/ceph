// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <chrono>
#include <cassert>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "os/ObjectStore.h"

#include "global/global_init.h"

#include "common/debug.h"
#include "common/strtol.h"
#include "common/ceph_argparse.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_filestore

using namespace std;

static void usage()
{
  cout << "usage: ceph_objectstore_bench [flags]\n"
      "	 --size\n"
      "	       total size in bytes\n"
      "	 --block-size\n"
      "	       block size in bytes for each write\n"
      "	 --repeats\n"
      "	       number of times to repeat the write cycle\n"
      "	 --threads\n"
      "	       number of threads to carry out this workload\n"
      "	 --multi-object\n"
    "	       have each thread write to a separate object\n" << std::endl;
  generic_server_usage();
}

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
  v = strict_iecstrtoll(val, err);
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
  int repeats;
  int threads;
  bool multi_object;
  Config()
    : size(1048576), block_size(4096),
      repeats(1), threads(1),
      multi_object(false) {}
};

class C_NotifyCond : public Context {
  std::mutex *mutex;
  std::condition_variable *cond;
  bool *done;
public:
  C_NotifyCond(std::mutex *mutex, std::condition_variable *cond, bool *done)
    : mutex(mutex), cond(cond), done(done) {}
  void finish(int r) override {
    std::lock_guard<std::mutex> lock(*mutex);
    *done = true;
    cond->notify_one();
  }
};

void osbench_worker(ObjectStore *os, const Config &cfg,
                    const coll_t cid, const ghobject_t oid,
                    uint64_t starting_offset)
{
  bufferlist data;
  data.append(buffer::create(cfg.block_size));

  dout(0) << "Writing " << cfg.size
      << " in blocks of " << cfg.block_size << dendl;

  ceph_assert(starting_offset < cfg.size);
  ceph_assert(starting_offset % cfg.block_size == 0);

  ObjectStore::CollectionHandle ch = os->open_collection(cid);
  ceph_assert(ch);

  for (int i = 0; i < cfg.repeats; ++i) {
    uint64_t offset = starting_offset;
    size_t len = cfg.size;

    vector<ObjectStore::Transaction> tls;

    std::cout << "Write cycle " << i << std::endl;
    while (len) {
      size_t count = len < cfg.block_size ? len : (size_t)cfg.block_size;

      auto t = new ObjectStore::Transaction;
      t->write(cid, oid, offset, count, data);
      tls.push_back(std::move(*t));
      delete t;

      offset += count;
      if (offset > cfg.size)
        offset -= cfg.size;
      len -= count;
    }

    // set up the finisher
    std::mutex mutex;
    std::condition_variable cond;
    bool done = false;

    tls.back().register_on_commit(new C_NotifyCond(&mutex, &cond, &done));
    os->queue_transactions(ch, tls);

    std::unique_lock<std::mutex> lock(mutex);
    cond.wait(lock, [&done](){ return done; });
    lock.unlock();
  }
}

int main(int argc, const char *argv[])
{
  // command-line arguments
  auto args = argv_to_vec(argc, argv);

  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  Config cfg;
  std::string val;
  vector<const char*>::iterator i = args.begin();
  while (i != args.end()) {
    if (ceph_argparse_double_dash(args, i))
      break;

    if (ceph_argparse_witharg(args, i, &val, "--size", (char*)nullptr)) {
      std::string err;
      if (!cfg.size.parse(val, &err)) {
        derr << "error parsing size: " << err << dendl;
        exit(1);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--block-size", (char*)nullptr)) {
      std::string err;
      if (!cfg.block_size.parse(val, &err)) {
        derr << "error parsing block-size: " << err << dendl;
        exit(1);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--repeats", (char*)nullptr)) {
      cfg.repeats = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--threads", (char*)nullptr)) {
      cfg.threads = atoi(val.c_str());
    } else if (ceph_argparse_flag(args, i, "--multi-object", (char*)nullptr)) {
      cfg.multi_object = true;
    } else {
      derr << "Error: can't understand argument: " << *i << "\n" << dendl;
      exit(1);
    }
  }

  common_init_finish(g_ceph_context);

  // create object store
  dout(0) << "objectstore " << g_conf()->osd_objectstore << dendl;
  dout(0) << "data " << g_conf()->osd_data << dendl;
  dout(0) << "journal " << g_conf()->osd_journal << dendl;
  dout(0) << "size " << cfg.size << dendl;
  dout(0) << "block-size " << cfg.block_size << dendl;
  dout(0) << "repeats " << cfg.repeats << dendl;
  dout(0) << "threads " << cfg.threads << dendl;

  auto os =
      ObjectStore::create(g_ceph_context,
                          g_conf()->osd_objectstore,
                          g_conf()->osd_data,
                          g_conf()->osd_journal);

  //Checking data folder: create if needed or error if it's not empty
  DIR *dir = ::opendir(g_conf()->osd_data.c_str());
  if (!dir) {
    std::string cmd("mkdir -p ");
    cmd+=g_conf()->osd_data;
    int r = ::system( cmd.c_str() );
    if( r<0 ){
      derr << "Failed to create data directory, ret = " << r << dendl;
      return 1;
    }
  }
  else {
     bool non_empty = readdir(dir) != NULL && readdir(dir) != NULL && readdir(dir) != NULL;
     if( non_empty ){
       derr << "Data directory '"<<g_conf()->osd_data<<"' isn't empty, please clean it first."<< dendl;
       return 1;
     }
  }
  ::closedir(dir);

  //Create folders for journal if needed
  string journal_base = g_conf()->osd_journal.substr(0, g_conf()->osd_journal.rfind('/'));
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
    derr << "bad objectstore type " << g_conf()->osd_objectstore << dendl;
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
  ObjectStore::CollectionHandle ch = os->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    os->queue_transaction(ch, std::move(t));
  }

  // create the objects
  std::vector<ghobject_t> oids;
  if (cfg.multi_object) {
    oids.reserve(cfg.threads);
    for (int i = 0; i < cfg.threads; i++) {
      std::stringstream oss;
      oss << "osbench-thread-" << i;
      oids.emplace_back(hobject_t(sobject_t(oss.str(), CEPH_NOSNAP)));

      ObjectStore::Transaction t;
      t.touch(cid, oids[i]);
      int r = os->queue_transaction(ch, std::move(t));
      ceph_assert(r == 0);
    }
  } else {
    oids.emplace_back(hobject_t(sobject_t("osbench", CEPH_NOSNAP)));

    ObjectStore::Transaction t;
    t.touch(cid, oids.back());
    int r = os->queue_transaction(ch, std::move(t));
    ceph_assert(r == 0);
  }

  // run the worker threads
  std::vector<std::thread> workers;
  workers.reserve(cfg.threads);

  using namespace std::chrono;
  auto t1 = high_resolution_clock::now();
  for (int i = 0; i < cfg.threads; i++) {
    const auto &oid = cfg.multi_object ? oids[i] : oids[0];
    workers.emplace_back(osbench_worker, os.get(), std::ref(cfg),
                         cid, oid, i * cfg.size / cfg.threads);
  }
  for (auto &worker : workers)
    worker.join();
  auto t2 = high_resolution_clock::now();
  workers.clear();

  auto duration = duration_cast<microseconds>(t2 - t1);
  byte_units total = cfg.size * cfg.repeats * cfg.threads;
  byte_units rate = (1000000LL * total) / duration.count();
  size_t iops = (1000000LL * total / cfg.block_size) / duration.count();
  dout(0) << "Wrote " << total << " in "
      << duration.count() << "us, at a rate of " << rate << "/s and "
      << iops << " iops" << dendl;

  // remove the objects
  ObjectStore::Transaction t;
  for (const auto &oid : oids)
    t.remove(cid, oid);
  os->queue_transaction(ch, std::move(t));

  os->umount();
  return 0;
}
