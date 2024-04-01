// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 *  Ceph ObjectStore engine
 *
 * IO engine using Ceph's ObjectStore class to test low-level performance of
 * Ceph OSDs.
 *
 */

#include <memory>
#include <system_error>
#include <vector>
#include <fstream>

#include "os/ObjectStore.h"
#include "global/global_init.h"
#include "common/errno.h"
#include "include/intarith.h"
#include "include/stringify.h"
#include "include/random.h"
#include "include/str_list.h"
#include "common/perf_counters.h"
#include "common/TracepointProvider.h"

#include <fio.h>
#include <optgroup.h>

#include "include/ceph_assert.h" // fio.h clobbers our assert.h
#include <algorithm>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_

using namespace std;

namespace {

/// fio configuration options read from the job file
struct Options {
  thread_data* td;
  char* conf;
  char* perf_output_file;
  char* throttle_values;
  char* deferred_throttle_values;
  unsigned long long
    cycle_throttle_period,
    oi_attr_len_low,
    oi_attr_len_high,
    snapset_attr_len_low,
    snapset_attr_len_high,
    pglog_omap_len_low,
    pglog_omap_len_high,
    pglog_dup_omap_len_low,
    pglog_dup_omap_len_high,
    _fastinfo_omap_len_low,
    _fastinfo_omap_len_high;
  unsigned simulate_pglog;
  unsigned single_pool_mode;
  unsigned preallocate_files;
  unsigned check_files;
};

template <class Func> // void Func(fio_option&)
fio_option make_option(Func&& func)
{
  // zero-initialize and set common defaults
  auto o = fio_option{};
  o.category = FIO_OPT_C_ENGINE;
  o.group    = FIO_OPT_G_RBD;
  func(std::ref(o));
  return o;
}

static std::vector<fio_option> ceph_options{
  make_option([] (fio_option& o) {
    o.name   = "conf";
    o.lname  = "ceph configuration file";
    o.type   = FIO_OPT_STR_STORE;
    o.help   = "Path to a ceph configuration file";
    o.off1   = offsetof(Options, conf);
  }),
  make_option([] (fio_option& o) {
    o.name   = "perf_output_file";
    o.lname  = "perf output target";
    o.type   = FIO_OPT_STR_STORE;
    o.help   = "Path to which to write json formatted perf output";
    o.off1   = offsetof(Options, perf_output_file);
    o.def    = 0;
  }),
  make_option([] (fio_option& o) {
    o.name   = "oi_attr_len";
    o.lname  = "OI Attr length";
    o.type   = FIO_OPT_STR_VAL;
    o.help   = "Set OI(aka '_') attribute to specified length";
    o.off1   = offsetof(Options, oi_attr_len_low);
    o.off2   = offsetof(Options, oi_attr_len_high);
    o.def    = 0;
    o.minval = 0;
  }),
  make_option([] (fio_option& o) {
    o.name   = "snapset_attr_len";
    o.lname  = "Attr 'snapset' length";
    o.type   = FIO_OPT_STR_VAL;
    o.help   = "Set 'snapset' attribute to specified length";
    o.off1   = offsetof(Options, snapset_attr_len_low);
    o.off2   = offsetof(Options, snapset_attr_len_high);
    o.def    = 0;
    o.minval = 0;
  }),
  make_option([] (fio_option& o) {
    o.name   = "_fastinfo_omap_len";
    o.lname  = "'_fastinfo' omap entry length";
    o.type   = FIO_OPT_STR_VAL;
    o.help   = "Set '_fastinfo' OMAP attribute to specified length";
    o.off1   = offsetof(Options, _fastinfo_omap_len_low);
    o.off2   = offsetof(Options, _fastinfo_omap_len_high);
    o.def    = 0;
    o.minval = 0;
  }),
  make_option([] (fio_option& o) {
    o.name   = "pglog_simulation";
    o.lname  = "pglog behavior simulation";
    o.type   = FIO_OPT_BOOL;
    o.help   = "Enables PG Log simulation behavior";
    o.off1   = offsetof(Options, simulate_pglog);
    o.def    = "0";
  }),
  make_option([] (fio_option& o) {
    o.name   = "pglog_omap_len";
    o.lname  = "pglog omap entry length";
    o.type   = FIO_OPT_STR_VAL;
    o.help   = "Set pglog omap entry to specified length";
    o.off1   = offsetof(Options, pglog_omap_len_low);
    o.off2   = offsetof(Options, pglog_omap_len_high);
    o.def    = 0;
    o.minval = 0;
  }),
  make_option([] (fio_option& o) {
    o.name   = "pglog_dup_omap_len";
    o.lname  = "uplicate pglog omap entry length";
    o.type   = FIO_OPT_STR_VAL;
    o.help   = "Set duplicate pglog omap entry to specified length";
    o.off1   = offsetof(Options, pglog_dup_omap_len_low);
    o.off2   = offsetof(Options, pglog_dup_omap_len_high);
    o.def    = 0;
    o.minval = 0;
  }),
  make_option([] (fio_option& o) {
    o.name   = "single_pool_mode";
    o.lname  = "single(shared among jobs) pool mode";
    o.type   = FIO_OPT_BOOL;
    o.help   = "Enables the mode when all jobs run against the same pool";
    o.off1   = offsetof(Options, single_pool_mode);
    o.def    = "0";
  }),
  make_option([] (fio_option& o) {
    o.name   = "preallocate_files";
    o.lname  = "preallocate files on init";
    o.type   = FIO_OPT_BOOL;
    o.help   = "Enables/disables file preallocation (touch and resize) on init";
    o.off1   = offsetof(Options, preallocate_files);
    o.def    = "1";
  }),
  make_option([] (fio_option& o) {
    o.name   = "check_files";
    o.lname  = "ensure files exist and are correct on init";
    o.type   = FIO_OPT_BOOL;
    o.help   = "Enables/disables checking of files on init";
    o.off1   = offsetof(Options, check_files);
    o.def    = "0";
  }),
  make_option([] (fio_option& o) {
    o.name   = "bluestore_throttle";
    o.lname  = "set bluestore throttle";
    o.type   = FIO_OPT_STR_STORE;
    o.help   = "comma delimited list of throttle values",
    o.off1   = offsetof(Options, throttle_values);
    o.def    = 0;
  }),
  make_option([] (fio_option& o) {
    o.name   = "bluestore_deferred_throttle";
    o.lname  = "set bluestore deferred throttle";
    o.type   = FIO_OPT_STR_STORE;
    o.help   = "comma delimited list of throttle values",
    o.off1   = offsetof(Options, deferred_throttle_values);
    o.def    = 0;
  }),
  make_option([] (fio_option& o) {
    o.name   = "vary_bluestore_throttle_period";
    o.lname = "period between different throttle values";
    o.type   = FIO_OPT_STR_VAL;
    o.help = "set to non-zero value to periodically cycle through throttle options";
    o.off1   = offsetof(Options, cycle_throttle_period);
    o.def    = "0";
    o.minval = 0;
  }),
  {} // fio expects a 'null'-terminated list
};


struct Collection {
  spg_t pg;
  coll_t cid;
  ObjectStore::CollectionHandle ch;
  // Can't use mutex directly in vectors hence dynamic allocation

  std::unique_ptr<std::mutex> lock;
  uint64_t pglog_ver_head = 1;
  uint64_t pglog_ver_tail = 1;
  uint64_t pglog_dup_ver_tail = 1;

  // use big pool ids to avoid clashing with existing collections
  static constexpr int64_t MIN_POOL_ID = 0x0000ffffffffffff;

  Collection(const spg_t& pg, ObjectStore::CollectionHandle _ch)
    : pg(pg), cid(pg), ch(_ch),
        lock(new std::mutex) {
  }
};

int destroy_collections(
  std::unique_ptr<ObjectStore>& os,
  std::vector<Collection>& collections)
{
  ObjectStore::Transaction t;
  bool failed = false;
  // remove our collections
  for (auto& coll : collections) {
    ghobject_t pgmeta_oid(coll.pg.make_pgmeta_oid());
    t.remove(coll.cid, pgmeta_oid);
    t.remove_collection(coll.cid);
    int r = os->queue_transaction(coll.ch, std::move(t));
    if (r && !failed) {
      derr << "Engine cleanup failed with " << cpp_strerror(-r) << dendl;
      failed = true;
    }
  }
  return 0;
}

int init_collections(std::unique_ptr<ObjectStore>& os,
		      uint64_t pool,
		      std::vector<Collection>& collections,
		      uint64_t count)
{
  ceph_assert(count > 0);
  collections.reserve(count);

  const int split_bits = cbits(count - 1);

  {
    // propagate Superblock object to ensure proper functioning of tools that
    // need it. E.g. ceph-objectstore-tool
    coll_t cid(coll_t::meta());
    bool exists = os->collection_exists(cid);
    if (!exists) {
      auto ch = os->create_new_collection(cid);

      OSDSuperblock superblock;
      bufferlist bl;
      encode(superblock, bl);

      ObjectStore::Transaction t;
      t.create_collection(cid, split_bits);
      t.write(cid, OSD_SUPERBLOCK_GOBJECT, 0, bl.length(), bl);
      int r = os->queue_transaction(ch, std::move(t));

      if (r < 0) {
	derr << "Failure to write OSD superblock: " << cpp_strerror(-r) << dendl;
	return r;
      }
    }
  }

  for (uint32_t i = 0; i < count; i++) {
    auto pg = spg_t{pg_t{i, pool}};
    coll_t cid(pg);

    bool exists = os->collection_exists(cid);
    auto ch = exists ?
      os->open_collection(cid) :
      os->create_new_collection(cid) ;

    collections.emplace_back(pg, ch);

    ObjectStore::Transaction t;
    auto& coll = collections.back();
    if (!exists) {
      t.create_collection(coll.cid, split_bits);
      ghobject_t pgmeta_oid(coll.pg.make_pgmeta_oid());
      t.touch(coll.cid, pgmeta_oid);
      int r = os->queue_transaction(coll.ch, std::move(t));
      if (r) {
	derr << "Engine init failed with " << cpp_strerror(-r) << dendl;
	destroy_collections(os, collections);
	return r;
      }
    }
  }
  return 0;
}

/// global engine state shared between all jobs within the process. this
/// includes g_ceph_context and the ObjectStore instance
struct Engine {
  /// the initial g_ceph_context reference to be dropped on destruction
  boost::intrusive_ptr<CephContext> cct;
  std::unique_ptr<ObjectStore> os;

  std::vector<Collection> collections; //< shared collections to spread objects over

  std::mutex lock;
  int ref_count;
  const bool unlink; //< unlink objects on destruction

  // file to which to output formatted perf information
  const std::optional<std::string> perf_output_file;

  explicit Engine(thread_data* td);
  ~Engine();

  static Engine* get_instance(thread_data* td) {
    // note: creates an Engine with the options associated with the first job
    static Engine engine(td);
    return &engine;
  }

  void ref() {
    std::lock_guard<std::mutex> l(lock);
    ++ref_count;
  }
  void deref() {
    std::lock_guard<std::mutex> l(lock);
    --ref_count;
    if (!ref_count) {
      ostringstream ostr;
      Formatter* f = Formatter::create(
	"json-pretty", "json-pretty", "json-pretty");
      f->open_object_section("perf_output");
      cct->get_perfcounters_collection()->dump_formatted(f, false, false);
      if (g_conf()->rocksdb_perf) {
	f->open_object_section("rocksdb_perf");
        os->get_db_statistics(f);
	f->close_section();
      }
      mempool::dump(f);
      {
	f->open_object_section("db_histogram");
	os->generate_db_histogram(f);
	f->close_section();
      }
      f->close_section();
      
      f->flush(ostr);
      delete f;

      if (unlink) {
	destroy_collections(os, collections);
      }
      os->umount();
      dout(0) << "FIO plugin perf dump:" << dendl;
      dout(0) << ostr.str() << dendl;
      if (perf_output_file) {
	try {
	  std::ofstream foutput(*perf_output_file);
	  foutput << ostr.str() << std::endl;
	} catch (std::exception &e) {
	  std::cerr << "Unable to write formatted output to "
		    << *perf_output_file
		    << ", exception: " << e.what()
		    << std::endl;
	}
      }
    }
  }
};

TracepointProvider::Traits bluestore_tracepoint_traits("libbluestore_tp.so",
						       "bluestore_tracing");

Engine::Engine(thread_data* td)
  : ref_count(0),
    unlink(td->o.unlink),
    perf_output_file(
      static_cast<Options*>(td->eo)->perf_output_file ?
      std::make_optional(static_cast<Options*>(td->eo)->perf_output_file) :
      std::nullopt)
{
  // add the ceph command line arguments
  auto o = static_cast<Options*>(td->eo);
  if (!o->conf) {
    throw std::runtime_error("missing conf option for ceph configuration file");
  }
  std::vector<const char*> args{
    "-i", "0", // identify as osd.0 for osd_data and osd_journal
    "--conf", o->conf, // use the requested conf file
  };
  if (td->o.directory) { // allow conf files to use ${fio_dir} for data
    args.emplace_back("--fio_dir");
    args.emplace_back(td->o.directory);
  }

  // claim the g_ceph_context reference and release it on destruction
  cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
		    CODE_ENVIRONMENT_UTILITY,
		    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  TracepointProvider::initialize<bluestore_tracepoint_traits>(g_ceph_context);

  // create the ObjectStore
  os = ObjectStore::create(g_ceph_context,
			   g_conf().get_val<std::string>("osd objectstore"),
			   g_conf().get_val<std::string>("osd data"),
			   g_conf().get_val<std::string>("osd journal"));
  if (!os)
    throw std::runtime_error("bad objectstore type " + g_conf()->osd_objectstore);

  unsigned num_shards;
  if(g_conf()->osd_op_num_shards)
    num_shards = g_conf()->osd_op_num_shards;
  else if(os->is_rotational())
    num_shards = g_conf()->osd_op_num_shards_hdd;
  else
    num_shards = g_conf()->osd_op_num_shards_ssd;
  os->set_cache_shards(num_shards);

  //normalize options
  o->oi_attr_len_high = max(o->oi_attr_len_low, o->oi_attr_len_high);
  o->snapset_attr_len_high = max(o->snapset_attr_len_low,
				      o->snapset_attr_len_high);
  o->pglog_omap_len_high = max(o->pglog_omap_len_low,
				    o->pglog_omap_len_high);
  o->pglog_dup_omap_len_high = max(o->pglog_dup_omap_len_low,
					o->pglog_dup_omap_len_high);
  o->_fastinfo_omap_len_high = max(o->_fastinfo_omap_len_low,
					o->_fastinfo_omap_len_high);

  int r = os->mkfs();
  if (r < 0)
    throw std::system_error(-r, std::system_category(), "mkfs failed");

  r = os->mount();
  if (r < 0)
    throw std::system_error(-r, std::system_category(), "mount failed");

  // create shared collections up to osd_pool_default_pg_num
  if (o->single_pool_mode) {
    uint64_t count = g_conf().get_val<uint64_t>("osd_pool_default_pg_num");
    if (count > td->o.nr_files)
      count = td->o.nr_files;
    init_collections(os, Collection::MIN_POOL_ID, collections, count);
  }
}

Engine::~Engine()
{
  ceph_assert(!ref_count);
}

struct Object {
  ghobject_t oid;
  Collection& coll;

  Object(const char* name, Collection& coll)
    : oid(hobject_t(name, "", CEPH_NOSNAP, coll.pg.ps(), coll.pg.pool(), "")),
      coll(coll) {}
};

/// treat each fio job either like a separate pool with its own collections and objects
/// or just a client using its own objects from the shared pool
struct Job {
  Engine* engine; //< shared ptr to the global Engine
  const unsigned subjob_number; //< subjob num
  std::vector<Collection> collections; //< job's private collections to spread objects over
  std::vector<Object> objects; //< associate an object with each fio_file
  std::vector<io_u*> events; //< completions for fio_ceph_os_event()
  const bool unlink; //< unlink objects on destruction

  bufferptr one_for_all_data; //< preallocated buffer long enough
                              //< to use for vairious operations
  std::mutex throttle_lock;
  const vector<unsigned> throttle_values;
  const vector<unsigned> deferred_throttle_values;
  std::chrono::duration<double> cycle_throttle_period;
  mono_clock::time_point last = ceph::mono_clock::zero();
  unsigned index = 0;

  static vector<unsigned> parse_throttle_str(const char *p) {
    vector<unsigned> ret;
    if (p == nullptr) {
      return ret;
    }
    ceph::for_each_substr(p, ",\"", [&ret] (auto &&s) mutable {
      if (s.size() > 0) {
	ret.push_back(std::stoul(std::string(s)));
      }
    });
    return ret;
  }
  void check_throttle();

  Job(Engine* engine, const thread_data* td);
  ~Job();
};

Job::Job(Engine* engine, const thread_data* td)
  : engine(engine),
    subjob_number(td->subjob_number),
    events(td->o.iodepth),
    unlink(td->o.unlink),
    throttle_values(
      parse_throttle_str(static_cast<Options*>(td->eo)->throttle_values)),
    deferred_throttle_values(
      parse_throttle_str(static_cast<Options*>(td->eo)->deferred_throttle_values)),
    cycle_throttle_period(
      static_cast<Options*>(td->eo)->cycle_throttle_period)
{
  engine->ref();
  auto o = static_cast<Options*>(td->eo);
  unsigned long long max_data = max(o->oi_attr_len_high,
				  o->snapset_attr_len_high);
  max_data = max(max_data, o->pglog_omap_len_high);
  max_data = max(max_data, o->pglog_dup_omap_len_high);
  max_data = max(max_data, o->_fastinfo_omap_len_high);
  one_for_all_data = buffer::create(max_data);

  std::vector<Collection>* colls;
  // create private collections up to osd_pool_default_pg_num
  if (!o->single_pool_mode) {
    uint64_t count = g_conf().get_val<uint64_t>("osd_pool_default_pg_num");
    if (count > td->o.nr_files)
      count = td->o.nr_files;
    // use the fio thread_number for our unique pool id
    const uint64_t pool = Collection::MIN_POOL_ID + td->thread_number + 1;
    init_collections(engine->os, pool, collections, count);
    colls = &collections;
  } else {
    colls = &engine->collections;
  }
  const uint64_t file_size = td->o.size / max(1u, td->o.nr_files);
  ObjectStore::Transaction t;

  // create an object for each file in the job
  objects.reserve(td->o.nr_files);
  unsigned checked_or_preallocated = 0;
  for (uint32_t i = 0; i < td->o.nr_files; i++) {
    auto f = td->files[i];
    f->real_file_size = file_size;
    f->engine_pos = i;

    // associate each object with a collection in a round-robin fashion.
    auto& coll = (*colls)[i % colls->size()];

    objects.emplace_back(f->file_name, coll);
    if (o->preallocate_files) {
      auto& oid = objects.back().oid;
      t.touch(coll.cid, oid);
      t.truncate(coll.cid, oid, file_size);
      int r = engine->os->queue_transaction(coll.ch, std::move(t));
      if (r) {
        engine->deref();
        throw std::system_error(r, std::system_category(), "job init");
      }
    }
    if (o->check_files) {
      auto& oid = objects.back().oid;
      struct stat st;
      int r = engine->os->stat(coll.ch, oid, &st);
      if (r || ((unsigned)st.st_size) != file_size) {
	derr << "Problem checking " << oid << ", r=" << r
	     << ", st.st_size=" << st.st_size
	     << ", file_size=" << file_size
	     << ", nr_files=" << td->o.nr_files << dendl;
        engine->deref();
        throw std::system_error(
	  r, std::system_category(), "job init -- cannot check file");
      }
    }
    if (o->check_files || o->preallocate_files) {
      ++checked_or_preallocated;
    }
  }
  if (o->check_files) {
    derr << "fio_ceph_objectstore checked " << checked_or_preallocated
	 << " files"<< dendl;
  }
  if (o->preallocate_files ){
    derr << "fio_ceph_objectstore preallocated " << checked_or_preallocated
	 << " files"<< dendl;
  }
}

Job::~Job()
{
  if (unlink) {
    ObjectStore::Transaction t;
    bool failed = false;
    // remove our objects
    for (auto& obj : objects) {
      t.remove(obj.coll.cid, obj.oid);
      int r = engine->os->queue_transaction(obj.coll.ch, std::move(t));
      if (r && !failed) {
	derr << "job cleanup failed with " << cpp_strerror(-r) << dendl;
	failed = true;
      }
    }
    destroy_collections(engine->os, collections);
  }
  engine->deref();
}

void Job::check_throttle()
{
  if (subjob_number != 0)
    return;

  std::lock_guard<std::mutex> l(throttle_lock);
  if (throttle_values.empty() && deferred_throttle_values.empty())
    return;

  if (ceph::mono_clock::is_zero(last) ||
      ((cycle_throttle_period != cycle_throttle_period.zero()) &&
       (ceph::mono_clock::now() - last) > cycle_throttle_period)) {
    unsigned tvals = throttle_values.size() ? throttle_values.size() : 1;
    unsigned dtvals = deferred_throttle_values.size() ? deferred_throttle_values.size() : 1;
    if (!throttle_values.empty()) {
      std::string val = std::to_string(throttle_values[index % tvals]);
      std::cerr << "Setting bluestore_throttle_bytes to " << val << std::endl;
      int r = engine->cct->_conf.set_val(
	"bluestore_throttle_bytes",
	val,
	nullptr);
      ceph_assert(r == 0);
    }
    if (!deferred_throttle_values.empty()) {
      std::string val = std::to_string(deferred_throttle_values[(index / tvals) % dtvals]);
      std::cerr << "Setting bluestore_deferred_throttle_bytes to " << val << std::endl;
      int r = engine->cct->_conf.set_val(
	"bluestore_throttle_deferred_bytes",
	val,
	nullptr);
      ceph_assert(r == 0);
    }
    engine->cct->_conf.apply_changes(nullptr);
    index++;
    index %= tvals * dtvals;
    last = ceph::mono_clock::now();
  }
}

int fio_ceph_os_setup(thread_data* td)
{
  // if there are multiple jobs, they must run in the same process against a
  // single instance of the ObjectStore. explicitly disable fio's default
  // job-per-process configuration
  td->o.use_thread = 1;

  try {
    // get or create the global Engine instance
    auto engine = Engine::get_instance(td);
    // create a Job for this thread
    td->io_ops_data = new Job(engine, td);
  } catch (std::exception& e) {
    std::cerr << "setup failed with " << e.what() << std::endl;
    return -1;
  }
  return 0;
}

void fio_ceph_os_cleanup(thread_data* td)
{
  auto job = static_cast<Job*>(td->io_ops_data);
  td->io_ops_data = nullptr;
  delete job;
}


io_u* fio_ceph_os_event(thread_data* td, int event)
{
  // return the requested event from fio_ceph_os_getevents()
  auto job = static_cast<Job*>(td->io_ops_data);
  return job->events[event];
}

int fio_ceph_os_getevents(thread_data* td, unsigned int min,
                          unsigned int max, const timespec* t)
{
  auto job = static_cast<Job*>(td->io_ops_data);
  unsigned int events = 0;
  io_u* u = NULL;
  unsigned int i = 0;

  // loop through inflight ios until we find 'min' completions
  do {
    io_u_qiter(&td->io_u_all, u, i) {
      if (!(u->flags & IO_U_F_FLIGHT))
        continue;

      if (u->engine_data) {
        u->engine_data = nullptr;
        job->events[events] = u;
        events++;
      }
    }
    if (events >= min)
      break;
    usleep(100);
  } while (1);

  return events;
}

/// completion context for ObjectStore::queue_transaction()
class UnitComplete : public Context {
  io_u* u;
 public:
  explicit UnitComplete(io_u* u) : u(u) {}
  void finish(int r) {
    // mark the pointer to indicate completion for fio_ceph_os_getevents()
    u->engine_data = reinterpret_cast<void*>(1ull);
  }
};

enum fio_q_status fio_ceph_os_queue(thread_data* td, io_u* u)
{
  fio_ro_check(td, u);



  auto o = static_cast<const Options*>(td->eo);
  auto job = static_cast<Job*>(td->io_ops_data);
  auto& object = job->objects[u->file->engine_pos];
  auto& coll = object.coll;
  auto& os = job->engine->os;

  job->check_throttle();

  if (u->ddir == DDIR_WRITE) {
    // provide a hint if we're likely to read this data back
    const int flags = td_rw(td) ? CEPH_OSD_OP_FLAG_FADVISE_WILLNEED : 0;

    bufferlist bl;
    bl.push_back(buffer::copy(reinterpret_cast<char*>(u->xfer_buf),
                              u->xfer_buflen ) );

    map<string,bufferptr,less<>> attrset;
    map<string, bufferlist> omaps;
    // enqueue a write transaction on the collection's handle
    ObjectStore::Transaction t;
    char ver_key[64];

    // fill attrs if any
    if (o->oi_attr_len_high) {
      ceph_assert(o->oi_attr_len_high >= o->oi_attr_len_low);
      // fill with the garbage as we do not care of the actual content...
      job->one_for_all_data.set_length(
        ceph::util::generate_random_number(
	  o->oi_attr_len_low, o->oi_attr_len_high));
      attrset["_"] = job->one_for_all_data;
    }
    if (o->snapset_attr_len_high) {
      ceph_assert(o->snapset_attr_len_high >= o->snapset_attr_len_low);
      job->one_for_all_data.set_length(
        ceph::util::generate_random_number
	  (o->snapset_attr_len_low, o->snapset_attr_len_high));
      attrset["snapset"] = job->one_for_all_data;

    }
    if (o->_fastinfo_omap_len_high) {
      ceph_assert(o->_fastinfo_omap_len_high >= o->_fastinfo_omap_len_low);
      // fill with the garbage as we do not care of the actual content...
      job->one_for_all_data.set_length(
	ceph::util::generate_random_number(
	  o->_fastinfo_omap_len_low, o->_fastinfo_omap_len_high));
      omaps["_fastinfo"].append(job->one_for_all_data);
    }

    uint64_t pglog_trim_head = 0, pglog_trim_tail = 0;
    uint64_t pglog_dup_trim_head = 0, pglog_dup_trim_tail = 0;
    if (o->simulate_pglog) {

      uint64_t pglog_ver_cnt = 0;
      {
	std::lock_guard<std::mutex> l(*coll.lock);
        pglog_ver_cnt = coll.pglog_ver_head++;
	if (o->pglog_omap_len_high &&
	    pglog_ver_cnt >=
	      coll.pglog_ver_tail +
	        g_conf()->osd_min_pg_log_entries + g_conf()->osd_pg_log_trim_min) {
	  pglog_trim_tail = coll.pglog_ver_tail;
	  coll.pglog_ver_tail = pglog_trim_head =
	    pglog_trim_tail + g_conf()->osd_pg_log_trim_min;

	  if (o->pglog_dup_omap_len_high &&
	      pglog_ver_cnt >=
		coll.pglog_dup_ver_tail + g_conf()->osd_pg_log_dups_tracked +
		  g_conf()->osd_pg_log_trim_min) {
	    pglog_dup_trim_tail = coll.pglog_dup_ver_tail;
	    coll.pglog_dup_ver_tail = pglog_dup_trim_head =
	      pglog_dup_trim_tail + g_conf()->osd_pg_log_trim_min;
	  }
	}
      }

      if (o->pglog_omap_len_high) {
	ceph_assert(o->pglog_omap_len_high >= o->pglog_omap_len_low);
	snprintf(ver_key, sizeof(ver_key),
	  "0000000011.%020llu", (unsigned long long)pglog_ver_cnt);
	// fill with the garbage as we do not care of the actual content...
        job->one_for_all_data.set_length(
	  ceph::util::generate_random_number(
	    o->pglog_omap_len_low, o->pglog_omap_len_high));
	omaps[ver_key].append(job->one_for_all_data);
      }
      if (o->pglog_dup_omap_len_high) {
	//insert dup
	ceph_assert(o->pglog_dup_omap_len_high >= o->pglog_dup_omap_len_low);
        for( auto i = pglog_trim_tail; i < pglog_trim_head; ++i) {
	  snprintf(ver_key, sizeof(ver_key),
	    "dup_0000000011.%020llu", (unsigned long long)i);
	  // fill with the garbage as we do not care of the actual content...
	  job->one_for_all_data.set_length(
	    ceph::util::generate_random_number(
	      o->pglog_dup_omap_len_low, o->pglog_dup_omap_len_high));
	  omaps[ver_key].append(job->one_for_all_data);
	}
      }
    }

    if (!attrset.empty()) {
      t.setattrs(coll.cid, object.oid, attrset);
    }
    t.write(coll.cid, object.oid, u->offset, u->xfer_buflen, bl, flags);

    set<string> rmkeys;
    for( auto i = pglog_trim_tail; i < pglog_trim_head; ++i) {
	snprintf(ver_key, sizeof(ver_key),
	  "0000000011.%020llu", (unsigned long long)i);
	rmkeys.emplace(ver_key);
    }
    for( auto i = pglog_dup_trim_tail; i < pglog_dup_trim_head; ++i) {
	snprintf(ver_key, sizeof(ver_key),
	  "dup_0000000011.%020llu", (unsigned long long)i);
	rmkeys.emplace(ver_key);
    }

    if (rmkeys.size()) {
      ghobject_t pgmeta_oid(coll.pg.make_pgmeta_oid());
      t.omap_rmkeys(coll.cid, pgmeta_oid, rmkeys);
    }

    if (omaps.size()) {
      ghobject_t pgmeta_oid(coll.pg.make_pgmeta_oid());
      t.omap_setkeys(coll.cid, pgmeta_oid, omaps);
    }
    t.register_on_commit(new UnitComplete(u));
    os->queue_transaction(coll.ch,
                          std::move(t));
    return FIO_Q_QUEUED;
  }

  if (u->ddir == DDIR_READ) {
    // ObjectStore reads are synchronous, so make the call and return COMPLETED
    bufferlist bl;
    int r = os->read(coll.ch, object.oid, u->offset, u->xfer_buflen, bl);
    if (r < 0) {
      u->error = r;
      td_verror(td, u->error, "xfer");
    } else {
      bl.begin().copy(bl.length(), static_cast<char*>(u->xfer_buf));
      u->resid = u->xfer_buflen - r;
    }
    return FIO_Q_COMPLETED;
  }

  derr << "WARNING: Only DDIR_READ and DDIR_WRITE are supported!" << dendl;
  u->error = -EINVAL;
  td_verror(td, u->error, "xfer");
  return FIO_Q_COMPLETED;
}

int fio_ceph_os_commit(thread_data* td)
{
  // commit() allows the engine to batch up queued requests to be submitted all
  // at once. it would be natural for queue() to collect transactions in a list,
  // and use commit() to pass them all to ObjectStore::queue_transactions(). but
  // because we spread objects over multiple collections, we a) need to use a
  // different sequencer for each collection, and b) are less likely to see a
  // benefit from batching requests within a collection
  return 0;
}

// open/close are noops. we set the FIO_DISKLESSIO flag in ioengine_ops to
// prevent fio from creating the files
int fio_ceph_os_open(thread_data* td, fio_file* f) { return 0; }
int fio_ceph_os_close(thread_data* td, fio_file* f) { return 0; }

int fio_ceph_os_io_u_init(thread_data* td, io_u* u)
{
  // no data is allocated, we just use the pointer as a boolean 'completed' flag
  u->engine_data = nullptr;
  return 0;
}

void fio_ceph_os_io_u_free(thread_data* td, io_u* u)
{
  u->engine_data = nullptr;
}


// ioengine_ops for get_ioengine()
struct ceph_ioengine : public ioengine_ops {
  ceph_ioengine() : ioengine_ops({}) {
    name        = "ceph-os";
    version     = FIO_IOOPS_VERSION;
    flags       = FIO_DISKLESSIO;
    setup       = fio_ceph_os_setup;
    queue       = fio_ceph_os_queue;
    commit      = fio_ceph_os_commit;
    getevents   = fio_ceph_os_getevents;
    event       = fio_ceph_os_event;
    cleanup     = fio_ceph_os_cleanup;
    open_file   = fio_ceph_os_open;
    close_file  = fio_ceph_os_close;
    io_u_init   = fio_ceph_os_io_u_init;
    io_u_free   = fio_ceph_os_io_u_free;
    options     = ceph_options.data();
    option_struct_size = sizeof(struct Options);
  }
};

} // anonymous namespace

extern "C" {
// the exported fio engine interface
void get_ioengine(struct ioengine_ops** ioengine_ptr) {
  static ceph_ioengine ioengine;
  *ioengine_ptr = &ioengine;
}
} // extern "C"
