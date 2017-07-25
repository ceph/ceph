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

#include "os/ObjectStore.h"
#include "global/global_init.h"
#include "common/errno.h"
#include "include/intarith.h"
#include "include/stringify.h"
#include "common/perf_counters.h"

#include <fio.h>
#include <optgroup.h>

#include "include/assert.h" // fio.h clobbers our assert.h

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_

namespace {

/// fio configuration options read from the job file
struct Options {
  thread_data* td;
  char* conf;
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
  {} // fio expects a 'null'-terminated list
};


/// global engine state shared between all jobs within the process. this
/// includes g_ceph_context and the ObjectStore instance
struct Engine {
  /// the initial g_ceph_context reference to be dropped on destruction
  boost::intrusive_ptr<CephContext> cct;
  std::unique_ptr<ObjectStore> os;

  std::mutex lock;
  int ref_count;

  Engine(const thread_data* td);
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
      Formatter* f = Formatter::create("json-pretty", "json-pretty", "json-pretty");
      cct->get_perfcounters_collection()->dump_formatted(f, false);
      ostr << "FIO plugin ";
      f->flush(ostr);
      if (g_conf->rocksdb_perf) {
        os->get_db_statistics(f);
        ostr << "FIO get_db_statistics ";
        f->flush(ostr);
      }
      delete f;
      os->umount();
      dout(0) <<  ostr.str() << dendl;
    }
  }
};

Engine::Engine(const thread_data* td) : ref_count(0)
{
  // add the ceph command line arguments
  auto o = static_cast<const Options*>(td->eo);
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
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // create the ObjectStore
  os.reset(ObjectStore::create(g_ceph_context,
                               g_conf->osd_objectstore,
                               g_conf->osd_data,
                               g_conf->osd_journal));
  if (!os)
    throw std::runtime_error("bad objectstore type " + g_conf->osd_objectstore);

  unsigned num_shards;
  if(g_conf->osd_op_num_shards)
    num_shards = g_conf->osd_op_num_shards;
  else if(os->is_rotational())
    num_shards = g_conf->osd_op_num_shards_hdd;
  else
    num_shards = g_conf->osd_op_num_shards_ssd;
  os->set_cache_shards(num_shards);

  int r = os->mkfs();
  if (r < 0)
    throw std::system_error(-r, std::system_category(), "mkfs failed");

  r = os->mount();
  if (r < 0)
    throw std::system_error(-r, std::system_category(), "mount failed");
}

Engine::~Engine()
{
  assert(!ref_count);
}


struct Collection {
  spg_t pg;
  coll_t cid;
  ObjectStore::Sequencer sequencer;

  // use big pool ids to avoid clashing with existing collections
  static constexpr int64_t MIN_POOL_ID = 0x0000ffffffffffff;

  Collection(const spg_t& pg)
    : pg(pg), cid(pg), sequencer(stringify(pg)) {
    sequencer.shard_hint = pg;
  }
};

struct Object {
  ghobject_t oid;
  Collection& coll;

  Object(const char* name, Collection& coll)
    : oid(hobject_t(name, "", CEPH_NOSNAP, coll.pg.ps(), coll.pg.pool(), "")),
      coll(coll) {}
};

/// treat each fio job like a separate pool with its own collections and objects
struct Job {
  Engine* engine; //< shared ptr to the global Engine
  std::vector<Collection> collections; //< spread objects over collections
  std::vector<Object> objects; //< associate an object with each fio_file
  std::vector<io_u*> events; //< completions for fio_ceph_os_event()
  const bool unlink; //< unlink objects on destruction

  Job(Engine* engine, const thread_data* td);
  ~Job();
};

Job::Job(Engine* engine, const thread_data* td)
  : engine(engine),
    events(td->o.iodepth),
    unlink(td->o.unlink)
{
  engine->ref();
  // use the fio thread_number for our unique pool id
  const uint64_t pool = Collection::MIN_POOL_ID + td->thread_number;

  // create a collection for each object, up to osd_pool_default_pg_num
  uint32_t count = g_conf->osd_pool_default_pg_num;
  if (count > td->o.nr_files)
    count = td->o.nr_files;

  assert(count > 0);
  collections.reserve(count);

  const int split_bits = cbits(count - 1);

  ObjectStore::Transaction t;
  for (uint32_t i = 0; i < count; i++) {
    auto pg = spg_t{pg_t{i, pool}};
    collections.emplace_back(pg);

    auto& cid = collections.back().cid;
    if (!engine->os->collection_exists(cid))
      t.create_collection(cid, split_bits);
  }

  const uint64_t file_size = td->o.size / max(1u, td->o.nr_files);

  // create an object for each file in the job
  for (uint32_t i = 0; i < td->o.nr_files; i++) {
    auto f = td->files[i];
    f->real_file_size = file_size;
    f->engine_pos = i;

    // associate each object with a collection in a round-robin fashion
    auto& coll = collections[i % collections.size()];

    objects.emplace_back(f->file_name, coll);
    auto& oid = objects.back().oid;

    t.touch(coll.cid, oid);
    t.truncate(coll.cid, oid, file_size);
  }

  // apply the entire transaction synchronously
  ObjectStore::Sequencer sequencer("job init");
  int r = engine->os->apply_transaction(&sequencer, std::move(t));
  if (r) {
   engine->deref();
    throw std::system_error(r, std::system_category(), "job init");
  }
}

Job::~Job()
{
  if (unlink) {
    ObjectStore::Transaction t;
    // remove our objects
    for (auto& obj : objects) {
      t.remove(obj.coll.cid, obj.oid);
    }
    // remove our collections
    for (auto& coll : collections) {
      t.remove_collection(coll.cid);
    }
    ObjectStore::Sequencer sequencer("job cleanup");
    int r = engine->os->apply_transaction(&sequencer, std::move(t));
    if (r)
      derr << "job cleanup failed with " << cpp_strerror(-r) << dendl;
  }
  engine->deref();
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
  io_u* u;
  unsigned int i;

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
  UnitComplete(io_u* u) : u(u) {}
  void finish(int r) {
    // mark the pointer to indicate completion for fio_ceph_os_getevents()
    u->engine_data = reinterpret_cast<void*>(1ull);
  }
};

int fio_ceph_os_queue(thread_data* td, io_u* u)
{
  fio_ro_check(td, u);

  auto job = static_cast<Job*>(td->io_ops_data);
  auto& object = job->objects[u->file->engine_pos];
  auto& coll = object.coll;
  auto& os = job->engine->os;

  if (u->ddir == DDIR_WRITE) {
    // provide a hint if we're likely to read this data back
    const int flags = td_rw(td) ? CEPH_OSD_OP_FLAG_FADVISE_WILLNEED : 0;

    bufferlist bl;
    bl.push_back(buffer::copy(reinterpret_cast<char*>(u->xfer_buf),
                              u->xfer_buflen ) );

    // enqueue a write transaction on the collection's sequencer
    ObjectStore::Transaction t;
    t.write(coll.cid, object.oid, u->offset, u->xfer_buflen, bl, flags);
    os->queue_transaction(&coll.sequencer,
                          std::move(t),
                          nullptr,
                          new UnitComplete(u));
    return FIO_Q_QUEUED;
  }

  if (u->ddir == DDIR_READ) {
    // ObjectStore reads are synchronous, so make the call and return COMPLETED
    bufferlist bl;
    int r = os->read(coll.cid, object.oid, u->offset, u->xfer_buflen, bl);
    if (r < 0) {
      u->error = r;
      td_verror(td, u->error, "xfer");
    } else {
      bl.copy(0, bl.length(), static_cast<char*>(u->xfer_buf));
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
