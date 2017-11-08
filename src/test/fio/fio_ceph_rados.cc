// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 *  Ceph Rados engine
 *
 * IO engine using Ceph's RADOS interface to test low-level performance of
 * Ceph OSDs.
 *
 */

#include <memory>
#include <system_error>
#include <vector>
#include <mutex>
#include <boost/intrusive_ptr.hpp>

#include "global/global_init.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/errno.h"
#include "common/debug.h"

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
  char* pool;
  bool stats_print;
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
    o.name   = "pool";
    o.lname  = "pool name to use";
    o.type   = FIO_OPT_STR_STORE;
    o.help   = "Ceph pool name to benchmark against";
    o.off1   = offsetof(Options, pool);
  }),
  make_option([] (fio_option& o) {
    o.name   = "stats_print";
    o.lname  = "whether to print Ceph stats";
    o.type   = FIO_OPT_BOOL;
    o.help   = "Determines if Ceph performance counters are printed on completion";
    o.off1   = offsetof(Options, stats_print);
  }),
  {} // fio expects a 'null'-terminated list
};


/// global engine state shared between all jobs within the process
struct Engine {
  /// the initial g_ceph_context reference to be dropped on destruction
  boost::intrusive_ptr<CephContext> cct;

  std::mutex lock;
  int ref_count;
  bool print_stats = false;

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
    if (!ref_count && print_stats) {
      ostringstream ostr;
      Formatter* f = Formatter::create("json-pretty", "json-pretty", "json-pretty");
      cct->get_perfcounters_collection()->dump_formatted(f, false);
      ostr << "FIO plugin ";
      f->flush(ostr);
      delete f;
      dout(0) <<  ostr.str() << dendl;
    }
  }
};

Engine::Engine(const thread_data* td) : ref_count(0)
{
  // add the ceph command line arguments
  auto o = static_cast<const Options*>(td->eo);
  if (!o->conf) {
    throw std::runtime_error("missing --conf option for ceph configuration file");
  }
  if (!o->pool) {
    throw std::runtime_error("missing --pool option for ceph configuration file");
  }
  print_stats = o->stats_print;
  std::vector<const char*> args{
    "--conf", o->conf, // use the requested conf file
  };

  // claim the g_ceph_context reference and release it on destruction
  cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
}

Engine::~Engine()
{
  assert(!ref_count);
}

class UnitComplete;
/// treat each fio job like a separate pool with its own collections and objects
struct Job {
  Engine* engine; //< ptr to the global Engine

  std::vector<std::string> objects; //< track create objects
  std::vector<io_u*> events; //< completions for fio_ceph_rados_event()
  const bool unlink; //< unlink objects on destruction

  librados::Rados rados;
  librados::IoCtx io_ctx;
//
  UnitComplete* rados_op_started(io_u* u);

  Job(Engine* engine, const thread_data* td);
  ~Job();
};

/// completion context for rados ops
class UnitComplete {
  io_u* u = nullptr;
  librados::AioCompletion* rados_completion = nullptr;
  Job* job = nullptr;
  bufferlist bl;

 public:
  UnitComplete() {}

  void init(Job* j, librados::AioCompletion* c) {
    job = j;
    rados_completion = c;
  }
  void start(io_u* _u) {
    assert(!u);
    u = _u;
  }
  librados::AioCompletion* get_rados_completion() { return rados_completion; }
  bufferlist* get_target_buffer() { return &bl; }

  static void on_complete_cb(void*, void* arg) {
    static_cast<UnitComplete*>(arg)->on_complete(0);
  }
  void on_complete(int r) {
    if (r != 0) {
      u->error = r;
    } else if (u->ddir == DDIR_READ) {
      bl.copy(0, bl.length(), static_cast<char*>(u->xfer_buf));
      u->resid = u->xfer_buflen - bl.length();
    }

    // mark the pointer to indicate completion for fio_ceph_rados_getevents()
    u->engine_data = reinterpret_cast<void*>(1ull);
    u = nullptr;
    bl.clear();
    rados_completion->release();
    delete this;
  }
};

Job::Job(Engine* engine, const thread_data* td)
  : engine(engine),
    events(td->o.iodepth),
    unlink(td->o.unlink)
{
  engine->ref();
  auto o = static_cast<const Options*>(td->eo);

  // open rados
  int ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     engine->deref();
     throw std::system_error(ret, std::system_category(), "job init");
  }

  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     engine->deref();
     throw std::system_error(ret, std::system_category(), "job connect");
  }
  ret = rados.ioctx_create(o->pool, io_ctx);
  if (ret < 0) {
     cerr << "error opening pool " << o->pool << ": "
          << cpp_strerror(ret) << std::endl;
     engine->deref();
     throw std::system_error(ret, std::system_category(), "job pool attach");
  }

  const uint64_t file_size = td->o.size / max(1u, td->o.nr_files);
  for (uint32_t i = 0; i < td->o.nr_files; i++) {
    auto f = td->files[i];
    f->real_file_size = file_size;
    f->engine_pos = i;

    std::string oname("fio_rados_bench.");
    oname += f->file_name;
    oname += '.';
    oname += stringify(td->thread_number); // vary objects for different jobs

    objects.emplace_back(oname);
    ret = io_ctx.create(oname, false);
    if (ret < 0) {
       cerr << "error creating object " << oname << ": "
              << cpp_strerror(ret) << std::endl;
       engine->deref();
       throw std::system_error(ret, std::system_category(), "job object attach");
    }
  }
}

Job::~Job()
{
  if (unlink) {
    for (auto& obj : objects) {
      io_ctx.remove(obj);
    }
  }
  engine->deref();
}

UnitComplete* Job::rados_op_started(io_u* u)
{
  UnitComplete* uc;
  uc = new UnitComplete();
  auto c = rados.aio_create_completion((void *) uc, 0,
    UnitComplete::on_complete_cb);
  uc->init(this, c);
  uc->start(u);
  return uc;
}

int fio_ceph_rados_setup(thread_data* td)
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

void fio_ceph_rados_cleanup(thread_data* td)
{
  auto job = static_cast<Job*>(td->io_ops_data);
  td->io_ops_data = nullptr;
  delete job;
}


io_u* fio_ceph_rados_event(thread_data* td, int event)
{
  // return the requested event from fio_ceph_rados_getevents()
  auto job = static_cast<Job*>(td->io_ops_data);
  return job->events[event];
}

int fio_ceph_rados_getevents(thread_data* td, unsigned int min,
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

int fio_ceph_rados_queue(thread_data* td, io_u* u)
{
  fio_ro_check(td, u);

  auto job = static_cast<Job*>(td->io_ops_data);
  auto& object = job->objects[u->file->engine_pos];

  if (u->ddir == DDIR_WRITE) {

    bufferlist bl;
    bl.push_back(buffer::copy(reinterpret_cast<char*>(u->xfer_buf),
                              u->xfer_buflen ) );

    librados::ObjectWriteOperation op;
    op.write(u->offset, bl);
    job->io_ctx.aio_operate(object,
                            job->rados_op_started(u)->get_rados_completion(),
                            &op);
    return FIO_Q_QUEUED;
  }

  if (u->ddir == DDIR_READ) {
    // ObjectStore reads are synchronous, so make the call and return COMPLETED
    UnitComplete* uc = job->rados_op_started(u);
    int r = job->io_ctx.aio_read(
      object,
      uc->get_rados_completion(),
      uc->get_target_buffer(),
      u->xfer_buflen,
      u->offset);
    if (r < 0) {
      uc->on_complete(r);
      return FIO_Q_COMPLETED;
    }
    return FIO_Q_QUEUED;
  }

  derr << "WARNING: Only DDIR_READ and DDIR_WRITE are supported!" << dendl;
  u->error = -EINVAL;
  td_verror(td, u->error, "xfer");
  return FIO_Q_COMPLETED;
}

int fio_ceph_rados_commit(thread_data* td)
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
int fio_ceph_rados_open(thread_data* td, fio_file* f) { return 0; }
int fio_ceph_rados_close(thread_data* td, fio_file* f) { return 0; }

int fio_ceph_rados_io_u_init(thread_data* td, io_u* u)
{
  // no data is allocated, we just use the pointer as a boolean 'completed' flag
  u->engine_data = nullptr;
  return 0;
}

void fio_ceph_rados_io_u_free(thread_data* td, io_u* u)
{
  u->engine_data = nullptr;
}


// ioengine_ops for get_ioengine()
struct ceph_ioengine : public ioengine_ops {
  ceph_ioengine() : ioengine_ops({}) {
    name        = "ceph-rados";
    version     = FIO_IOOPS_VERSION;
    flags       = FIO_DISKLESSIO;
    setup       = fio_ceph_rados_setup;
    queue       = fio_ceph_rados_queue;
    commit      = fio_ceph_rados_commit;
    getevents   = fio_ceph_rados_getevents;
    event       = fio_ceph_rados_event;
    cleanup     = fio_ceph_rados_cleanup;
    open_file   = fio_ceph_rados_open;
    close_file  = fio_ceph_rados_close;
    io_u_init   = fio_ceph_rados_io_u_init;
    io_u_free   = fio_ceph_rados_io_u_free;
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
