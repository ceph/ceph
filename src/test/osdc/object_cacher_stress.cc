// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdlib>
#include <ctime>
#include <sstream>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/Mutex.h"
#include "common/snap_types.h"
#include "global/global_init.h"
#include "include/atomic.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/stringify.h"
#include "osdc/ObjectCacher.h"

#include "FakeWriteback.h"

// XXX: Only tests default namespace
struct op_data {
  op_data(std::string oid, uint64_t offset, uint64_t len, bool read)
    : extent(oid, 0, offset, len, 0), is_read(read)
  {
    extent.oloc.pool = 0;
    extent.buffer_extents.push_back(make_pair(0, len));
  }

  ObjectExtent extent;
  bool is_read;
  ceph::bufferlist result;
  atomic_t done;
};

class C_Count : public Context {
  op_data *m_op;
  atomic_t *m_outstanding;
public:
  C_Count(op_data *op, atomic_t *outstanding)
    : m_op(op), m_outstanding(outstanding) {}
  void finish(int r) {
    m_op->done.inc();
    assert(m_outstanding->read() > 0);
    m_outstanding->dec();
  }
};

int stress_test(uint64_t num_ops, uint64_t num_objs,
		uint64_t max_obj_size, uint64_t delay_ns,
		uint64_t max_op_len, float percent_reads)
{
  Mutex lock("object_cacher_stress::object_cacher");
  FakeWriteback writeback(g_ceph_context, &lock, delay_ns);

  ObjectCacher obc(g_ceph_context, "test", writeback, lock, NULL, NULL,
		   g_conf->client_oc_size,
		   g_conf->client_oc_max_objects,
		   g_conf->client_oc_max_dirty,
		   g_conf->client_oc_target_dirty,
		   g_conf->client_oc_max_dirty_age,
		   true);
  obc.start();

  atomic_t outstanding_reads;
  vector<ceph::shared_ptr<op_data> > ops;
  ObjectCacher::ObjectSet object_set(NULL, 0, 0);
  SnapContext snapc;
  ceph::buffer::ptr bp(max_op_len);
  ceph::bufferlist bl;
  bp.zero();
  bl.append(bp);

  // schedule ops
  std::cout << "Test configuration:\n\n"
	    << setw(10) << "ops: " << num_ops << "\n"
	    << setw(10) << "objects: " << num_objs << "\n"
	    << setw(10) << "obj size: " << max_obj_size << "\n"
	    << setw(10) << "delay: " << delay_ns << "\n"
	    << setw(10) << "max op len: " << max_op_len << "\n"
	    << setw(10) << "percent reads: " << percent_reads << "\n\n";

  for (uint64_t i = 0; i < num_ops; ++i) {
    uint64_t offset = random() % max_obj_size;
    uint64_t max_len = MIN(max_obj_size - offset, max_op_len);
    // no zero-length operations
    uint64_t length = random() % (MAX(max_len - 1, 1)) + 1;
    std::string oid = "test" + stringify(random() % num_objs);
    bool is_read = random() < percent_reads * RAND_MAX;
    ceph::shared_ptr<op_data> op(new op_data(oid, offset, length, is_read));
    ops.push_back(op);
    std::cout << "op " << i << " " << (is_read ? "read" : "write")
	      << " " << op->extent << "\n";
    if (op->is_read) {
      ObjectCacher::OSDRead *rd = obc.prepare_read(CEPH_NOSNAP, &op->result, 0);
      rd->extents.push_back(op->extent);
      outstanding_reads.inc();
      Context *completion = new C_Count(op.get(), &outstanding_reads);
      lock.Lock();
      int r = obc.readx(rd, &object_set, completion);
      lock.Unlock();
      assert(r >= 0);
      if ((uint64_t)r == length)
	completion->complete(r);
      else
	assert(r == 0);
    } else {
      ObjectCacher::OSDWrite *wr = obc.prepare_write(snapc, bl, utime_t(), 0);
      wr->extents.push_back(op->extent);
      lock.Lock();
      obc.writex(wr, &object_set, lock, NULL);
      lock.Unlock();
    }
  }

  // check that all reads completed
  for (uint64_t i = 0; i < num_ops; ++i) {
    if (!ops[i]->is_read)
      continue;
    std::cout << "waiting for read " << i << ops[i]->extent << std::endl;
    uint64_t done = 0;
    while (done == 0) {
      done = ops[i]->done.read();
      if (!done) {
	usleep(500);
      }
    }
    if (done > 1) {
      std::cout << "completion called more than once!\n" << std::endl;
      return EXIT_FAILURE;
    }
  }

  lock.Lock();
  obc.release_set(&object_set);
  lock.Unlock();

  int r = 0;
  Mutex mylock("librbd::ImageCtx::flush_cache");
  Cond cond;
  bool done;
  Context *onfinish = new C_SafeCond(&mylock, &cond, &done, &r);
  lock.Lock();
  bool already_flushed = obc.flush_set(&object_set, onfinish);
  std::cout << "already flushed = " << already_flushed << std::endl;
  lock.Unlock();
  mylock.Lock();
  while (!done) {
    cond.Wait(mylock);
  }
  mylock.Unlock();

  lock.Lock();
  bool unclean = obc.release_set(&object_set);
  lock.Unlock();

  if (unclean) {
    std::cout << "unclean buffers left over!" << std::endl;
    return EXIT_FAILURE;
  }

  obc.stop();

  std::cout << "Test completed successfully." << std::endl;

  return EXIT_SUCCESS;
}

int main(int argc, const char **argv)
{
  std::vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);

  long long delay_ns = 0;
  long long num_ops = 1000;
  long long obj_bytes = 4 << 20;
  long long max_len = 128 << 10;
  long long num_objs = 10;
  float percent_reads = 0.90;
  int seed = time(0) % 100000;
  std::ostringstream err;
  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end();) {
    if (ceph_argparse_withlonglong(args, i, &delay_ns, &err, "--delay-ns", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_withlonglong(args, i, &num_ops, &err, "--ops", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_withlonglong(args, i, &num_objs, &err, "--objects", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_withlonglong(args, i, &obj_bytes, &err, "--obj-size", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_withlonglong(args, i, &max_len, &err, "--max-op-size", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_withfloat(args, i, &percent_reads, &err, "--percent-read", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_withint(args, i, &seed, &err, "--seed", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else {
      cerr << "unknown option " << *i << std::endl;
      return EXIT_FAILURE;
    }
  }

  srandom(seed);
  return stress_test(num_ops, num_objs, obj_bytes, delay_ns, max_len, percent_reads);
}
