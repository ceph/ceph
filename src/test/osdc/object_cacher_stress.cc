// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdlib>
#include <ctime>
#include <sstream>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/ceph_argparse.h"
#include "common/ceph_mutex.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/snap_types.h"
#include "global/global_init.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/stringify.h"
#include "osdc/ObjectCacher.h"

#include "FakeWriteback.h"
#include "MemWriteback.h"

#include <atomic>

using namespace std;

// XXX: Only tests default namespace
struct op_data {
  op_data(const std::string &oid, uint64_t offset, uint64_t len, bool read)
    : extent(oid, 0, offset, len, 0), is_read(read)
  {
    extent.oloc.pool = 0;
    extent.buffer_extents.push_back(make_pair(0, len));
  }

  ObjectExtent extent;
  bool is_read;
  ceph::bufferlist result;
  std::atomic<unsigned> done = { 0 };
};

class C_Count : public Context {
  op_data *m_op;
  std::atomic<unsigned> *m_outstanding = nullptr;
public:
  C_Count(op_data *op, std::atomic<unsigned> *outstanding)
    : m_op(op), m_outstanding(outstanding) {}
  void finish(int r) override {
    m_op->done++;
    ceph_assert(*m_outstanding > 0);
    (*m_outstanding)--;
  }
};

int stress_test(uint64_t num_ops, uint64_t num_objs,
		uint64_t max_obj_size, uint64_t delay_ns,
		uint64_t max_op_len, float percent_reads)
{
  ceph::mutex lock = ceph::make_mutex("object_cacher_stress::object_cacher");
  FakeWriteback writeback(g_ceph_context, &lock, delay_ns);

  ObjectCacher obc(g_ceph_context, "test", writeback, lock, NULL, NULL,
		   g_conf()->client_oc_size,
		   g_conf()->client_oc_max_objects,
		   g_conf()->client_oc_max_dirty,
		   g_conf()->client_oc_target_dirty,
		   g_conf()->client_oc_max_dirty_age,
		   true);
  obc.start();

  std::atomic<unsigned> outstanding_reads = { 0 };
  vector<std::shared_ptr<op_data> > ops;
  ObjectCacher::ObjectSet object_set(NULL, 0, 0);
  SnapContext snapc;
  ceph::buffer::ptr bp(max_op_len);
  ceph::bufferlist bl;
  uint64_t journal_tid = 0;
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
    uint64_t max_len = std::min(max_obj_size - offset, max_op_len);
    // no zero-length operations
    uint64_t length = random() % (std::max<uint64_t>(max_len - 1, 1)) + 1;
    std::string oid = "test" + stringify(random() % num_objs);
    bool is_read = random() < percent_reads * float(RAND_MAX);
    std::shared_ptr<op_data> op(new op_data(oid, offset, length, is_read));
    ops.push_back(op);
    std::cout << "op " << i << " " << (is_read ? "read" : "write")
	      << " " << op->extent << "\n";
    if (op->is_read) {
      ObjectCacher::OSDRead *rd = obc.prepare_read(CEPH_NOSNAP, &op->result, 0);
      rd->extents.push_back(op->extent);
      outstanding_reads++;
      Context *completion = new C_Count(op.get(), &outstanding_reads);
      lock.lock();
      int r = obc.readx(rd, &object_set, completion);
      lock.unlock();
      ceph_assert(r >= 0);
      if ((uint64_t)r == length)
	completion->complete(r);
      else
	ceph_assert(r == 0);
    } else {
      ObjectCacher::OSDWrite *wr = obc.prepare_write(snapc, bl,
						     ceph::real_clock::zero(), 0,
						     ++journal_tid);
      wr->extents.push_back(op->extent);
      lock.lock();
      obc.writex(wr, &object_set, NULL);
      lock.unlock();
    }
  }

  // check that all reads completed
  for (uint64_t i = 0; i < num_ops; ++i) {
    if (!ops[i]->is_read)
      continue;
    std::cout << "waiting for read " << i << ops[i]->extent << std::endl;
    uint64_t done = 0;
    while (done == 0) {
      done = ops[i]->done;
      if (!done) {
	usleep(500);
      }
    }
    if (done > 1) {
      std::cout << "completion called more than once!\n" << std::endl;
      return EXIT_FAILURE;
    }
  }

  lock.lock();
  obc.release_set(&object_set);
  lock.unlock();

  int r = 0;
  ceph::mutex mylock = ceph::make_mutex("librbd::ImageCtx::flush_cache");
  ceph::condition_variable cond;
  bool done;
  Context *onfinish = new C_SafeCond(mylock, cond, &done, &r);
  lock.lock();
  bool already_flushed = obc.flush_set(&object_set, onfinish);
  std::cout << "already flushed = " << already_flushed << std::endl;
  lock.unlock();
  {
    std::unique_lock locker{mylock};
    cond.wait(locker, [&done] { return done; });
  }
  lock.lock();
  bool unclean = obc.release_set(&object_set);
  lock.unlock();

  if (unclean) {
    std::cout << "unclean buffers left over!" << std::endl;
    return EXIT_FAILURE;
  }

  obc.stop();

  std::cout << "Test completed successfully." << std::endl;

  return EXIT_SUCCESS;
}

int correctness_test(uint64_t delay_ns)
{
  std::cerr << "starting correctness test" << std::endl;
  ceph::mutex lock = ceph::make_mutex("object_cacher_stress::object_cacher");
  MemWriteback writeback(g_ceph_context, &lock, delay_ns);

  ObjectCacher obc(g_ceph_context, "test", writeback, lock, NULL, NULL,
		   1<<21, // max cache size, 2MB
		   1, // max objects, just one
		   1<<18, // max dirty, 256KB
		   1<<17, // target dirty, 128KB
		   g_conf()->client_oc_max_dirty_age,
		   true);
  obc.start();
  std::cerr << "just start()ed ObjectCacher" << std::endl;

  SnapContext snapc;
  ceph_tid_t journal_tid = 0;
  std::string oid("correctness_test_obj");
  ObjectCacher::ObjectSet object_set(NULL, 0, 0);
  ceph::bufferlist zeroes_bl;
  zeroes_bl.append_zero(1<<20);

  // set up a 4MB all-zero object
  std::cerr << "writing 4x1MB object" << std::endl;
  std::map<int, C_SaferCond> create_finishers;
  for (int i = 0; i < 4; ++i) {
    ObjectCacher::OSDWrite *wr = obc.prepare_write(snapc, zeroes_bl,
						   ceph::real_clock::zero(), 0,
						   ++journal_tid);
    ObjectExtent extent(oid, 0, zeroes_bl.length()*i, zeroes_bl.length(), 0);
    extent.oloc.pool = 0;
    extent.buffer_extents.push_back(make_pair(0, 1<<20));
    wr->extents.push_back(extent);
    lock.lock();
    obc.writex(wr, &object_set, &create_finishers[i]);
    lock.unlock();
  }

  // write some 1-valued bits at 256-KB intervals for checking consistency
  std::cerr << "Writing some 0xff values" << std::endl;
  ceph::buffer::ptr ones(1<<16);
  memset(ones.c_str(), 0xff, ones.length());
  ceph::bufferlist ones_bl;
  ones_bl.append(ones);
  for (int i = 1<<18; i < 1<<22; i+=1<<18) {
    ObjectCacher::OSDWrite *wr = obc.prepare_write(snapc, ones_bl,
						   ceph::real_clock::zero(), 0,
						   ++journal_tid);
    ObjectExtent extent(oid, 0, i, ones_bl.length(), 0);
    extent.oloc.pool = 0;
    extent.buffer_extents.push_back(make_pair(0, 1<<16));
    wr->extents.push_back(extent);
    lock.lock();
    obc.writex(wr, &object_set, &create_finishers[i]);
    lock.unlock();
  }

  for (auto i = create_finishers.begin(); i != create_finishers.end(); ++i) {
    i->second.wait();
  }
  std::cout << "Finished setting up object" << std::endl;
  lock.lock();
  C_SaferCond flushcond;
  bool done = obc.flush_all(&flushcond);
  if (!done) {
    std::cout << "Waiting for flush" << std::endl;
    lock.unlock();
    flushcond.wait();
    lock.lock();
  }
  lock.unlock();

  /* now read the back half of the object in, check consistency,
   */
  std::cout << "Reading back half of object (1<<21~1<<21)" << std::endl;
  bufferlist readbl;
  C_SaferCond backreadcond;
  ObjectCacher::OSDRead *back_half_rd = obc.prepare_read(CEPH_NOSNAP, &readbl, 0);
  ObjectExtent back_half_extent(oid, 0, 1<<21, 1<<21, 0);
  back_half_extent.oloc.pool = 0;
  back_half_extent.buffer_extents.push_back(make_pair(0, 1<<21));
  back_half_rd->extents.push_back(back_half_extent);
  lock.lock();
  int r = obc.readx(back_half_rd, &object_set, &backreadcond);
  lock.unlock();
  ceph_assert(r >= 0);
  if (r == 0) {
    std::cout << "Waiting to read data into cache" << std::endl;
    r = backreadcond.wait();
  }

  ceph_assert(r == 1<<21);

  /* Read the whole object in,
   * verify we have to wait for it to complete,
   * overwrite a small piece, (http://tracker.ceph.com/issues/16002),
   * and check consistency */

  readbl.clear();
  std::cout<< "Reading whole object (0~1<<22)" << std::endl;
  C_SaferCond frontreadcond;
  ObjectCacher::OSDRead *whole_rd = obc.prepare_read(CEPH_NOSNAP, &readbl, 0);
  ObjectExtent whole_extent(oid, 0, 0, 1<<22, 0);
  whole_extent.oloc.pool = 0;
  whole_extent.buffer_extents.push_back(make_pair(0, 1<<22));
  whole_rd->extents.push_back(whole_extent);
  lock.lock();
  r = obc.readx(whole_rd, &object_set, &frontreadcond);
  // we cleared out the cache by reading back half, it shouldn't pass immediately!
  ceph_assert(r == 0);
  std::cout << "Data (correctly) not available without fetching" << std::endl;

  ObjectCacher::OSDWrite *verify_wr = obc.prepare_write(snapc, ones_bl,
							ceph::real_clock::zero(), 0,
							++journal_tid);
  ObjectExtent verify_extent(oid, 0, (1<<18)+(1<<16), ones_bl.length(), 0);
  verify_extent.oloc.pool = 0;
  verify_extent.buffer_extents.push_back(make_pair(0, 1<<16));
  verify_wr->extents.push_back(verify_extent);
  C_SaferCond verify_finisher;
  obc.writex(verify_wr, &object_set, &verify_finisher);
  lock.unlock();
  std::cout << "wrote dirtying data" << std::endl;

  std::cout << "Waiting to read data into cache" << std::endl;
  frontreadcond.wait();
  verify_finisher.wait();

  std::cout << "Validating data" << std::endl;

  for (int i = 1<<18; i < 1<<22; i+=1<<18) {
    bufferlist ones_maybe;
    ones_maybe.substr_of(readbl, i, ones_bl.length());
    ceph_assert(0 == memcmp(ones_maybe.c_str(), ones_bl.c_str(), ones_bl.length()));
  }
  bufferlist ones_maybe;
  ones_maybe.substr_of(readbl, (1<<18)+(1<<16), ones_bl.length());
  ceph_assert(0 == memcmp(ones_maybe.c_str(), ones_bl.c_str(), ones_bl.length()));

  std::cout << "validated that data is 0xff where it should be" << std::endl;
  
  lock.lock();
  C_SaferCond flushcond2;
  done = obc.flush_all(&flushcond2);
  if (!done) {
    std::cout << "Waiting for final write flush" << std::endl;
    lock.unlock();
    flushcond2.wait();
    lock.lock();
  }

  bool unclean = obc.release_set(&object_set);
  if (unclean) {
    std::cout << "unclean buffers left over!" << std::endl;
    vector<ObjectExtent> discard_extents;
    int i = 0;
    for (auto oi = object_set.objects.begin(); !oi.end(); ++oi) {
      discard_extents.emplace_back(oid, i++, 0, 1<<22, 0);
    }
    obc.discard_set(&object_set, discard_extents);
    lock.unlock();
    obc.stop();
    goto fail;
  }
  lock.unlock();

  obc.stop();

  std::cout << "Testing ObjectCacher correctness complete" << std::endl;
  return EXIT_SUCCESS;

 fail:
  return EXIT_FAILURE;
}

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  long long delay_ns = 0;
  long long num_ops = 1000;
  long long obj_bytes = 4 << 20;
  long long max_len = 128 << 10;
  long long num_objs = 10;
  float percent_reads = 0.90;
  int seed = time(0) % 100000;
  bool stress = false;
  bool correctness = false;
  std::ostringstream err;
  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end();) {
    if (ceph_argparse_witharg(args, i, &delay_ns, err, "--delay-ns", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &num_ops, err, "--ops", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &num_objs, err, "--objects", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &obj_bytes, err, "--obj-size", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &max_len, err, "--max-op-size", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &percent_reads, err, "--percent-read", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &seed, err, "--seed", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << argv[0] << ": " << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_flag(args, i, "--stress-test", NULL)) {
      stress = true;
    } else if (ceph_argparse_flag(args, i, "--correctness-test", NULL)) {
      correctness = true;
    } else {
      cerr << "unknown option " << *i << std::endl;
      return EXIT_FAILURE;
    }
  }

  if (stress) {
    srandom(seed);
    return stress_test(num_ops, num_objs, obj_bytes, delay_ns, max_len, percent_reads);
  }
  if (correctness) {
    return correctness_test(delay_ns);
  }
}
