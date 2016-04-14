// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/version.h"

#include <iostream>
#include <sstream>
#include <map>
#include <numeric>
#include <string>
#include <vector>
#include <stdlib.h>
#include <unistd.h>

#include "test/osd/RadosModel.h"


using namespace std;

class WeightedTestGenerator : public TestOpGenerator
{
public:

  WeightedTestGenerator(int ops,
			int objects,
			map<TestOpType, unsigned int> op_weights,
			TestOpStat *stats,
			int max_seconds,
			bool ec_pool,
			bool balance_reads) :
    m_nextop(NULL), m_op(0), m_ops(ops), m_seconds(max_seconds),
    m_objects(objects), m_stats(stats),
    m_total_weight(0),
    m_ec_pool(ec_pool),
    m_balance_reads(balance_reads)
  {
    m_start = time(0);
    for (map<TestOpType, unsigned int>::const_iterator it = op_weights.begin();
	 it != op_weights.end();
	 ++it) {
      m_total_weight += it->second;
      m_weight_sums.insert(pair<TestOpType, unsigned int>(it->first,
							  m_total_weight));
    }
  }

  TestOp *next(RadosTestContext &context)
  {
    TestOp *retval = NULL;

    ++m_op;
    if (m_op <= m_objects) {
      stringstream oid;
      oid << m_op;
      if (m_op % 2) {
	// make it a long name
	oid << " " << string(300, 'o');
      }
      cout << m_op << ": write initial oid " << oid.str() << std::endl;
      context.oid_not_flushing.insert(oid.str());
      if (m_ec_pool) {
	return new WriteOp(m_op, &context, oid.str(), true, true);
      } else {
	return new WriteOp(m_op, &context, oid.str(), false, true);
      }
    } else if (m_op >= m_ops) {
      return NULL;
    }

    if (m_nextop) {
      retval = m_nextop;
      m_nextop = NULL;
      return retval;
    }

    while (retval == NULL) {
      unsigned int rand_val = rand() % m_total_weight;

      time_t now = time(0);
      if (m_seconds && now - m_start > m_seconds)
	break;

      for (map<TestOpType, unsigned int>::const_iterator it = m_weight_sums.begin();
	   it != m_weight_sums.end();
	   ++it) {
	if (rand_val < it->second) {
	  retval = gen_op(context, it->first);
	  break;
	}
      }
    }
    return retval;
  }

private:

  TestOp *gen_op(RadosTestContext &context, TestOpType type)
  {
    string oid, oid2;
    //cout << "oids not in use " << context.oid_not_in_use.size() << std::endl;
    assert(context.oid_not_in_use.size());

    switch (type) {
    case TEST_OP_READ:
      oid = *(rand_choose(context.oid_not_in_use));
      return new ReadOp(m_op, &context, oid, m_balance_reads, m_stats);

    case TEST_OP_WRITE:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "write oid " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new WriteOp(m_op, &context, oid, false, false, m_stats);

    case TEST_OP_WRITE_EXCL:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "write (excl) oid "
	   << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new WriteOp(m_op, &context, oid, false, true, m_stats);

    case TEST_OP_DELETE:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "delete oid " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new DeleteOp(m_op, &context, oid, m_stats);

    case TEST_OP_SNAP_CREATE:
      cout << m_op << ": " << "snap_create" << std::endl;
      return new SnapCreateOp(m_op, &context, m_stats);

    case TEST_OP_SNAP_REMOVE:
      if (context.snaps.size() <= context.snaps_in_use.size()) {
	return NULL;
      }
      while (true) {
	int snap = rand_choose(context.snaps)->first;
	if (context.snaps_in_use.lookup(snap))
	  continue;  // in use; try again!
	cout << m_op << ": " << "snap_remove snap " << snap << std::endl;
	return new SnapRemoveOp(m_op, &context, snap, m_stats);
      }

    case TEST_OP_ROLLBACK:
      {
	string oid = *(rand_choose(context.oid_not_in_use));
	cout << m_op << ": " << "rollback oid " << oid << " current snap is "
	     << context.current_snap << std::endl;
	return new RollbackOp(m_op, &context, oid);
      }

    case TEST_OP_SETATTR:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "setattr oid " << oid
	   << " current snap is " << context.current_snap << std::endl;
      return new SetAttrsOp(m_op, &context, oid, m_stats);

    case TEST_OP_RMATTR:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "rmattr oid " << oid
	   << " current snap is " << context.current_snap << std::endl;
      return new RemoveAttrsOp(m_op, &context, oid, m_stats);

    case TEST_OP_WATCH:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "watch oid " << oid
	   << " current snap is " << context.current_snap << std::endl;
      return new WatchOp(m_op, &context, oid, m_stats);

    case TEST_OP_COPY_FROM:
      oid = *(rand_choose(context.oid_not_in_use));
      do {
	oid2 = *(rand_choose(context.oid_not_in_use));
      } while (oid == oid2);
      cout << m_op << ": " << "copy_from oid " << oid << " from oid " << oid2
	   << " current snap is " << context.current_snap << std::endl;
      return new CopyFromOp(m_op, &context, oid, oid2, m_stats);

    case TEST_OP_HIT_SET_LIST:
      {
	uint32_t hash = rjhash32(rand());
	cout << m_op << ": " << "hit_set_list " << hash << std::endl;
	return new HitSetListOp(m_op, &context, hash, m_stats);
      }

    case TEST_OP_UNDIRTY:
      {
	oid = *(rand_choose(context.oid_not_in_use));
	cout << m_op << ": " << "undirty oid " << oid << std::endl;
	return new UndirtyOp(m_op, &context, oid, m_stats);
      }

    case TEST_OP_IS_DIRTY:
      {
	oid = *(rand_choose(context.oid_not_flushing));
	return new IsDirtyOp(m_op, &context, oid, m_stats);
      }

    case TEST_OP_CACHE_FLUSH:
      {
	oid = *(rand_choose(context.oid_not_in_use));
	return new CacheFlushOp(m_op, &context, oid, m_stats, true);
      }

    case TEST_OP_CACHE_TRY_FLUSH:
      {
	oid = *(rand_choose(context.oid_not_in_use));
	return new CacheFlushOp(m_op, &context, oid, m_stats, false);
      }

    case TEST_OP_CACHE_EVICT:
      {
	oid = *(rand_choose(context.oid_not_in_use));
	return new CacheEvictOp(m_op, &context, oid, m_stats);
      }

    case TEST_OP_APPEND:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "append oid " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new WriteOp(m_op, &context, oid, true, false, m_stats);

    case TEST_OP_APPEND_EXCL:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "append oid (excl) " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new WriteOp(m_op, &context, oid, true, true, m_stats);

    default:
      cerr << m_op << ": Invalid op type " << type << std::endl;
      assert(0);
    }
  }

  TestOp *m_nextop;
  int m_op;
  int m_ops;
  int m_seconds;
  int m_objects;
  time_t m_start;
  TestOpStat *m_stats;
  map<TestOpType, unsigned int> m_weight_sums;
  unsigned int m_total_weight;
  bool m_ec_pool;
  bool m_balance_reads;
};

int main(int argc, char **argv)
{
  int ops = 1000;
  int objects = 50;
  int max_in_flight = 16;
  int64_t size = 4000000; // 4 MB
  int64_t min_stride_size = -1, max_stride_size = -1;
  int max_seconds = 0;
  bool pool_snaps = false;
  bool write_fadvise_dontneed = false;

  struct {
    TestOpType op;
    const char *name;
    bool ec_pool_valid;
  } op_types[] = {
    { TEST_OP_READ, "read", true },
    { TEST_OP_WRITE, "write", false },
    { TEST_OP_WRITE_EXCL, "write_excl", false },
    { TEST_OP_DELETE, "delete", true },
    { TEST_OP_SNAP_CREATE, "snap_create", true },
    { TEST_OP_SNAP_REMOVE, "snap_remove", true },
    { TEST_OP_ROLLBACK, "rollback", true },
    { TEST_OP_SETATTR, "setattr", true },
    { TEST_OP_RMATTR, "rmattr", true },
    { TEST_OP_WATCH, "watch", true },
    { TEST_OP_COPY_FROM, "copy_from", true },
    { TEST_OP_HIT_SET_LIST, "hit_set_list", true },
    { TEST_OP_IS_DIRTY, "is_dirty", true },
    { TEST_OP_UNDIRTY, "undirty", true },
    { TEST_OP_CACHE_FLUSH, "cache_flush", true },
    { TEST_OP_CACHE_TRY_FLUSH, "cache_try_flush", true },
    { TEST_OP_CACHE_EVICT, "cache_evict", true },
    { TEST_OP_APPEND, "append", true },
    { TEST_OP_APPEND_EXCL, "append_excl", true },
    { TEST_OP_READ /* grr */, NULL },
  };

  map<TestOpType, unsigned int> op_weights;
  string pool_name = "rbd";
  bool ec_pool = false;
  bool no_omap = false;
  bool balance_reads = false;

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--max-ops") == 0)
      ops = atoi(argv[++i]);
    else if (strcmp(argv[i], "--pool") == 0)
      pool_name = argv[++i];
    else if (strcmp(argv[i], "--max-seconds") == 0)
      max_seconds = atoi(argv[++i]);
    else if (strcmp(argv[i], "--objects") == 0)
      objects = atoi(argv[++i]);
    else if (strcmp(argv[i], "--max-in-flight") == 0)
      max_in_flight = atoi(argv[++i]);
    else if (strcmp(argv[i], "--size") == 0)
      size = atoi(argv[++i]);
    else if (strcmp(argv[i], "--min-stride-size") == 0)
      min_stride_size = atoi(argv[++i]);
    else if (strcmp(argv[i], "--max-stride-size") == 0)
      max_stride_size = atoi(argv[++i]);
    else if (strcmp(argv[i], "--no-omap") == 0)
      no_omap = true;
    else if (strcmp(argv[i], "--balance_reads") == 0)
      balance_reads = true;
    else if (strcmp(argv[i], "--pool-snaps") == 0)
      pool_snaps = true;
    else if (strcmp(argv[i], "--write-fadvise-dontneed") == 0)
      write_fadvise_dontneed = true;
    else if (strcmp(argv[i], "--ec-pool") == 0) {
      if (!op_weights.empty()) {
	cerr << "--ec-pool must be specified prior to any ops" << std::endl;
	exit(1);
      }
      ec_pool = true;
      no_omap = true;
    } else if (strcmp(argv[i], "--op") == 0) {
      i++;
      if (i == argc) {
        cerr << "Missing op after --op" << std::endl;
        return 1;
      }
      int j;
      for (j = 0; op_types[j].name; ++j) {
	if (strcmp(op_types[j].name, argv[i]) == 0) {
	  break;
	}
      }
      if (!op_types[j].name) {
	cerr << "unknown op " << argv[i] << std::endl;
	exit(1);
      }
      i++;
      if (i == argc) {
	cerr << "Weight unspecified." << std::endl;
	return 1;
      }
      int weight = atoi(argv[i]);
      if (weight < 0) {
	cerr << "Weights must be nonnegative." << std::endl;
	return 1;
      } else if (weight > 0) {
	if (ec_pool && !op_types[j].ec_pool_valid) {
	  cerr << "Error: cannot use op type " << op_types[j].name
	       << " with --ec-pool" << std::endl;
	  exit(1);
	}
	cout << "adding op weight " << op_types[j].name << " -> " << weight << std::endl;
	op_weights.insert(pair<TestOpType, unsigned int>(op_types[j].op, weight));
      }
    } else {
      cerr << "unknown arg " << argv[i] << std::endl;
      //usage();
      exit(1);
    }
  }

  if (op_weights.empty()) {
    cerr << "No operations specified" << std::endl;
    //usage();
    exit(1);
  }

  if (min_stride_size < 0)
    min_stride_size = size / 10;
  if (max_stride_size < 0)
    max_stride_size = size / 5;

  cout << pretty_version_to_str() << std::endl;
  cout << "Configuration:" << std::endl
       << "\tNumber of operations: " << ops << std::endl
       << "\tNumber of objects: " << objects << std::endl
       << "\tMax in flight operations: " << max_in_flight << std::endl
       << "\tObject size (in bytes): " << size << std::endl
       << "\tWrite stride min: " << min_stride_size << std::endl
       << "\tWrite stride max: " << max_stride_size << std::endl;

  if (min_stride_size > max_stride_size) {
    cerr << "Error: min_stride_size cannot be more than max_stride_size"
	 << std::endl;
    return 1;
  }

  if (min_stride_size > size || max_stride_size > size) {
    cerr << "Error: min_stride_size and max_stride_size must be "
	 << "smaller than object size" << std::endl;
    return 1;
  }

  if (max_in_flight * 2 > objects) {
    cerr << "Error: max_in_flight must be <= than the number of objects / 2"
	 << std::endl;
    return 1;
  }

  char *id = getenv("CEPH_CLIENT_ID");
  RadosTestContext context(
    pool_name,
    max_in_flight,
    size,
    min_stride_size,
    max_stride_size,
    no_omap,
    pool_snaps,
    write_fadvise_dontneed,
    id);

  TestOpStat stats;
  WeightedTestGenerator gen = WeightedTestGenerator(
    ops, objects,
    op_weights, &stats, max_seconds,
    ec_pool, balance_reads);
  int r = context.init();
  if (r < 0) {
    cerr << "Error initializing rados test context: "
	 << cpp_strerror(r) << std::endl;
    exit(1);
  }
  context.loop(&gen);

  context.shutdown();
  cerr << context.errors << " errors." << std::endl;
  cerr << stats << std::endl;
  return 0;
}
