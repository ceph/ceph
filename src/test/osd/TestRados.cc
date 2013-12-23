// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
			int max_seconds) :
    m_nextop(NULL), m_op(0), m_ops(ops), m_seconds(max_seconds), m_objects(objects), m_stats(stats),
    m_total_weight(0)
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
      cout << m_op << ": write initial oid " << oid.str() << std::endl;
      return new WriteOp(m_op, &context, oid.str());
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

    cout << m_op << ": ";
    switch (type) {
    case TEST_OP_READ:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "read oid " << oid << std::endl;
      return new ReadOp(m_op, &context, oid, m_stats);

    case TEST_OP_WRITE:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "write oid " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new WriteOp(m_op, &context, oid, m_stats);

    case TEST_OP_DELETE:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "delete oid " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new DeleteOp(m_op, &context, oid, m_stats);

    case TEST_OP_SNAP_CREATE:
      cout << "snap_create" << std::endl;
      return new SnapCreateOp(m_op, &context, m_stats);

    case TEST_OP_SNAP_REMOVE:
      if (context.snaps.size() <= context.snaps_in_use.size()) {
	return NULL;
      }
      while (true) {
	int snap = rand_choose(context.snaps)->first;
	if (context.snaps_in_use.lookup(snap))
	  continue;  // in use; try again!
	cout << "snap_remove snap " << snap << std::endl;
	return new SnapRemoveOp(m_op, &context, snap, m_stats);
      }

    case TEST_OP_ROLLBACK:
      {
	string oid = *(rand_choose(context.oid_not_in_use));
	cout << "rollback oid " << oid << std::endl;
	return new RollbackOp(m_op, &context, oid);
      }

    case TEST_OP_SETATTR:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "setattr oid " << oid
	   << " current snap is " << context.current_snap << std::endl;
      return new SetAttrsOp(m_op, &context, oid, m_stats);

    case TEST_OP_RMATTR:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "rmattr oid " << oid
	   << " current snap is " << context.current_snap << std::endl;
      return new RemoveAttrsOp(m_op, &context, oid, m_stats);

    case TEST_OP_TMAPPUT:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "tmapput oid " << oid
	   << " current snap is " << context.current_snap << std::endl;
      return new TmapPutOp(m_op, &context, oid, m_stats);

    case TEST_OP_WATCH:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "watch oid " << oid
	   << " current snap is " << context.current_snap << std::endl;
      return new WatchOp(m_op, &context, oid, m_stats);

    case TEST_OP_COPY_FROM:
      oid = *(rand_choose(context.oid_not_in_use));
      do {
	oid2 = *(rand_choose(context.oid_not_in_use));
      } while (oid == oid2);
      cout << "copy_from oid " << oid << " from oid " << oid2
	   << " current snap is " << context.current_snap << std::endl;
      return new CopyFromOp(m_op, &context, oid, oid2, m_stats);

    case TEST_OP_HIT_SET_LIST:
      {
	uint32_t hash = rjhash32(rand());
	cout << "hit_set_list " << hash << std::endl;
	return new HitSetListOp(m_op, &context, hash, m_stats);
      }

    case TEST_OP_UNDIRTY:
      {
	oid = *(rand_choose(context.oid_not_in_use));
	cout << "undirty oid " << oid << std::endl;
	return new UndirtyOp(m_op, &context, oid, m_stats);
      }

    case TEST_OP_IS_DIRTY:
      {
	oid = *(rand_choose(context.oid_not_in_use));
	cout << "is_dirty oid " << oid << std::endl;
	return new IsDirtyOp(m_op, &context, oid, m_stats);
      }

    case TEST_OP_CACHE_FLUSH:
      {
	oid = *(rand_choose(context.oid_not_in_use));
	cout << "cache_flush oid " << oid << std::endl;
	return new CacheFlushOp(m_op, &context, oid, m_stats, true);
      }

    case TEST_OP_CACHE_TRY_FLUSH:
      {
	oid = *(rand_choose(context.oid_not_in_use));
	cout << "cache_try_flush oid " << oid << std::endl;
	return new CacheFlushOp(m_op, &context, oid, m_stats, false);
      }

    case TEST_OP_CACHE_EVICT:
      {
	oid = *(rand_choose(context.oid_not_in_use));
	cout << "cache_evict oid " << oid << std::endl;
	return new CacheEvictOp(m_op, &context, oid, m_stats);
      }

    default:
      cerr << "Invalid op type " << type << std::endl;
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
};

int main(int argc, char **argv)
{
  int ops = 1000;
  int objects = 50;
  int max_in_flight = 16;
  int64_t size = 4000000; // 4 MB
  int64_t min_stride_size = -1, max_stride_size = -1;
  int max_seconds = 0;

  struct {
    TestOpType op;
    const char *name;
  } op_types[] = {
    { TEST_OP_READ, "read" },
    { TEST_OP_WRITE, "write" },
    { TEST_OP_DELETE, "delete" },
    { TEST_OP_SNAP_CREATE, "snap_create" },
    { TEST_OP_SNAP_REMOVE, "snap_remove" },
    { TEST_OP_ROLLBACK, "rollback" },
    { TEST_OP_SETATTR, "setattr" },
    { TEST_OP_RMATTR, "rmattr" },
    { TEST_OP_TMAPPUT, "tmapput" },
    { TEST_OP_WATCH, "watch" },
    { TEST_OP_COPY_FROM, "copy_from" },
    { TEST_OP_HIT_SET_LIST, "hit_set_list" },
    { TEST_OP_IS_DIRTY, "is_dirty" },
    { TEST_OP_UNDIRTY, "undirty" },
    { TEST_OP_CACHE_FLUSH, "cache_flush" },
    { TEST_OP_CACHE_TRY_FLUSH, "cache_try_flush" },
    { TEST_OP_CACHE_EVICT, "cache_evict" },
    { TEST_OP_READ /* grr */, NULL },
  };

  map<TestOpType, unsigned int> op_weights;
  string pool_name = "data";

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
    else if (strcmp(argv[i], "--op") == 0) {
      i++;
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
      int weight = atoi(argv[++i]);
      if (weight < 0) {
	cerr << "Weights must be nonnegative." << std::endl;
	return 1;
      }
      cout << "adding op weight " << op_types[j].name << " -> " << weight << std::endl;
      op_weights.insert(pair<TestOpType, unsigned int>(op_types[j].op, weight));
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
  VarLenGenerator cont_gen(size, min_stride_size, max_stride_size);
  RadosTestContext context(pool_name, max_in_flight, cont_gen, id);

  TestOpStat stats;
  WeightedTestGenerator gen = WeightedTestGenerator(ops, objects,
						    op_weights, &stats, max_seconds);
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
