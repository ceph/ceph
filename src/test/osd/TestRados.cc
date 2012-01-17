// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "common/Mutex.h"
#include "common/Cond.h"

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
			TestOpStat *stats) :
    m_nextop(NULL), m_op(0), m_ops(ops), m_objects(objects), m_stats(stats),
    m_total_weight(0)
  {
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
      cout << "Writing initial " << oid.str() << std::endl;
      return new WriteOp(&context, oid.str());
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
      for (map<TestOpType, unsigned int>::const_iterator it = m_weight_sums.begin();
	   it != m_weight_sums.end();
	   ++it) {
	if (rand_val < it->second) {
	  cout << m_op << ": ";
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
    string oid;
    switch (type) {
    case TEST_OP_READ:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "Reading " << oid << std::endl;
      return new ReadOp(&context, oid, m_stats);

    case TEST_OP_WRITE:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "Writing " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new WriteOp(&context, oid, m_stats);

    case TEST_OP_DELETE:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << "Deleting " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new DeleteOp(&context, oid, m_stats);

    case TEST_OP_SNAP_CREATE:
      cout << "Snapping" << std::endl;
      return new SnapCreateOp(&context, m_stats);

    case TEST_OP_SNAP_REMOVE:
      if (context.snaps.empty()) {
	return NULL;
      } else {
	int snap = rand_choose(context.snaps)->first;
	cout << "RemovingSnap " << snap << std::endl;
	return new SnapRemoveOp(&context, snap, m_stats);
      }

    case TEST_OP_ROLLBACK:
      if (context.snaps.empty()) {
	return NULL;
      } else {
	int snap = rand_choose(context.snaps)->first;
	string oid = *(rand_choose(context.oid_not_in_use));
	cout << "RollingBack " << oid << " to " << snap << std::endl;
	m_nextop = new ReadOp(&context, oid, m_stats);
        return new RollbackOp(&context, oid, snap);
      }

    default:
      cerr << "Invalid op type " << type << std::endl;
      assert(0);
    }
  }

  TestOp *m_nextop;
  int m_op;
  int m_ops;
  int m_objects;
  TestOpStat *m_stats;
  map<TestOpType, unsigned int> m_weight_sums;
  unsigned int m_total_weight;
};

int main(int argc, char **argv)
{
  int ops = 1000;
  int objects = 50;
  int max_in_flight = 16;
  uint64_t size = 4000000; // 4 MB
  uint64_t min_stride_size, max_stride_size;

  const int NUM_OP_TYPES = 6;
  TestOpType op_types[NUM_OP_TYPES] = {
    TEST_OP_READ, TEST_OP_WRITE, TEST_OP_DELETE,
    TEST_OP_SNAP_CREATE, TEST_OP_SNAP_REMOVE, TEST_OP_ROLLBACK
  };

  map<TestOpType, unsigned int> op_weights;

  for (int i = 0; i < NUM_OP_TYPES; ++i) {
    if (argc > i + 1) {
      int weight = atoi(argv[i + 1]);
      if (weight < 0) {
	cerr << "Weights must be nonnegative." << std::endl;
	return 1;
      }
      cout << "adding op weight " << op_types[i] << " -> " << weight << std::endl;
      op_weights.insert(pair<TestOpType, unsigned int>(op_types[i], weight));
    }
  }

  if (argc > 1 + NUM_OP_TYPES) {
    ops = atoi(argv[1 + NUM_OP_TYPES]);
  }

  if (argc > 2 + NUM_OP_TYPES) {
    objects = atoi(argv[2 + NUM_OP_TYPES]);
  }

  if (argc > 3 + NUM_OP_TYPES) {
    max_in_flight = atoi(argv[3 + NUM_OP_TYPES]);
  }

  if (argc > 4 + NUM_OP_TYPES) {
    size = atoi(argv[4 + NUM_OP_TYPES]);
  }

  if (argc > 5 + NUM_OP_TYPES) {
    min_stride_size = atoi(argv[5 + NUM_OP_TYPES]);
  } else {
    min_stride_size = size / 10;
  }

  if (argc > 6 + NUM_OP_TYPES) {
    max_stride_size = atoi(argv[6 + NUM_OP_TYPES]);
  } else {
    max_stride_size = size / 5;
  }

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

  if (max_in_flight > objects) {
    cerr << "Error: max_in_flight must be less than the number of objects"
	 << std::endl;
    return 1;
  }

  char *id = getenv("CEPH_CLIENT_ID");
  string pool_name = "data";
  VarLenGenerator cont_gen(size, min_stride_size, max_stride_size);
  RadosTestContext context(pool_name, max_in_flight, cont_gen, id);

  TestOpStat stats;
  WeightedTestGenerator gen = WeightedTestGenerator(ops, objects,
						    op_weights, &stats);
  context.loop(&gen);

  context.shutdown();
  cerr << context.errors << " errors." << std::endl;
  cerr << stats << std::endl;
  return 0;
}
