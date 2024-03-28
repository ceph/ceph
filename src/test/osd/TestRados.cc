// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
			bool balance_reads,
			bool localize_reads,
			bool set_redirect,
			bool set_chunk,
			bool enable_dedup) :
    m_nextop(NULL), m_op(0), m_ops(ops), m_seconds(max_seconds),
    m_objects(objects), m_stats(stats),
    m_total_weight(0),
    m_ec_pool(ec_pool),
    m_balance_reads(balance_reads),
    m_localize_reads(localize_reads),
    m_set_redirect(set_redirect),
    m_set_chunk(set_chunk),
    m_enable_dedup(enable_dedup)
  {
    m_start = time(0);
    for (map<TestOpType, unsigned int>::const_iterator it = op_weights.begin();
	 it != op_weights.end();
	 ++it) {
      m_total_weight += it->second;
      m_weight_sums.insert(pair<TestOpType, unsigned int>(it->first,
							  m_total_weight));
    }
    if (m_set_redirect || m_set_chunk) {
      if (m_set_redirect) {
	m_ops = ops+m_objects+m_objects;
      } else {
	/* create 10 chunks per an object*/
	m_ops = ops+m_objects+m_objects*10;
      }
    }
  }

  TestOp *next(RadosTestContext &context) override
  {
    TestOp *retval = NULL;

    ++m_op;
    if (m_op <= m_objects && !m_set_redirect && !m_set_chunk ) {
      stringstream oid;
      oid << m_op;
      /*if (m_op % 2) {
	// make it a long name
	oid << " " << string(300, 'o');
	}*/
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
    
    if (m_set_redirect || m_set_chunk) {
      if (init_extensible_tier(context, retval)) {
	return retval;
      }
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

  bool init_extensible_tier(RadosTestContext &context, TestOp *& op) {
    /*
     * set-redirect or set-chunk test (manifest test)
     * 0. make default objects (using create op)
     * 1. set-redirect or set-chunk
     * 2. initialize target objects (using write op)
     * 3. wait for set-* completion
     */
    int copy_manifest_end = 0;
    if (m_set_chunk) {
      copy_manifest_end = m_objects*2;
    } else {
      copy_manifest_end = m_objects*3;
    }
    int make_manifest_end = copy_manifest_end;
    if (m_set_chunk) {
      /* make 10 chunks per an object*/
      make_manifest_end = make_manifest_end + m_objects * 10;
    } else {
      /* redirect */
      make_manifest_end = make_manifest_end + m_objects;
    }

    if (m_op <= m_objects) {
      stringstream oid;
      oid << m_op;
      /*if (m_op % 2) {
	oid << " " << string(300, 'o');
	}*/
      cout << m_op << ": write initial oid " << oid.str() << std::endl;
      context.oid_not_flushing.insert(oid.str());
      if (m_ec_pool) {
	op = new WriteOp(m_op, &context, oid.str(), true, true);
      } else {
	op = new WriteOp(m_op, &context, oid.str(), false, true);
      }
      return true;
    } else if (m_op <= copy_manifest_end) {
	stringstream oid, oid2;
	//int _oid = m_op-m_objects;
	int _oid = m_op % m_objects + 1;
	oid << _oid;
	/*if ((_oid) % 2) {
	  oid << " " << string(300, 'o');
	  }*/

        if (context.oid_in_use.count(oid.str())) {
          /* previous write is not finished */
          op = NULL;
          m_op--;
          cout << m_op << " wait for completion of write op! " << std::endl;
          return true;
        }

	int _oid2 = m_op - m_objects + 1;
	if (_oid2 > copy_manifest_end - m_objects) {
	  _oid2 -= (copy_manifest_end - m_objects);
	}
	oid2 << _oid2 << " " << context.low_tier_pool_name;
	if ((_oid2) % 2) {
	  oid2 << " " << string(300, 'm');
	}
	cout << m_op << ": " << "copy oid " << oid.str() << " target oid " 
	      << oid2.str() << std::endl;
	op = new CopyOp(m_op, &context, oid.str(), oid2.str(), context.low_tier_pool_name);
	return true;
    } else if (m_op <= make_manifest_end) {
      if (m_set_redirect) {
	stringstream oid, oid2;
	int _oid = m_op-copy_manifest_end;
	oid << _oid;
	/*if ((_oid) % 2) {
	  oid << " " << string(300, 'o');
	  }*/
	oid2 << _oid << " " << context.low_tier_pool_name;
	if ((_oid) % 2) {
	  oid2 << " " << string(300, 'm');
	}
	if (context.oid_in_use.count(oid.str())) {
	  /* previous copy is not finished */
	  op = NULL;
	  m_op--;
	  cout << m_op << " retry set_redirect !" << std::endl;
	  return true;
	}
	cout << m_op << ": " << "set_redirect oid " << oid.str() << " target oid " 
	      << oid2.str() << std::endl;
	op = new SetRedirectOp(m_op, &context, oid.str(), oid2.str(), context.pool_name);
	return true;
      } else if (m_set_chunk) {
	stringstream oid;
	int _oid = m_op % m_objects +1;
	oid << _oid;
	/*if ((_oid) % 2) {
	  oid << " " << string(300, 'o');
	  }*/
	if (context.oid_in_use.count(oid.str())) {
	  /* previous set-chunk is not finished */
	  op = NULL;
	  m_op--;
	  cout << m_op << " retry set_chunk !" << std::endl;
	  return true;
	}
	stringstream oid2;
	oid2 << _oid << " " << context.low_tier_pool_name;
	if ((_oid) % 2) {
	  oid2 << " " << string(300, 'm');
	}

	cout << m_op << ": " << "set_chunk oid " << oid.str() 
	     <<  " target oid " << oid2.str()  << std::endl;
	op = new SetChunkOp(m_op, &context, oid.str(), oid2.str(), m_stats);
	return true;
      }
    } else if (m_op == make_manifest_end + 1) {
      int set_size = context.oid_not_in_use.size();
      int set_manifest_size = context.oid_redirect_not_in_use.size();
      cout << m_op << " oid_not_in_use " << set_size << " oid_redirect_not_in_use " << set_manifest_size <<  std::endl;
      /* wait for redirect or set_chunk initialization */
      if (set_size != m_objects || set_manifest_size != 0) {
	op = NULL;
	m_op--;
	cout << m_op << " wait for manifest initialization " << std::endl;
	return true;
      }
      for (int t_op = m_objects+1; t_op <= m_objects*2; t_op++) {
	stringstream oid;
	oid << t_op << " " << context.low_tier_pool_name;
	if (t_op % 2) {
	  oid << " " << string(300, 'm');
	}
	cout << " redirect_not_in_use: " << oid.str() << std::endl;
	context.oid_redirect_not_in_use.insert(oid.str());
      }
    } 

    return false;
  }

private:

  TestOp *gen_op(RadosTestContext &context, TestOpType type)
  {
    string oid, oid2;
    ceph_assert(context.oid_not_in_use.size());

    switch (type) {
    case TEST_OP_READ:
      oid = *(rand_choose(context.oid_not_in_use));
      return new ReadOp(m_op, &context, oid, m_balance_reads, m_localize_reads,
			m_stats);

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

    case TEST_OP_WRITESAME:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "writesame oid "
	   << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new WriteSameOp(m_op, &context, oid, m_stats);

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

    case TEST_OP_CHUNK_READ:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "chunk read oid " << oid << " target oid " << oid2 << std::endl;
      return new ChunkReadOp(m_op, &context, oid, context.pool_name, false, m_stats);

    case TEST_OP_TIER_PROMOTE:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "tier_promote oid " << oid << std::endl;
      return new TierPromoteOp(m_op, &context, oid, m_stats);

    case TEST_OP_TIER_FLUSH:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "tier_flush oid " << oid << std::endl;
      return new TierFlushOp(m_op, &context, oid, m_stats);

    case TEST_OP_SET_REDIRECT:
      oid = *(rand_choose(context.oid_not_in_use));
      oid2 = *(rand_choose(context.oid_redirect_not_in_use));
      cout << m_op << ": " << "set_redirect oid " << oid << " target oid " << oid2 << std::endl;
      return new SetRedirectOp(m_op, &context, oid, oid2, context.pool_name, m_stats);

    case TEST_OP_UNSET_REDIRECT:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "unset_redirect oid " << oid << std::endl;
      return new UnsetRedirectOp(m_op, &context, oid, m_stats);

    case TEST_OP_SET_CHUNK:
      {
	ceph_assert(m_enable_dedup);
	oid = *(rand_choose(context.oid_not_in_use));
	cout << m_op << ": " << "set_chunk oid " << oid 
	     <<  " target oid " << std::endl;
	return new SetChunkOp(m_op, &context, oid, "", m_stats);
      }

    case TEST_OP_TIER_EVICT:
      oid = *(rand_choose(context.oid_not_in_use));
      cout << m_op << ": " << "tier_evict oid " << oid << std::endl;
      return new TierEvictOp(m_op, &context, oid, m_stats);

    default:
      cerr << m_op << ": Invalid op type " << type << std::endl;
      ceph_abort();
      return nullptr;
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
  bool m_localize_reads;
  bool m_set_redirect;
  bool m_set_chunk;
  bool m_enable_dedup;
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
    { TEST_OP_WRITESAME, "writesame", false },
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
    { TEST_OP_SET_REDIRECT, "set_redirect", true },
    { TEST_OP_UNSET_REDIRECT, "unset_redirect", true },
    { TEST_OP_CHUNK_READ, "chunk_read", true },
    { TEST_OP_TIER_PROMOTE, "tier_promote", true },
    { TEST_OP_TIER_FLUSH, "tier_flush", true },
    { TEST_OP_SET_CHUNK, "set_chunk", true },
    { TEST_OP_TIER_EVICT, "tier_evict", true },
    { TEST_OP_READ /* grr */, NULL },
  };

  struct {
    const char *name;
  } chunk_algo_types[] = {
    { "fastcdc" },
    { "fixcdc" },
  };

  map<TestOpType, unsigned int> op_weights;
  string pool_name = "rbd";
  string low_tier_pool_name = "";
  bool ec_pool = false;
  bool no_omap = false;
  bool no_sparse = false;
  bool balance_reads = false;
  bool localize_reads = false;
  bool set_redirect = false;
  bool set_chunk = false;
  bool enable_dedup = false;
  string chunk_algo = "";
  string chunk_size = "";
  size_t max_attr_len = 20000;


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
    else if (strcmp(argv[i], "--no-sparse") == 0)
      no_sparse = true;
    else if (strcmp(argv[i], "--balance-reads") == 0)
      balance_reads = true;
    else if (strcmp(argv[i], "--localize-reads") == 0)
      localize_reads = true;
    else if (strcmp(argv[i], "--pool-snaps") == 0)
      pool_snaps = true;
    else if (strcmp(argv[i], "--write-fadvise-dontneed") == 0)
      write_fadvise_dontneed = true;
    else if (strcmp(argv[i], "--max-attr-len") == 0)
      max_attr_len = atoi(argv[++i]);
    else if (strcmp(argv[i], "--ec-pool") == 0) {
      if (!op_weights.empty()) {
	cerr << "--ec-pool must be specified prior to any ops" << std::endl;
	exit(1);
      }
      ec_pool = true;
      no_omap = true;
      no_sparse = true;
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
    } else if (strcmp(argv[i], "--set_redirect") == 0) {
      set_redirect = true;
    } else if (strcmp(argv[i], "--set_chunk") == 0) {
      set_chunk = true;
    } else if (strcmp(argv[i], "--low_tier_pool") == 0) {
      /*
       * disallow redirect or chunk object into the same pool
       * to prevent the race. see https://github.com/ceph/ceph/pull/20096
       */
      low_tier_pool_name = argv[++i];
    } else if (strcmp(argv[i], "--enable_dedup") == 0) {
      enable_dedup = true;
    } else if (strcmp(argv[i], "--dedup_chunk_algo") == 0) {
      i++;
      if (i == argc) {
        cerr << "Missing chunking algorithm after --dedup_chunk_algo" << std::endl;
        return 1;
      }
      int j;
      for (j = 0; chunk_algo_types[j].name; ++j) {
	if (strcmp(chunk_algo_types[j].name, argv[i]) == 0) {
	  break;
	}
      }
      if (!chunk_algo_types[j].name) {
	cerr << "unknown op " << argv[i] << std::endl;
	exit(1);
      }
      chunk_algo = chunk_algo_types[j].name;
    } else if (strcmp(argv[i], "--dedup_chunk_size") == 0) {
      chunk_size = argv[++i];
    } else {
      cerr << "unknown arg " << argv[i] << std::endl;
      exit(1);
    }
  }

  if (set_redirect || set_chunk) {
    if (low_tier_pool_name == "") {
      cerr << "low_tier_pool is needed" << std::endl;
      exit(1);
    }
  }

  if (enable_dedup) {
    if (chunk_algo == "" || chunk_size == "") {
      cerr << "Missing chunking algorithm: " << chunk_algo 
	   << " or chunking size: " << chunk_size << std::endl;
      exit(1);
    }
  }

  if (op_weights.empty()) {
    cerr << "No operations specified" << std::endl;
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

  if (min_stride_size >= max_stride_size) {
    cerr << "Error: max_stride_size must be more than min_stride_size"
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
    no_sparse,
    pool_snaps,
    write_fadvise_dontneed,
    low_tier_pool_name,
    enable_dedup,
    chunk_algo,
    chunk_size,
    max_attr_len,
    id);

  TestOpStat stats;
  WeightedTestGenerator gen = WeightedTestGenerator(
    ops, objects,
    op_weights, &stats, max_seconds,
    ec_pool, balance_reads, localize_reads,
    set_redirect, set_chunk, enable_dedup);
  int r = context.init();
  if (r < 0) {
    cerr << "Error initializing rados test context: "
	 << cpp_strerror(r) << std::endl;
    exit(1);
  }
  context.loop(&gen);
  if (enable_dedup) {
    if (!context.check_chunks_refcount(context.low_tier_io_ctx, context.io_ctx)) {
      cerr << " Invalid refcount " << std::endl;
      exit(1);
    }
  }

  context.shutdown();
  cerr << context.errors << " errors." << std::endl;
  cerr << stats << std::endl;
  return 0;
}
