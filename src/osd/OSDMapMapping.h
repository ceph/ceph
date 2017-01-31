// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_OSDMAPMAPPING_H
#define CEPH_OSDMAPMAPPING_H

#include <vector>
#include <map>

#include "osd/osd_types.h"
#include "common/WorkQueue.h"

class OSDMap;

/// a precalculated mapping of every PG for a given OSDMap
class OSDMapMapping {
  struct PoolMapping {
    unsigned size = 0;
    unsigned pg_num = 0;
    std::vector<int32_t> table;

    size_t row_size() const {
      return
	1 + // acting_primary
	1 + // up_primary
	1 + // num acting
	1 + // num up
	size + // acting
	size;  // up
    }

    PoolMapping(int s, int p)
      : size(s),
	pg_num(p),
	table(pg_num * row_size()) {
    }

    void get(size_t ps,
	     std::vector<int> *up,
	     int *up_primary,
	     std::vector<int> *acting,
	     int *acting_primary) const {
      const int32_t *row = &table[row_size() * ps];
      if (acting_primary) {
	*acting_primary = row[0];
      }
      if (up_primary) {
	*up_primary = row[1];
      }
      if (acting) {
	acting->resize(row[2]);
	for (int i = 0; i < row[2]; ++i) {
	  (*acting)[i] = row[4 + i];
	}
      }
      if (up) {
	up->resize(row[3]);
	for (int i = 0; i < row[3]; ++i) {
	  (*up)[i] = row[4 + size + i];
	}
      }
    }

    void set(size_t ps,
	     const std::vector<int>& up,
	     int up_primary,
	     const std::vector<int>& acting,
	     int acting_primary) {
      int32_t *row = &table[row_size() * ps];
      row[0] = acting_primary;
      row[1] = up_primary;
      row[2] = acting.size();
      row[3] = up.size();
      for (int i = 0; i < row[2]; ++i) {
	row[4 + i] = acting[i];
      }
      for (int i = 0; i < row[3]; ++i) {
	row[4 + size + i] = up[i];
      }
    }
  };

  std::map<int64_t,PoolMapping> pools;
  std::vector<std::vector<pg_t>> acting_rmap;  // osd -> pg
  //unused: std::vector<std::vector<pg_t>> up_rmap;  // osd -> pg
  epoch_t epoch;
  uint64_t num_pgs = 0;

  void _init_mappings(const OSDMap& osdmap);
  void _update_range(
    const OSDMap& map,
    int64_t pool,
    unsigned pg_begin, unsigned pg_end);

  void _build_rmap(const OSDMap& osdmap);

  void _start(const OSDMap& osdmap) {
    _init_mappings(osdmap);
  }
  void _finish(const OSDMap& osdmap);

  void _dump();

  friend class ParallelOSDMapper;

public:
  void get(pg_t pgid,
	   std::vector<int> *up,
	   int *up_primary,
	   std::vector<int> *acting,
	   int *acting_primary) const {
    auto p = pools.find(pgid.pool());
    assert(p != pools.end());
    p->second.get(pgid.ps(), up, up_primary, acting, acting_primary);
  }

  const std::vector<pg_t>& get_osd_acting_pgs(unsigned osd) {
    assert(osd < acting_rmap.size());
    return acting_rmap[osd];
  }
  /* unsued
  const std::vector<pg_t>& get_osd_up_pgs(unsigned osd) {
    assert(osd < up_rmap.size());
    return up_rmap[osd];
  }
  */

  void update(const OSDMap& map);
  void update(const OSDMap& map, pg_t pgid);

  epoch_t get_epoch() const {
    return epoch;
  }

  uint64_t get_num_pgs() const {
    return num_pgs;
  }
};

/// thread pool to calculate mapping on multiple CPUs
class ParallelOSDMapper {
public:
  struct Job {
    unsigned shards = 0;
    const OSDMap *osdmap;
    OSDMapMapping *mapping;
    bool aborted = false;
    Context *onfinish = nullptr;

    Mutex lock = {"ParallelOSDMapper::Job::lock"};
    Cond cond;

    Job(const OSDMap *om, OSDMapMapping *m) : osdmap(om), mapping(m) {}
    ~Job() {
      assert(shards == 0);
    }

    void set_finish_event(Context *fin) {
      lock.Lock();
      if (shards == 0) {
	// already done.
	lock.Unlock();
	fin->complete(0);
      } else {
	// set finisher
	onfinish = fin;
	lock.Unlock();
      }
    }
    bool is_done() {
      Mutex::Locker l(lock);
      return shards == 0;
    }
    void wait() {
      Mutex::Locker l(lock);
      while (shards > 0) {
	cond.Wait(lock);
      }
    }
    void abort() {
      Context *fin = nullptr;
      {
	Mutex::Locker l(lock);
	aborted = true;
	fin = onfinish;
	onfinish = nullptr;
	while (shards > 0) {
	  cond.Wait(lock);
	}
      }
      if (fin) {
	fin->complete(-ECANCELED);
      }
    }

    void start_one() {
      Mutex::Locker l(lock);
      ++shards;
    }
    void finish_one();
  };

protected:
  CephContext *cct;

  struct item {
    Job *job;
    const OSDMap *osdmap;
    OSDMapMapping *mapping;
    int64_t pool;
    unsigned begin, end;

    item(Job *j, const OSDMap *m, OSDMapMapping *mg,
	 int64_t p, unsigned b, unsigned e)
      : job(j),
	osdmap(m),
	mapping(mg),
	pool(p),
	begin(b),
	end(e) {}
  };
  std::deque<item*> q;

  struct WQ : public ThreadPool::WorkQueue<item> {
    ParallelOSDMapper *m;

    WQ(ParallelOSDMapper *m_, ThreadPool *tp)
      : ThreadPool::WorkQueue<item>("ParallelOSDMapper::WQ", 0, 0, tp),
        m(m_) {}

    bool _enqueue(item *i) override {
      m->q.push_back(i);
      return true;
    }
    void _dequeue(item *i) override {
      ceph_abort();
    }
    item *_dequeue() override {
      while (!m->q.empty()) {
	item *i = m->q.front();
	m->q.pop_front();
	if (i->job->aborted) {
	  i->job->finish_one();
	  delete i;
	} else {
	  return i;
	}
      }
      return nullptr;
    }

    void _process(item *i, ThreadPool::TPHandle &h) override;

    void _clear() override {
      assert(_empty());
    }

    bool _empty() override {
      return m->q.empty();
    }
  } wq;

public:
  ParallelOSDMapper(CephContext *cct, ThreadPool *tp)
    : cct(cct),
      wq(this, tp) {}

  std::unique_ptr<Job> queue(
    const OSDMap& osdmap,
    OSDMapMapping *mapping,
    unsigned pgs_per_item);

  void drain() {
    wq.drain();
  }
};

#endif
