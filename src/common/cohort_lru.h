// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 CohortFS, LLC.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef COHORT_LRU_H
#define COHORT_LRU_H

#include <stdint.h>
#include <atomic>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/rbtree.hpp>
#include <boost/intrusive/avltree.hpp>
#include <mutex>
#include <atomic>
#include <vector>
#include <algorithm>
#include <iostream>
#include "common/likely.h"

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64 /* XXX arch-specific define */
#endif
#define CACHE_PAD(_n) char __pad ## _n [CACHE_LINE_SIZE]

namespace cohort {

  namespace lru {

    namespace bi = boost::intrusive;

    /* public flag values */
    constexpr uint32_t FLAG_NONE = 0x0000;
    constexpr uint32_t FLAG_INITIAL = 0x0001;

    enum class Edge : std::uint8_t
    {
      MRU = 0,
      LRU
    };

    typedef bi::link_mode<bi::safe_link> link_mode; // for debugging

    class Object
    {
    private:
      uint32_t lru_flags;
      std::atomic<uint32_t> lru_refcnt;
      std::atomic<uint32_t> lru_adj;
      bi::list_member_hook< link_mode > lru_hook;

      typedef bi::list<Object,
		       bi::member_hook<
			 Object, bi::list_member_hook< link_mode >,
			 &Object::lru_hook >,
		       bi::constant_time_size<true>> Queue;

    public:

      Object() : lru_flags(FLAG_NONE), lru_refcnt(0), lru_adj(0) {}

      uint32_t get_refcnt() const { return lru_refcnt; }

      virtual bool reclaim() = 0;

      virtual ~Object() {}

    private:
      template <typename LK>
      friend class LRU;
    };

    /* allocator & recycler interface (create or re-use LRU objects) */
    class ObjectFactory
    {
    public:
      virtual Object* alloc(void) = 0;
      virtual void recycle(Object*) = 0;
      virtual ~ObjectFactory() {};
    };

    template <typename LK>
    class LRU
    {
    private:

      struct Lane {
	LK lock;
	Object::Queue q;
	// Object::Queue pinned; /* placeholder for possible expansion */
	CACHE_PAD(0);
	Lane() {}
      };

      Lane *qlane;
      int n_lanes;
      std::atomic<uint32_t> evict_lane;
      const uint32_t lane_hiwat;

      static constexpr uint32_t lru_adj_modulus = 5;

      static constexpr uint32_t SENTINEL_REFCNT = 1;

      /* internal flag values */
      static constexpr uint32_t FLAG_INLRU = 0x0001;
      static constexpr uint32_t FLAG_PINNED  = 0x0002; // possible future use
      static constexpr uint32_t FLAG_EVICTING = 0x0004;

      Lane& lane_of(void* addr) {
	return qlane[(uint64_t)(addr) % n_lanes];
      }

      uint32_t next_evict_lane() {
	return (evict_lane++ % n_lanes);
      }

      bool can_reclaim(Object* o) {
	return ((o->lru_refcnt == SENTINEL_REFCNT) &&
		(!(o->lru_flags & FLAG_EVICTING)));
      }

      Object* evict_block() {
	uint32_t lane_ix = next_evict_lane();
	for (int ix = 0; ix < n_lanes; ++ix,
	       lane_ix = next_evict_lane()) {
	  Lane& lane = qlane[lane_ix];
	  lane.lock.lock();
	  /* if object at LRU has refcnt==1, it may be reclaimable */
	  Object* o = &(lane.q.back());
#if 0 /* XXX save for refactor */
	  std::cout << __func__
		    << " " << o
		    << " refcnt: " << o->lru_refcnt
		    << std::endl;
#endif
	  if (can_reclaim(o)) {
	    ++(o->lru_refcnt);
	    o->lru_flags |= FLAG_EVICTING;
	    lane.lock.unlock();
	    if (o->reclaim()) {
	      lane.lock.lock();
	      --(o->lru_refcnt);
	      /* assertions that o state has not changed across
	       * relock */
	      assert(o->lru_refcnt == SENTINEL_REFCNT);
	      assert(o->lru_flags & FLAG_INLRU);
	      Object::Queue::iterator it =
		Object::Queue::s_iterator_to(*o);
	      lane.q.erase(it);
	      lane.lock.unlock();
	      return o;
	    } else {
	      // XXX can't make unreachable (means what?)
	      --(o->lru_refcnt);
	      o->lru_flags &= ~FLAG_EVICTING;
	      /* unlock in next block */
	    }
	  } /* can_reclaim(o) */
	  lane.lock.unlock();
	} /* each lane */
	return nullptr;
      } /* evict_block */

    public:

      LRU(int lanes, uint32_t _hiwat)
	: n_lanes(lanes), evict_lane(0), lane_hiwat(_hiwat)
	  {
	    assert(n_lanes > 0);
	    qlane = new Lane[n_lanes];
	  }

      ~LRU() { delete[] qlane; }

      bool ref(Object* o, uint32_t flags) {
	++(o->lru_refcnt);
	if (flags & FLAG_INITIAL) {
	  if ((++(o->lru_adj) % lru_adj_modulus) == 0) {
	    Lane& lane = lane_of(o);
	    lane.lock.lock();
	    /* move to MRU */
	    Object::Queue::iterator it =
	      Object::Queue::s_iterator_to(*o);
	    lane.q.erase(it);
	    lane.q.push_front(*o);
	    lane.lock.unlock();
	  } /* adj */
	} /* initial ref */
	return true;
      } /* ref */

      void unref(Object* o, uint32_t flags) {
	uint32_t refcnt = --(o->lru_refcnt);
	if (unlikely(refcnt == 0)) {
	  Lane& lane = lane_of(o);
	  lane.lock.lock();
	  refcnt = o->lru_refcnt.load();
	  if (unlikely(refcnt == 0)) {
	    Object::Queue::iterator it =
	      Object::Queue::s_iterator_to(*o);
	    lane.q.erase(it);
	    delete o;
	  }
	  lane.lock.unlock();
	} else if (unlikely(refcnt == SENTINEL_REFCNT)) {
	  Lane& lane = lane_of(o);
	  lane.lock.lock();
	  refcnt = o->lru_refcnt.load();
	  if (likely(refcnt == SENTINEL_REFCNT)) {
	    /* move to LRU */
	    Object::Queue::iterator it =
	      Object::Queue::s_iterator_to(*o);
	    lane.q.erase(it);
	    /* hiwat check */
	    if (lane.q.size() > lane_hiwat) {
	      delete o;
	    } else {
	      lane.q.push_back(*o);
	    }
	  }
	  lane.lock.unlock();
	}
      } /* unref */

      Object* insert(ObjectFactory* fac, Edge edge, uint32_t flags) {
	/* use supplied functor to re-use an evicted object, or
	 * allocate a new one of the descendant type */
	Object* o = evict_block();
	if (o)
	  fac->recycle(o); /* recycle existing object */
	else
	  o = fac->alloc(); /* get a new one */

	o->lru_flags = FLAG_INLRU;

	Lane& lane = lane_of(o);
	lane.lock.lock();
	switch (edge) {
	case Edge::MRU:
	  lane.q.push_front(*o);
	  break;
	case Edge::LRU:
	  lane.q.push_back(*o);
	  break;
	default:
	  abort();
	  break;
	}
	if (flags & FLAG_INITIAL)
	  o->lru_refcnt += 2; /* sentinel ref + initial */
	else
	  ++(o->lru_refcnt); /* sentinel */
	lane.lock.unlock();
	return o;
      } /* insert */

    };

    template <typename T, typename TTree, typename CLT, typename CEQ,
	      typename K, typename LK>
    class TreeX
    {
    public:

      static constexpr uint32_t FLAG_NONE = 0x0000;
      static constexpr uint32_t FLAG_LOCK = 0x0001;
      static constexpr uint32_t FLAG_UNLOCK = 0x0002;
      static constexpr uint32_t FLAG_UNLOCK_ON_MISS = 0x0004;

      typedef T value_type;
      typedef TTree container_type;
      typedef typename TTree::iterator iterator;
      typedef std::pair<iterator, bool> check_result;
      typedef typename TTree::insert_commit_data insert_commit_data;
      int n_part;
      int csz;

      typedef std::unique_lock<LK> unique_lock;

      struct Partition {
	LK lock;
	TTree tr;
	T** cache;
	int csz;
	CACHE_PAD(0);

	Partition() : tr(), cache(nullptr), csz(0) {}

	~Partition() {
	  if (csz)
	    ::operator delete(cache);
	}
      };

      struct Latch {
	Partition* p;
	LK* lock;
	insert_commit_data commit_data;

	Latch() : p(nullptr), lock(nullptr) {}
      };

      Partition& partition_of_scalar(uint64_t x) {
	return part[x % n_part];
      }

      Partition& get(uint8_t x) {
	return part[x];
      }

      Partition*& get() {
	return part;
      }

      void lock() {
	std::for_each(locks.begin(), locks.end(),
		      [](LK* lk){ lk->lock(); });
      }

      void unlock() {
	std::for_each(locks.begin(), locks.end(),
		      [](LK* lk){ lk->unlock(); });
      }

      TreeX(int n_part=1, int csz=127) : n_part(n_part), csz(csz) {
	assert(n_part > 0);
	part = new Partition[n_part];
	for (int ix = 0; ix < n_part; ++ix) {
	  Partition& p = part[ix];
	  if (csz) {
	    p.csz = csz;
	    p.cache = (T**) ::operator new(csz * sizeof(T*));
	    memset(p.cache, 0, csz * sizeof(T*));
	  }
	  locks.push_back(&p.lock);
	}
      }

      ~TreeX() {
	delete[] part;
      }

      T* find(uint64_t hk, const K& k, uint32_t flags) {
	T* v;
	Latch lat;
	uint32_t slot = 0;
	lat.p = &(partition_of_scalar(hk));
	if (flags & FLAG_LOCK) {
	  lat.lock = &lat.p->lock;
	  lat.lock->lock();
	}
	if (csz) { /* template specialize? */
	  slot = hk % csz;
	  v = lat.p->cache[slot];
	  if (v) {
	    if (CEQ()(*v, k)) {
	      if (flags & FLAG_LOCK)
		lat.lock->unlock();
	      return v;
	    }
	    v = nullptr;
	  }
	} else {
	  v = nullptr;
	}
	iterator it = lat.p->tr.find(k, CLT());
	if (it != lat.p->tr.end()){
	  v = &(*(it));
	  if (csz) {
	    /* fill cache slot at hk */
	    lat.p->cache[slot] = v;
	  }
	}
	if (flags & FLAG_LOCK)
	  lat.lock->unlock();
	return v;
      } /* find */

      T* find_latch(uint64_t hk, const K& k, Latch& lat,
		    uint32_t flags) {
	uint32_t slot = 0;
	T* v;
	lat.p = &(partition_of_scalar(hk));
	lat.lock = &lat.p->lock;
	if (flags & FLAG_LOCK)
	  lat.lock->lock();
	if (csz) { /* template specialize? */
	  slot = hk % csz;
	  v = lat.p->cache[slot];
	  if (v) {
	    if (CEQ()(*v, k)) {
	      if ((flags & FLAG_LOCK) && (flags & FLAG_UNLOCK))
		lat.lock->unlock();
	      return v;
	    }
	    v = nullptr;
	  }
	} else {
	  v = nullptr;
	}
	check_result r = lat.p->tr.insert_unique_check(
	  k, CLT(), lat.commit_data);
	if (! r.second /* !insertable (i.e., !found) */) {
	  v = &(*(r.first));
	  if (csz) {
	    /* fill cache slot at hk */
	    lat.p->cache[slot] = v;
	  }
	}
	if ((flags & FLAG_LOCK) && (flags & FLAG_UNLOCK))
	  lat.lock->unlock();
	return v;
      } /* find_latch */

      void insert_latched(T* v, Latch& lat, uint32_t flags) {
	(void) lat.p->tr.insert_unique_commit(*v, lat.commit_data);
	if (flags & FLAG_UNLOCK)
	  lat.lock->unlock();
      } /* insert_latched */

      void insert(uint64_t hk, T* v, uint32_t flags) {
	Partition& p = partition_of_scalar(hk);
	if (flags & FLAG_LOCK)
	  p.lock.lock();
	p.tr.insert_unique(*v);
	if (flags & FLAG_LOCK)
	  p.lock.unlock();
      } /* insert */

      void remove(uint64_t hk, T* v, uint32_t flags) {
	Partition& p = partition_of_scalar(hk);
	iterator it = TTree::s_iterator_to(*v);
	if (flags & FLAG_LOCK)
	  p.lock.lock();
	p.tr.erase(it);
	if (csz) { /* template specialize? */
	  uint32_t slot = hk % csz;
	  T* v2 = p.cache[slot];
	  /* we are intrusive, just compare addresses */
	  if (v == v2)
	    p.cache[slot] = nullptr;
	}
	if (flags & FLAG_LOCK)
	  p.lock.unlock();
      } /* remove */

      void drain(std::function<void(T*)> uref,
		 uint32_t flags = FLAG_NONE) {
	/* clear a table, call supplied function on
	 * each element found (e.g., retuns sentinel
	 * references) */
	for (int t_ix = 0; t_ix < n_part; ++t_ix) {
	  Partition& p = part[t_ix];
	  if (flags & FLAG_LOCK) /* LOCKED */
	    p.lock.lock();
	  while (p.tr.size() > 0) {
	    iterator it = p.tr.begin();
	    T* v = &(*it);
	    p.tr.erase(it); /* must precede uref(v), in
			     * safe_link mode */
	    uref(v);
	  }
	  if (flags & FLAG_LOCK) /* we locked it, !LOCKED */
	    p.lock.unlock();
	} /* each partition */
      } /* drain */

    private:
      Partition *part;
      std::vector<LK*> locks;
    };

  } /* namespace LRU */
} /* namespace cohort */

#endif /* COHORT_LRU_H */
