// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef WRR_QUEUE_H
#define WRR_QUEUE_H

#include "common/Formatter.h"
#include "common/OpQueue.h"

#include <map>
#include <list>

/**
 * Weighted Round Robin queue with stri priority queue
 *
 * This queue attempts to be fair to all classes of
 * operations but is also weighted so that higher classes
 * get more share of the operations. It is not a strict
 * weighted round robin, but only approximated to keep
 * overhead of the algorithm low.
 */

template <typename T, typename K>
class WrrQueue : public OpQueue <T, K> {
  int64_t total_priority;

  typedef std::list<std::pair<unsigned, T> > ListPairs;
  static unsigned filter_list_pairs(
    ListPairs *l, std::function<bool (T)> f,
    std::list<T> *out) {
    unsigned ret = 0;
    if (out) {
      for (typename ListPairs::reverse_iterator i = l->rbegin();
	   i != l->rend();
	   ++i) {
	if (f(i->second)) {
	  out->push_front(i->second);
	}
      }
    }
    for (typename ListPairs::iterator i = l->begin();
	 i != l->end();
      ) {
      if (f(i->second)) {
	l->erase(i++);
	++ret;
      } else {
	++i;
      }
    }
    return ret;
  }

  struct SubQueue {
  private:
    typedef std::map<K, ListPairs> Classes;
    Classes q;
    typename Classes::iterator cur;
    unsigned q_size;
  public:
    SubQueue(const SubQueue &other)
      : q(other.q),
	cur(q.begin()),
	q_size(0) {}
    SubQueue()
      :	cur(q.begin()),
	q_size(0) {}
    void enqueue(K cl, unsigned cost, T item,
		 bool front = CEPH_OP_QUEUE_BACK) {
      if (front == CEPH_OP_QUEUE_FRONT) {
	q[cl].push_front(std::make_pair(cost, item));
      } else {
	q[cl].push_back(std::make_pair(cost, item));
      }
      if (cur == q.end()) {
	cur = q.begin();
      }
      ++q_size;
    }
    std::pair<unsigned, T> front() const {
      assert(!(q.empty()));
      assert(cur != q.end());
      assert(!(cur->second.empty()));
      return cur->second.front();
    }
    void pop_front() {
      assert(!(q.empty()));
      assert(cur != q.end());
      for (typename Classes::iterator i = q.begin(); i != q.end(); ++i) {
      }
      assert(!(cur->second.empty()));
      cur->second.pop_front();
      if (cur->second.empty()) {
	q.erase(cur++);
      } else {
	++cur;
      }
      if (cur == q.end()) {
	cur = q.begin();
      }
      --q_size;
    }
    unsigned size() const {
      return q_size;
    }
    bool empty() const {
      return (q_size == 0) ? true : false;
    }
    unsigned remove_by_filter(std::function<bool (T)> f, std::list<T> *out) {
      unsigned count = 0;
      for (typename Classes::iterator i = q.begin();
	   i != q.end();
	   ) {
	count += filter_list_pairs(&(i->second), f, out);
	if (i->second.empty()) {
	  if (cur == i) {
	    ++cur;
	  }
	  q.erase(i++);
	} else {
	  ++i;
	}
      }
      if (cur == q.end()) {
	cur = q.begin();
      }
      q_size -= count;
      return count;
    }
    unsigned remove_by_class(K k, std::list<T> *out) {
      typename Classes::iterator i = q.find(k);
      if (i == q.end()) {
	return 0;
      }
      unsigned count = i->second.size();
      q_size -= count;
      if (out) {
	for (typename ListPairs::reverse_iterator j =
	       i->second.rbegin();
	     j != i->second.rend();
	     ++j) {
	  out->push_front(j->second);
	}
      }
      if (i == cur) {
	++cur;
      }
      q.erase(i);
      if (cur == q.end()) {
	cur = q.begin();
      }
      return count;
    }

    void dump(Formatter *f) const {
      f->dump_int("num_keys", q.size());
      if (!empty()) {
	f->dump_int("first_item_cost", front().first);
      }
    }
  };

  unsigned high_size, wrr_size;
  unsigned max_cost;

  typedef std::map<unsigned, SubQueue> SubQueues;
  SubQueues high_queue;
  SubQueues queue;
  typename SubQueues::reverse_iterator dq;

  SubQueue *create_queue(unsigned priority) {
    typename SubQueues::iterator p = queue.find(priority);
    if (p != queue.end()) {
      return &p->second;
    }
    total_priority += priority;
    SubQueue *sq = &queue[priority];
    return sq;
  }

public:
  WrrQueue(unsigned max_per, unsigned min_c)
    : total_priority(0),
      high_size(0),
      wrr_size(0),
      max_cost(0),
      dq(queue.rbegin())
  {
    srand(time(0));
  }

  unsigned length() const final {
    return high_size + wrr_size;
  }

  void remove_by_filter(std::function<bool (T)> f, std::list<T> *removed = 0) final {
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ) {
      wrr_size -= i->second.remove_by_filter(f, removed);
      ++i;
    }
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      high_size -= i->second.remove_by_filter(f, removed);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void remove_by_class(K k, std::list<T> *out = 0) final {
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ) {
      wrr_size -= i->second.remove_by_class(k, out);
      ++i;
    }
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      high_size -= i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void enqueue (K cl, T item, unsigned priority, unsigned cost = 0,
		bool front = CEPH_OP_QUEUE_BACK,
		unsigned opclass = CEPH_OP_CLASS_NORMAL) final {
    switch (opclass){
      case CEPH_OP_CLASS_NORMAL :
	if (cost > max_cost) {
	  max_cost = cost;
	}
	create_queue(priority)->enqueue(cl, cost, item, front);
	++wrr_size;
	break;
      case CEPH_OP_CLASS_STRICT :
	high_queue[priority].enqueue(cl, 0, item, front);
	++high_size;
	break;
      default :
	assert(1);
    }
  }

  bool empty() const final {
    assert(total_priority >= 0);
    assert((total_priority == 0) || !(queue.empty()));
    return (high_size + wrr_size  == 0) ? true : false;
  }

  void inc_dq() {
    ++dq;
    if (dq == queue.rend()) {
      dq = queue.rbegin();
    }
  }

  T dequeue() final {
    unsigned junk;
    return dequeue(junk);
  }

  T dequeue(unsigned &missed) {
    assert(!empty());
    missed = 0;

    if (!(high_queue.empty())) {
      T ret = high_queue.rbegin()->second.front().second;
      high_queue.rbegin()->second.pop_front();
      if (high_queue.rbegin()->second.empty()) {
	high_queue.erase(high_queue.rbegin()->first);
      }
      --high_size;
      return ret;
    }
    while (true) {
      if (dq->second.size() != 0) {
	if (dq->second.size() == wrr_size) {
	  break;
	}
	else {
	  // flip coin to see if the previous prioirty gets to run again.
	  if ((rand() % total_priority) <= dq->first) {
	    if (dq->second.front().first == 0) {
	      break;
	    }
	    else {
	      // flip a coin to see if this priority gets to run based on cost
	      if ((rand() % max_cost) <=
		  (max_cost - (unsigned)((double)dq->second.front().first * 0.9))){
		break;
	      }
	    }
	  }
	}
      }
      inc_dq();
      ++missed;
    }
    T ret = dq->second.front().second;
    dq->second.pop_front();
    --wrr_size;
    return ret;
  }

  void dump(Formatter *f) const {
    f->dump_int("total_priority", total_priority);
    f->open_array_section("high_queues");
    for (typename SubQueues::const_iterator p = high_queue.begin();
	 p != high_queue.end();
	 ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", p->first);
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("queues");
    for (typename SubQueues::const_iterator p = queue.begin();
	 p != queue.end();
	 ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", p->first);
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
  }
};

#endif
