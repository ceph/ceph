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

#ifndef PRIORITY_QUEUE_H
#define PRIORITY_QUEUE_H

#include "common/Mutex.h"

#include <map>
#include <utility>
#include <list>
#include <algorithm>

/**
 * Manages queue for normal and strict priority items
 *
 * On dequeue, the queue will select the lowest priority queue
 * such that the q has bucket > cost of front queue item.
 *
 * If there is no such queue, we choose the next queue item for
 * the highest priority queue.
 *
 * Before returning a dequeued item, we place into each bucket
 * cost * (priority/total_priority) tokens.
 *
 * enqueue_strict and enqueue_strict_front queue items into queues
 * which are serviced in strict priority order before items queued
 * with enqueue and enqueue_front
 *
 * Within a priority class, we schedule round robin based on the class
 * of type K used to enqueue items.  e.g. you could use entity_inst_t
 * to provide fairness for different clients.
 */
template <typename T, typename K>
class PrioritizedQueue {
  int64_t total_priority;

  template <class F>
  static unsigned filter_list_pairs(list<pair<unsigned, T> > *l, F f) {
    unsigned ret = 0;
    for (typename list<pair<unsigned, T> >::iterator i = l->begin();
	 i != l->end();
	 ) {
      if (f(i->second)) {
	l->erase(i++);
	ret++;
      } else {
	++i;
      }
    }
    return ret;
  }

  struct SubQueue {
  private:
    map<K, list<pair<unsigned, T> > > q;
    unsigned bucket;
    int64_t size;
    typename map<K, list<pair<unsigned, T> > >::iterator cur;
  public:
    SubQueue(const SubQueue &other)
      : q(other.q), bucket(other.bucket), size(other.size),
	cur(q.begin()) {}
    SubQueue() : bucket(0), size(0), cur(q.begin()) {}
    unsigned num_tokens() const {
      return bucket;
    }
    void put_tokens(unsigned tokens) {
      bucket += tokens;
    }
    void take_tokens(unsigned tokens) {
      bucket -= tokens;
    }
    void enqueue(K cl, unsigned cost, T item) {
      q[cl].push_back(make_pair(cost, item));
      if (cur == q.end())
	cur = q.begin();
      size++;
    }
    void enqueue_front(K cl, unsigned cost, T item) {
      q[cl].push_front(make_pair(cost, item));
      if (cur == q.end())
	cur = q.begin();
      size++;
    }
    pair<unsigned, T> front() const {
      assert(!(q.empty()));
      assert(cur != q.end());
      return cur->second.front();
    }
    void pop_front() {
      assert(!(q.empty()));
      assert(cur != q.end());
      cur->second.pop_front();
      if (cur->second.empty())
	q.erase(cur++);
      else
	++cur;
      if (cur == q.end())
	cur = q.begin();
      size--;
    }
    unsigned length() const {
      assert(size >= 0);
      return (unsigned)size;
    }
    bool empty() const {
      return q.empty();
    }
    template <class F>
    void remove_by_filter(F f) {
      for (typename map<K, list<pair<unsigned, T> > >::iterator i = q.begin();
	   i != q.end();
	   ) {
	size -= filter_list_pairs(&(i->second), f);
	if (i->second.empty()) {
	  if (cur == i)
	    ++cur;
	  q.erase(i++);
	} else {
	  ++i;
	}
      }
      if (cur == q.end())
	cur = q.begin();
    }
    void remove_by_class(K k, list<T> *out) {
      typename map<K, list<pair<unsigned, T> > >::iterator i = q.find(k);
      if (i == q.end())
	return;
      size -= i->second.size();
      if (i == cur) {
	++cur;
	if (cur == q.end())
	  cur = q.begin();
      }
      if (out) {
	for (typename list<pair<unsigned, T> >::reverse_iterator j =
	       i->second.rbegin();
	     j != i->second.rend();
	     ++j) {
	  out->push_front(j->second);
	}
      }
      q.erase(i);
    }
  };
  map<unsigned, SubQueue> high_queue;
  map<unsigned, SubQueue> queue;

  SubQueue *create_queue(unsigned priority) {
    typename map<unsigned, SubQueue>::iterator p = queue.find(priority);
    if (p != queue.end())
      return &p->second;
    total_priority += priority;
    return &queue[priority];
  }

  void remove_queue(unsigned priority) {
    assert(queue.count(priority));
    queue.erase(priority);
    total_priority -= priority;
    assert(total_priority >= 0);
  }

  void distribute_tokens(unsigned cost) {
    if (total_priority == 0)
      return;
    for (typename map<unsigned, SubQueue>::iterator i = queue.begin();
	 i != queue.end();
	 ++i) {
      i->second.put_tokens(((i->first * cost) / total_priority) + 1);
    }
  }

public:
  PrioritizedQueue() : total_priority(0) {}

  unsigned length() {
    unsigned total = 0;
    for (typename map<unsigned, SubQueue>::iterator i = queue.begin();
	 i != queue.end();
	 ++i) {
      assert(i->second.length());
      total += i->second.length();
    }
    for (typename map<unsigned, SubQueue>::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ++i) {
      assert(i->second.length());
      total += i->second.length();
    }
    return total;
  }

  template <class F>
  void remove_by_filter(F f) {
    for (typename map<unsigned, SubQueue>::iterator i = queue.begin();
	 i != queue.end();
	 ) {
      unsigned priority = i->first;
      
      i->second.remove_by_filter(f);
      if (i->second.empty()) {
	++i;
	remove_queue(priority);
      } else {
	++i;
      }
    }
    for (typename map<unsigned, SubQueue>::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      i->second.remove_by_filter(f);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void remove_by_class(K k, list<T> *out = 0) {
    for (typename map<unsigned, SubQueue>::iterator i = queue.begin();
	 i != queue.end();
	 ) {
      i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	unsigned priority = i->first;
	++i;
	remove_queue(priority);
      } else {
	++i;
      }
    }
    for (typename map<unsigned, SubQueue>::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void enqueue_strict(K cl, unsigned priority, T item) {
    high_queue[priority].enqueue(cl, 0, item);
  }

  void enqueue_strict_front(K cl, unsigned priority, T item) {
    high_queue[priority].enqueue_front(cl, 0, item);
  }

  void enqueue(K cl, unsigned priority, unsigned cost, T item) {
    create_queue(priority)->enqueue(cl, cost, item);
  }

  void enqueue_front(K cl, unsigned priority, unsigned cost, T item) {
    create_queue(priority)->enqueue_front(cl, cost, item);
  }

  bool empty() {
    assert(total_priority >= 0);
    assert((total_priority == 0) || !(queue.empty()));
    return queue.empty() && high_queue.empty();
  }

  T dequeue() {
    assert(!empty());

    if (!(high_queue.empty())) {
      T ret = high_queue.rbegin()->second.front().second;
      high_queue.rbegin()->second.pop_front();
      if (high_queue.rbegin()->second.empty())
	high_queue.erase(high_queue.rbegin()->first);
      return ret;
    }

    for (typename map<unsigned, SubQueue>::iterator i = queue.begin();
	 i != queue.end();
	 ++i) {
      assert(!(i->second.empty()));
      if (i->second.front().first < i->second.num_tokens()) {
	T ret = i->second.front().second;
	unsigned cost = i->second.front().first;
	i->second.take_tokens(cost);
	i->second.pop_front();
	if (i->second.empty())
	  remove_queue(i->first);
	distribute_tokens(cost);
	return ret;
      }
    }
    T ret = queue.rbegin()->second.front().second;
    unsigned cost = queue.rbegin()->second.front().first;
    queue.rbegin()->second.pop_front();
    if (queue.rbegin()->second.empty())
      remove_queue(queue.rbegin()->first);
    distribute_tokens(cost);
    return ret;
  }
};

#endif
