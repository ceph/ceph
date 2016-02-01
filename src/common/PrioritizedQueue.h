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
#include "common/Formatter.h"
#include "include/unordered_map.h"
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
  int64_t max_tokens_per_subqueue;
  int64_t min_cost;

  typedef std::list<std::pair<unsigned, T> > ListPairs;
  template <class F>
  static unsigned filter_list_pairs(
    ListPairs *l, F f,
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
    typedef ceph::unordered_map<K, ListPairs> Classes;
    Classes q;
    unsigned tokens, max_tokens;
    int64_t size;
    K cur_key;
    typename Classes::iterator cur;
  public:
    SubQueue(const SubQueue &other)
      : q(other.q),
	tokens(other.tokens),
	max_tokens(other.max_tokens),
	size(other.size),
        cur_key(other.cur_key),
	cur(q.begin()) {}
    SubQueue()
      : tokens(0),
	max_tokens(0),
	size(0), cur(q.begin()) {
      if (cur != q.end())
        cur_key = cur->first;
    }
    void set_max_tokens(unsigned mt) {
      max_tokens = mt;
    }
    unsigned get_max_tokens() const {
      return max_tokens;
    }
    unsigned num_tokens() const {
      return tokens;
    }
    void put_tokens(unsigned t) {
      tokens += t;
      if (tokens > max_tokens)
	tokens = max_tokens;
    }
    void take_tokens(unsigned t) {
      if (tokens > t)
	tokens -= t;
      else
	tokens = 0;
    }
    void enqueue(const K &cl, unsigned cost, const T &item) {
      int bucket_count = q.bucket_count();
      q[cl].push_back(std::make_pair(cost, item));
      if (cur == q.end()) {
	cur = q.begin();
        cur_key = cur->first;
      }
      else if (q.bucket_count() != bucket_count) {
        cur = q.find(cur_key);
        assert(cur != q.end());
      }
      size++;
    }
    void enqueue_front(const K &cl, unsigned cost, const T &item) {
      int bucket_count = q.bucket_count();
      q[cl].push_front(std::make_pair(cost, item));
      if (cur == q.end()) {
	cur = q.begin();
        cur_key = cur->first;
      }
      else if (q.bucket_count() != bucket_count) {
        cur = q.find(cur_key);
        assert(cur != q.end());
      }
      size++;
    }
    std::pair<unsigned, T>& front() const {
      assert(!(q.empty()));
      assert(cur != q.end());
      return cur->second.front();
    }
    void pop_front() {
      assert(!(q.empty()));
      assert(cur != q.end());
      cur->second.pop_front();
      if (cur->second.empty())
  	cur = q.erase(cur);
      else
	++cur;
      if (cur == q.end())
	cur = q.begin();
      if (cur != q.end())
        cur_key = cur->first;
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
    void remove_by_filter(F f, std::list<T> *out) {
      for (typename Classes::iterator i = q.begin();
	   i != q.end();
	   ) {
	size -= filter_list_pairs(&(i->second), f, out);
	if (i->second.empty()) {
	  if (cur == i) {
	    ++cur;
            if (cur == q.end())
	      cur = q.begin();
            if (cur != q.end())
              cur_key = cur->first;
          }
	  q.erase(i++);
	} else {
	  ++i;
	}
      }
      cur = q.find(cur_key);
    }
    void remove_by_class(K k, std::list<T> *out) {
      typename Classes::iterator i = q.find(k);
      if (i == q.end())
	return;
      size -= i->second.size();
      if (i == cur) {
	++cur;
        if (cur == q.end())
	  cur = q.begin();
        if (cur != q.end())
          cur_key = cur->first;
      }
      if (out) {
	for (typename ListPairs::reverse_iterator j =
	       i->second.rbegin();
	     j != i->second.rend();
	     ++j) {
	  out->push_front(j->second);
	}
      }
      q.erase(i);
      cur = q.find(cur_key);
    }

    void dump(Formatter *f) const {
      f->dump_int("tokens", tokens);
      f->dump_int("max_tokens", max_tokens);
      f->dump_int("size", size);
      f->dump_int("num_keys", q.size());
      if (!empty())
	f->dump_int("first_item_cost", front().first);
    }
  };
  std::set<unsigned> high_queue_keys;
  std::set<unsigned> queue_keys;
  std::vector<SubQueue> high_queue;
  std::vector<SubQueue> queue;

  SubQueue *create_queue(unsigned priority) {
    typename std::set<unsigned>::iterator p = queue_keys.find(priority);
    if (p != queue_keys.end()) {
      return &queue[priority];
    }
    queue_keys.insert(priority);
    total_priority += priority;
    if (priority >= queue.size()) {
      queue.resize(priority + 1);
    }
    SubQueue *sq = &queue[priority];
    sq->set_max_tokens(max_tokens_per_subqueue);
    return sq;
  }

  void remove_queue(unsigned priority) {
    assert(queue_keys.count(priority));
    queue_keys.erase(priority);
    total_priority -= priority;
    assert(total_priority >= 0);
  }

  void remove_queue(std::set<unsigned>::iterator iter) {
    assert(iter != queue_keys.end());
    queue_keys.erase(iter);
    total_priority -= *iter;
    assert(total_priority >= 0);
  }

  void distribute_tokens(unsigned cost) {
    if (total_priority == 0)
      return;
    for (typename std::set<unsigned>::iterator i = queue_keys.begin();
	 i != queue_keys.end();
	 ++i) {
      queue[*i].put_tokens(((*i * cost) / total_priority) + 1);
    }
  }

public:
  PrioritizedQueue(unsigned max_per, unsigned min_c)
    : total_priority(0),
      max_tokens_per_subqueue(max_per),
      min_cost(min_c)
  {}

  unsigned length() const {
    unsigned total = 0;
    for (typename std::set<unsigned>::const_iterator i = queue_keys.begin();
	 i != queue_keys.end();
	 ++i) {
      assert(queue[*i].length());
      total += queue[*i].length();
    }
    for (typename std::set<unsigned>::const_iterator i = high_queue_keys.begin();
	 i != high_queue_keys.end();
	 ++i) {
      assert(high_queue[*i].length());
      total += high_queue[*i].length();
    }
    return total;
  }

  template <class F>
  void remove_by_filter(F f, std::list<T> *removed = 0) {
    for (typename std::set<unsigned>::iterator i = queue_keys.begin();
	 i != queue_keys.end();
	 ) {
      queue[*i].remove_by_filter(f, removed);
      if (queue[*i].empty()) {
	remove_queue(i++);
      } else {
	++i;
      }
    }
    for (typename std::set<unsigned>::iterator i = high_queue_keys.begin();
	 i != high_queue_keys.end();
	 ) {
      high_queue[*i].remove_by_filter(f, removed);
      if (high_queue[*i].empty()) {
        high_queue_keys.erase(i++);
      } else {
        ++i;
      }
    }
  }

  void remove_by_class(K k, std::list<T> *out = 0) {
    for (typename std::set<unsigned>::iterator i = queue_keys.begin();
	 i != queue_keys.end();
	 ) {
      queue[*i].remove_by_class(k, out);
      if (queue[*i].empty()) {
	remove_queue(i++);
      } else {
	++i;
      }
    }
    for (typename std::set<unsigned>::iterator i = high_queue_keys.begin();
	 i != high_queue_keys.end();
	 ) {
      high_queue[*i].remove_by_class(k, out);
      if (high_queue[*i].empty()) {
        high_queue_keys.erase(i++);
      } else {
        ++i;
      }
    }
  }

  void enqueue_strict(const K &cl, unsigned priority, const T &item) {
    high_queue_keys.insert(priority);
    if (priority >= high_queue.size()) {
      high_queue.resize(priority + 1);
    }
    high_queue[priority].enqueue(cl, 0, item);
  }

  void enqueue_strict_front(const K &cl, unsigned priority, const T &item) {
    high_queue_keys.insert(priority);
    if (priority >= high_queue.size()) {
      high_queue.resize(priority + 1);
    }
    high_queue[priority].enqueue_front(cl, 0, item);
  }

  void enqueue(const K &cl, unsigned priority, unsigned cost, const T &item) {
    if (cost < min_cost)
      cost = min_cost;
    if (cost > max_tokens_per_subqueue)
      cost = max_tokens_per_subqueue;
    create_queue(priority)->enqueue(cl, cost, item);
  }

  void enqueue_front(const K &cl, unsigned priority, unsigned cost, const T &item) {
    if (cost < min_cost)
      cost = min_cost;
    if (cost > max_tokens_per_subqueue)
      cost = max_tokens_per_subqueue;
    create_queue(priority)->enqueue_front(cl, cost, item);
  }

  bool empty() const {
    assert(total_priority >= 0);
    assert((total_priority == 0) || !(queue_keys.empty()));
    return queue_keys.empty() && high_queue_keys.empty();
  }

  T dequeue() {
    assert(!empty());

    if (!(high_queue_keys.empty())) {
      unsigned priority = *high_queue_keys.rbegin();
      T ret = high_queue[priority].front().second;
      high_queue[priority].pop_front();
      if (high_queue[priority].empty()) {
        high_queue_keys.erase(priority);
      }
      return ret;
    }

    // if there are multiple buckets/subqueues with sufficient tokens,
    // we behave like a strict priority queue among all subqueues that
    // are eligible to run.
    for (typename std::set<unsigned>::iterator i = queue_keys.begin();
	 i != queue_keys.end();
	 ++i) {
      SubQueue &sq = queue[*i];
      assert(!(sq.empty()));
      if (sq.front().first < sq.num_tokens()) {
	T ret = sq.front().second;
	unsigned cost = sq.front().first;
	sq.take_tokens(cost);
	sq.pop_front();
	if (sq.empty())
	  remove_queue(i);
	distribute_tokens(cost);
	return ret;
      }
    }

    // if no subqueues have sufficient tokens, we behave like a strict
    // priority queue.
    unsigned priority = *(--queue_keys.end());
    SubQueue &sq = queue[priority];
    T ret = sq.front().second;
    unsigned cost = sq.front().first;
    sq.pop_front();
    if (sq.empty())
      remove_queue(--queue_keys.end());
    distribute_tokens(cost);
    return ret;
  }

  void dump(Formatter *f) const {
    f->dump_int("total_priority", total_priority);
    f->dump_int("max_tokens_per_subqueue", max_tokens_per_subqueue);
    f->dump_int("min_cost", min_cost);
    f->open_array_section("high_queues");
    for (typename std::set<unsigned>::const_iterator p = high_queue_keys.begin();
	 p != high_queue_keys.end();
	 ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", *p);
      high_queue[*p].dump(f);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("queues");
    for (typename std::set<unsigned>::const_iterator p = queue_keys.begin();
	 p != queue_keys.end();
	 ++p) {
      f->open_object_section("subqueue");
      f->dump_int("priority", *p);
      queue[*p].dump(f);
      f->close_section();
    }
    f->close_section();
  }
};

#endif
