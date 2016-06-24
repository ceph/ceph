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

#include "common/Formatter.h"
#include "common/OpQueue.h"

#include <map>
#include <list>

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
class PrioritizedQueue : public OpQueue <T, K> {
  int64_t total_priority;
  int64_t max_tokens_per_subqueue;
  int64_t min_cost;

  typedef std::list<std::pair<unsigned, T> > ListPairs;
  static unsigned filter_list_pairs(
    ListPairs *l,
    std::function<bool (const T&)> f) {
    unsigned ret = 0;
    for (typename ListPairs::iterator i = l->end();
	 i != l->begin();
      ) {
      auto next = i;
      --next;
      if (f(next->second)) {
	++ret;
	l->erase(next);
      } else {
	i = next;
      }
    }
    return ret;
  }

  struct SubQueue {
  private:
    typedef std::map<K, ListPairs> Classes;
    Classes q;
    unsigned tokens, max_tokens;
    int64_t size;
    typename Classes::iterator cur;
  public:
    SubQueue(const SubQueue &other)
      : q(other.q),
	tokens(other.tokens),
	max_tokens(other.max_tokens),
	size(other.size),
	cur(q.begin()) {}
    SubQueue()
      : tokens(0),
	max_tokens(0),
	size(0), cur(q.begin()) {}
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
      if (tokens > max_tokens) {
	tokens = max_tokens;
      }
    }
    void take_tokens(unsigned t) {
      if (tokens > t) {
	tokens -= t;
      } else {
	tokens = 0;
      }
    }
    void enqueue(K cl, unsigned cost, T item) {
      q[cl].push_back(std::make_pair(cost, item));
      if (cur == q.end())
	cur = q.begin();
      size++;
    }
    void enqueue_front(K cl, unsigned cost, T item) {
      q[cl].push_front(std::make_pair(cost, item));
      if (cur == q.end())
	cur = q.begin();
      size++;
    }
    std::pair<unsigned, T> front() const {
      assert(!(q.empty()));
      assert(cur != q.end());
      return cur->second.front();
    }
    void pop_front() {
      assert(!(q.empty()));
      assert(cur != q.end());
      cur->second.pop_front();
      if (cur->second.empty()) {
	q.erase(cur++);
      } else {
	++cur;
      }
      if (cur == q.end()) {
	cur = q.begin();
      }
      size--;
    }
    unsigned length() const {
      assert(size >= 0);
      return (unsigned)size;
    }
    bool empty() const {
      return q.empty();
    }
    void remove_by_filter(
      std::function<bool (const T&)> f) {
      for (typename Classes::iterator i = q.begin();
	   i != q.end();
	   ) {
	size -= filter_list_pairs(&(i->second), f);
	if (i->second.empty()) {
	  if (cur == i) {
	    ++cur;
	  }
	  q.erase(i++);
	} else {
	  ++i;
	}
      }
      if (cur == q.end())
	cur = q.begin();
    }
    void remove_by_class(K k, std::list<T> *out) {
      typename Classes::iterator i = q.find(k);
      if (i == q.end()) {
	return;
      }
      size -= i->second.size();
      if (i == cur) {
	++cur;
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
      if (cur == q.end()) {
	cur = q.begin();
      }
    }

    void dump(ceph::Formatter *f) const {
      f->dump_int("tokens", tokens);
      f->dump_int("max_tokens", max_tokens);
      f->dump_int("size", size);
      f->dump_int("num_keys", q.size());
      if (!empty()) {
	f->dump_int("first_item_cost", front().first);
      }
    }
  };

  typedef std::map<unsigned, SubQueue> SubQueues;
  SubQueues high_queue;
  SubQueues queue;

  SubQueue *create_queue(unsigned priority) {
    typename SubQueues::iterator p = queue.find(priority);
    if (p != queue.end()) {
      return &p->second;
    }
    total_priority += priority;
    SubQueue *sq = &queue[priority];
    sq->set_max_tokens(max_tokens_per_subqueue);
    return sq;
  }

  void remove_queue(unsigned priority) {
    assert(queue.count(priority));
    queue.erase(priority);
    total_priority -= priority;
    assert(total_priority >= 0);
  }

  void distribute_tokens(unsigned cost) {
    if (total_priority == 0) {
      return;
    }
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ++i) {
      i->second.put_tokens(((i->first * cost) / total_priority) + 1);
    }
  }

public:
  PrioritizedQueue(unsigned max_per, unsigned min_c)
    : total_priority(0),
      max_tokens_per_subqueue(max_per),
      min_cost(min_c)
  {}

  unsigned length() const override final {
    unsigned total = 0;
    for (typename SubQueues::const_iterator i = queue.begin();
	 i != queue.end();
	 ++i) {
      assert(i->second.length());
      total += i->second.length();
    }
    for (typename SubQueues::const_iterator i = high_queue.begin();
	 i != high_queue.end();
	 ++i) {
      assert(i->second.length());
      total += i->second.length();
    }
    return total;
  }

  void remove_by_filter(
      std::function<bool (const T&)> f) override final {
    for (typename SubQueues::iterator i = queue.begin();
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
    for (typename SubQueues::iterator i = high_queue.begin();
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

  void remove_by_class(K k, std::list<T> *out = 0) override final {
    for (typename SubQueues::iterator i = queue.begin();
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
    for (typename SubQueues::iterator i = high_queue.begin();
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

  void enqueue_strict(K cl, unsigned priority, T item) override final {
    high_queue[priority].enqueue(cl, 0, item);
  }

  void enqueue_strict_front(K cl, unsigned priority, T item) override final {
    high_queue[priority].enqueue_front(cl, 0, item);
  }

  void enqueue(K cl, unsigned priority, unsigned cost, T item) override final {
    if (cost < min_cost)
      cost = min_cost;
    if (cost > max_tokens_per_subqueue)
      cost = max_tokens_per_subqueue;
    create_queue(priority)->enqueue(cl, cost, item);
  }

  void enqueue_front(K cl, unsigned priority, unsigned cost, T item) override final {
    if (cost < min_cost)
      cost = min_cost;
    if (cost > max_tokens_per_subqueue)
      cost = max_tokens_per_subqueue;
    create_queue(priority)->enqueue_front(cl, cost, item);
  }

  bool empty() const override final {
    assert(total_priority >= 0);
    assert((total_priority == 0) || !(queue.empty()));
    return queue.empty() && high_queue.empty();
  }

  T dequeue() override final {
    assert(!empty());

    if (!(high_queue.empty())) {
      T ret = high_queue.rbegin()->second.front().second;
      high_queue.rbegin()->second.pop_front();
      if (high_queue.rbegin()->second.empty()) {
	high_queue.erase(high_queue.rbegin()->first);
      }
      return ret;
    }

    // if there are multiple buckets/subqueues with sufficient tokens,
    // we behave like a strict priority queue among all subqueues that
    // are eligible to run.
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ++i) {
      assert(!(i->second.empty()));
      if (i->second.front().first < i->second.num_tokens()) {
	T ret = i->second.front().second;
	unsigned cost = i->second.front().first;
	i->second.take_tokens(cost);
	i->second.pop_front();
	if (i->second.empty()) {
	  remove_queue(i->first);
	}
	distribute_tokens(cost);
	return ret;
      }
    }

    // if no subqueues have sufficient tokens, we behave like a strict
    // priority queue.
    T ret = queue.rbegin()->second.front().second;
    unsigned cost = queue.rbegin()->second.front().first;
    queue.rbegin()->second.pop_front();
    if (queue.rbegin()->second.empty()) {
      remove_queue(queue.rbegin()->first);
    }
    distribute_tokens(cost);
    return ret;
  }

  void dump(ceph::Formatter *f) const override final {
    f->dump_int("total_priority", total_priority);
    f->dump_int("max_tokens_per_subqueue", max_tokens_per_subqueue);
    f->dump_int("min_cost", min_cost);
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
