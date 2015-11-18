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
  public:
    SubQueue(const SubQueue &other)
      : q(other.q),
	cur(q.begin()) {}
    SubQueue()
      :	cur(q.begin()) {}
    void enqueue(K cl, unsigned cost, T item) {
      q[cl].push_back(std::make_pair(cost, item));
      if (cur == q.end())
	cur = q.begin();
    }
    void enqueue_front(K cl, unsigned cost, T item) {
      q[cl].push_front(std::make_pair(cost, item));
      if (cur == q.end())
	cur = q.begin();
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
      if (cur->second.empty())
	q.erase(cur++);
      else
	++cur;
      if (cur == q.end())
	cur = q.begin();
    }
    unsigned length() const {
      assert(q.size() >= 0);
      return (unsigned) q.size();
    }
    bool empty() const {
      return q.empty();
    }
    unsigned remove_by_filter(std::function<bool (T)> f, std::list<T> *out) {
      unsigned count = 0;
      for (typename Classes::iterator i = q.begin();
	   i != q.end();
	   ) {
	count = filter_list_pairs(&(i->second), f, out);
	if (i->second.empty()) {
	  if (cur == i)
	    ++cur;
	} else {
	  ++i;
	}
      }
      if (cur == q.end())
	cur = q.begin();
      return count;
    }
    unsigned remove_by_class(K k, std::list<T> *out) {
      unsigned count = q.find(k)->second.size();
      typename Classes::iterator i = q.find(k);
      if (i == q.end())
	return count;
      if (i == cur)
	++cur;
      if (out) {
	for (typename ListPairs::reverse_iterator j =
	       i->second.rbegin();
	     j != i->second.rend();
	     ++j) {
	  out->push_front(j->second);
	}
      }
      if (cur == q.end())
	cur = q.begin();
      i->second.clear();
      return count;
    }

    void dump(Formatter *f) const {
      f->dump_int("num_keys", q.size());
      if (!empty())
	f->dump_int("first_item_cost", front().first);
    }
  };

  unsigned _size;

  typedef std::map<unsigned, SubQueue> SubQueues;
  SubQueues high_queue;
  SubQueues queue;
  typename SubQueues::reverse_iterator dq;

  SubQueue *create_queue(unsigned priority) {
    typename SubQueues::iterator p = queue.find(priority);
    if (p != queue.end())
      return &p->second;
    total_priority += priority;
    SubQueue *sq = &queue[priority];
    return sq;
  }

  void remove_queue(unsigned priority) {
  }

public:
  WrrQueue(unsigned max_per, unsigned min_c)
    : total_priority(0),
      _size(0),
      dq(queue.rbegin())
  {}

  unsigned length() const final {
    unsigned total = 0;
    for (typename SubQueues::const_iterator i = queue.begin();
         i != queue.end();
         ++i) {
      total += i->second.length();
    }
    for (typename SubQueues::const_iterator i = high_queue.begin();
         i != high_queue.end();
         ++i) {
      total += i->second.length();
    }
    return total;
    //return _size;
  }

  void remove_by_filter(std::function<bool (T)> f, std::list<T> *removed = 0) final {
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ) {
      _size -= i->second.remove_by_filter(f, removed);
      ++i;
    }
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      _size -= i->second.remove_by_filter(f, removed);
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
      _size -= i->second.remove_by_class(k, out);
      ++i;
    }
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      _size -= i->second.remove_by_class(k, out);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void enqueue_strict(K cl, unsigned priority, T item) final {
    high_queue[priority].enqueue(cl, 0, item);
    ++_size;
  }

  void enqueue_strict_front(K cl, unsigned priority, T item) final {
    high_queue[priority].enqueue_front(cl, 0, item);
    ++_size;
  }

  void enqueue(K cl, unsigned priority, unsigned cost, T item) final {
    create_queue(priority)->enqueue(cl, cost, item);
    ++_size;
  }

  void enqueue_front(K cl, unsigned priority, unsigned cost, T item) final {
    create_queue(priority)->enqueue_front(cl, cost, item);
    ++_size;
  }

  bool empty() const final {
    assert(total_priority >= 0);
    assert((total_priority == 0) || !(queue.empty()));
    return (_size == 0) ? true : false;
  }

  void print_queue() {
    cerr << "Number of queues: " << std::dec << queue.size() << " -:- ";
    for (typename SubQueues::reverse_iterator ri = queue.rbegin();
	ri != queue.rend(); ri++){
      cerr << ri->first << ", ";
    }
    cerr << "\n";
    cerr << "Queue pointer currently at priority " << std::dec << dq->first << ".\n";
  }

  void inc_dq() {
    ++dq;
    if (dq == queue.rend())
      dq = queue.rbegin();
  }

  T dequeue() final {
    assert(!empty());

    cerr << "Queue size: " << std::dec << _size << " == " << std::dec << length() << "\n";
    //cerr << "Dequeing OP, but first going to check the high queue...\n";
    //cerr << "High queue has " << std::dec << high_queue.size() << " ops in the queue.\n";
    if (!(high_queue.empty())) {
      //cerr << "High queue has ops, going to schedule one of them. (" << std::dec << high_queue.size() << " ops in the high_queue)\n";
      for (typename SubQueues::const_iterator i = high_queue.begin();
           i != high_queue.end();
           ++i) {
        //assert(i->second.length());
        //cerr << "High queue priority " << std::dec << i->first << " has " << std::dec << i->second.length() << " ops.\n";
      }
      T ret = high_queue.rbegin()->second.front().second;
      high_queue.rbegin()->second.pop_front();
      if (high_queue.rbegin()->second.empty())
	high_queue.erase(high_queue.rbegin()->first);
      --_size;
      return ret;
    }
    if ( dq == queue.rend()) {
      //cerr << "For some odd reason the queue pointer is at the end of the queue, going to set it to the start.\n";
      dq = queue.rbegin();
    }
    //print_queue();
    //cerr << "Dequeuing op with priority " << std::dec << dq->first << "... (" << std::dec << length() << " ops in the queue)\n";
    //cerr << "Start Subqueue length: " << std::dec << dq->second.length() << "\n";
    while (dq->second.length() == 0) {
      // Look thorugh other priorities for something to run
      //cerr << "Loop Subqueue length: " << std::dec << dq->second.length() << "\n";
      //cerr << "Didn't find an OP at priority " << std::dec << dq->first << ". Total Priorty: " << std::dec << total_priority << "\n";
      inc_dq();
    }
    T ret = dq->second.front().second;
    dq->second.pop_front();
    
    //if (dq->second.length() == 0) {
    //  remove_queue(dq->first);
    //}

    //cerr << "Going to dispatch " << std::hex << &ret << " with priority " << std::dec << dq->first << "...\n";

    // Flip a coin to see if we dequeue another of the sam priorty or move on
    if ( (rand() % total_priority) > dq->first) {
      //cerr << "Lost coin toss, next priority gets a turn.\n";
      inc_dq();
    } else {
      //cerr << "Won coin toss, I get to run again.\n";
    }
    --_size;
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
