// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once


#include <functional>
#include <map>
#include <list>
#include <cmath>

#include "common/Formatter.h"
#include "common/OpQueue.h"
#include "dmclock_server.h"


namespace ceph {

  namespace dmc = crimson::dmclock;

  enum class osd_op_type_t {
    client, osd_subop, bg_snaptrim, bg_recovery, bg_scrub };

  template <typename T, typename K>
  class mClockQueue : public OpQueue <T, K> {

    using priority_t = unsigned;
    using cost_t = unsigned;

    typedef std::list<std::pair<cost_t, T> > ListPairs;

    static unsigned filter_list_pairs(ListPairs *l,
				      std::function<bool (T)> f) {
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
      // client-class to ordered queue
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

      void enqueue(K cl, cost_t cost, T item) {
	q[cl].push_back(std::make_pair(cost, item));
	if (cur == q.end())
	  cur = q.begin();
	size++;
      }

      void enqueue_front(K cl, cost_t cost, T item) {
	q[cl].push_front(std::make_pair(cost, item));
	if (cur == q.end())
	  cur = q.begin();
	size++;
      }

      std::pair<cost_t, T> front() const {
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
	std::function<bool (T)> f) {
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
	f->dump_int("size", size);
	f->dump_int("num_keys", q.size());
      }
    };

    using SubQueues = std::map<priority_t, SubQueue>;

    SubQueues high_queue;

    crimson::dmclock::PullPriorityQueue<osd_op_type_t,T> queue;

    // when enqueue_front is called, rather than try to re-calc tags
    // to put in mClock priority queue, we'll just keep a separate
    // list from which we dequeue items first, and only when it's
    // empty do we use queue.
    std::list<std::pair<K,T>> queue_front;

    static double cost_to_tag(unsigned cost) {
      static const double log_of_2 = std::log(2.0);
      return std::log(cost) / log_of_2;
    }

  public:

    
    dmc::ClientInfo client_info_f(const osd_op_type_t& client) {
      static dmc::ClientInfo _default(1.0, 1.0, 1.0);
      return _default;
    }

    mClockQueue() :
      queue(std::bind(&mClockQueue::client_info_f, this, std::placeholders::_1),
	    true)
    {
      // empty
    }

    unsigned length() const override final {
      unsigned total = 0;
      total += queue_front.size();
      total += queue.request_count();
      for (auto i = high_queue.cbegin(); i != high_queue.cend(); ++i) {
	assert(i->second.length());
	total += i->second.length();
      }
      return total;
    }

    void remove_by_filter(std::function<bool (T)> f) override final {
#if 0 // REPLACE
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
#endif
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
#if 0 // REPLACE
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
#endif
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
      double tag_cost = cost_to_tag(cost);
      osd_op_type_t op_type = osd_op_type_t::client;
      queue.add_request(item, op_type, tag_cost);
    }

    void enqueue_front(K cl, unsigned priority, unsigned cost, T item) override final {
      queue_front.emplace_front(std::pair<K,T>(cl, item));
    }

    bool empty() const override final {
      return queue.empty() && high_queue.empty() && queue_front.empty();
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

      if (!queue_front.empty()) {
	T ret = queue_front.front().second;
	queue_front.pop_front();
	return ret;
      }

      auto pr = queue.pull_request();
      assert(pr.is_retn());
      auto& retn = pr.get_retn();
      return *(retn.request);
    }

    void dump(ceph::Formatter *f) const override final {
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

      f->open_object_section("queue_front");
      f->dump_int("size", queue_front.size());
      f->close_section();

      f->open_object_section("queue");
      f->dump_int("size", queue.request_count());
      f->close_section();
    }
  };

} // namespace ceph
