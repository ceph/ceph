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

#ifndef WP_QUEUE_H
#define WP_QUEUE_H

#include "OpQueue.h"

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/rbtree.hpp>
#include <boost/intrusive/avl_set.hpp>

namespace bi = boost::intrusive;

template <typename T>
class MapKey
{
  public:
  bool operator()(const unsigned i, const T &k) const
  {
    return i < k.key;
  }
  bool operator()(const T &k, const unsigned i) const
  {
    return k.key < i;
  }
};

template <typename T>
class DelItem
{
  public:
  void operator()(T* delete_this)
    { delete delete_this; }
};

template <typename T, typename K>
class WeightedPriorityQueue :  public OpQueue <T, K>
{
  private:
    class ListPair : public bi::list_base_hook<>
    {
      public:
	K klass;
        unsigned cost;
        T item;
        ListPair(K& k, unsigned c, T& i) :
	  klass(k),
          cost(c),
          item(i)
          {}
    };
    class SubQueue : public bi::set_base_hook<>
    {
      typedef bi::list<ListPair> QueueItems;
      typedef typename QueueItems::iterator QI;
      public:
	unsigned key;	// priority
	QueueItems qitems;
	SubQueue(unsigned& p) :
	  key(p)
	  {}
      bool empty() const {
        return qitems.empty();
      }
      void insert(K& cl, unsigned cost, T& item, bool front = false) {
	if (front) {
	  qitems.push_front(*new ListPair(cl, cost, item));
	} else {
	  qitems.push_back(*new ListPair(cl, cost, item));
	}
      }
      unsigned get_cost() const {
	return qitems.begin()->cost;
      }
      T pop() {
	T ret = qitems.begin()->item;
	qitems.erase_and_dispose(qitems.begin(), DelItem<ListPair>());
	return ret;
      }
      unsigned filter_list_pairs(std::function<bool (T)>& f, std::list<T>* out) {
	unsigned count = 0;
        // intrusive containers can't erase with a reverse_iterator
        // so we have to walk backwards on our own. Since there is
        // no iterator before begin, we have to test at the end.
        for (QI i = --qitems.end();; --i) {
          if (f(i->item)) {
            if (out) {
              out->push_front(i->item);
            }
            i = qitems.erase_and_dispose(i, DelItem<ListPair>());
            ++count;
          }
          if (i == qitems.begin()) {
            break;
          }
        }
	return count;
      }
      unsigned filter_class(K& cl, std::list<T>* out) {
	unsigned count = 0;
        for (QI i = --qitems.end();; --i) {
	  if (i->klass == cl) {
	    if (out) {
	      out->push_front(i->item);
	    }
	    i = qitems.erase_and_dispose(i, DelItem<ListPair>());
	    ++count;
	  }
	  if (i == qitems.begin()) {
	    break;
	  }
        }
	return count;
      }
      void dump(ceph::Formatter *f) const {
	f->dump_int("num_keys", qitems.size());
	f->dump_int("first_item_cost", qitems.begin()->cost);
      }
    };
    class Queue {
      typedef bi::rbtree<SubQueue> SubQueues;
      typedef typename SubQueues::iterator Sit;
      SubQueues queues;
      unsigned total_prio;
      unsigned max_cost;
      public:
	unsigned size;
	Queue() :
	  total_prio(0),
	  max_cost(0),
	  size(0)
	  {}
	bool empty() const {
	  return !size;
	}
	void insert(unsigned p, K& cl, unsigned cost, T& item, bool front = false) {
	  typename SubQueues::insert_commit_data insert_data;
      	  std::pair<typename SubQueues::iterator, bool> ret =
      	    queues.insert_unique_check(p, MapKey<SubQueue>(), insert_data);
      	  if (ret.second) {
      	    ret.first = queues.insert_unique_commit(*new SubQueue(p), insert_data);
	    total_prio += p;
      	  }
      	  ret.first->insert(cl, cost, item, front);
	  if (cost > max_cost) {
	    max_cost = cost;
	  }
	  ++size;
	}
	T pop(bool strict = false) {
	  --size;
	  Sit i = --queues.end();
	  if (strict) {
	    T ret = i->pop();
	    if (i->empty()) {
	      queues.erase_and_dispose(i, DelItem<SubQueue>());
	    }
	    return ret;
	  }
	  if (queues.size() > 1) {
	    while (true) {
	      // Pick a new priority out of the total priority.
	      unsigned prio = rand() % total_prio + 1;
	      unsigned tp = total_prio - i->key;
	      // Find the priority coresponding to the picked number.
	      // Subtract high priorities to low priorities until the picked number
	      // is more than the total and try to dequeue that priority.
	      // Reverse the direction from previous implementation because there is a higher
	      // chance of dequeuing a high priority op so spend less time spinning.
	      while (prio <= tp) {
		--i;
		tp -= i->key;
	      }
	      // Flip a coin to see if this priority gets to run based on cost.
	      // The next op's cost is multiplied by .9 and subtracted from the
	      // max cost seen. Ops with lower costs will have a larger value
	      // and allow them to be selected easier than ops with high costs.
	      if (max_cost == 0 || rand() % max_cost <=
		  (max_cost - ((i->get_cost() * 9) / 10))) {
		break;
	      }
	      i = --queues.end();
	    }
	  }
	  T ret = i->pop();
	  if (i->empty()) {
	    total_prio -= i->key;
	    queues.erase_and_dispose(i, DelItem<SubQueue>());
	  }
	  return ret;
	}
	void filter_list_pairs(std::function<bool (T)>& f, std::list<T>* out) {
	  for (Sit i = queues.begin(); i != queues.end();) {
      	    size -= i->filter_list_pairs(f, out);
	    if (i->empty()) {
	      total_prio -= i->key;
	      i = queues.erase_and_dispose(i, DelItem<SubQueue>());
	    } else {
	      ++i;
	    }
      	  }
	}
	void filter_class(K& cl, std::list<T>* out) {
	  for (Sit i = queues.begin(); i != queues.end();) {
	    size -= i->filter_class(cl, out);
	    if (i->empty()) {
	      total_prio -= i->key;
	      i = queues.erase_and_dispose(i, DelItem<SubQueue>());
	    } else {
	      ++i;
	    }
	  }
	}
	void dump(ceph::Formatter *f) const {
	  for (typename SubQueues::const_iterator i = queues.begin();
	        i != queues.end(); ++i) {
	    f->dump_int("total_priority", total_prio);
	    f->dump_int("max_cost", max_cost);
	    f->open_object_section("subqueue");
	    f->dump_int("priority", i->key);
	    i->dump(f);
	    f->close_section();
	  }
	}
    };

    Queue strict;
    Queue normal;
  public:
    WeightedPriorityQueue(unsigned max_per, unsigned min_c) :
      strict(),
      normal()
      {
	std::srand(time(0));
      }
    unsigned length() const override final {
      return strict.size + normal.size;
    }
    void remove_by_filter(std::function<bool (T)> f, std::list<T>* removed = 0) override final {
      strict.filter_list_pairs(f, removed);
      normal.filter_list_pairs(f, removed);
    }
    void remove_by_class(K cl, std::list<T>* removed = 0) override final {
      strict.filter_class(cl, removed);
      normal.filter_class(cl, removed);
    }
    bool empty() const override final {
      return !(strict.size + normal.size);
    }
    void enqueue_strict(K cl, unsigned p, T item) override final {
      strict.insert(p, cl, 0, item);
    }
    void enqueue_strict_front(K cl, unsigned p, T item) override final {
      strict.insert(p, cl, 0, item, true);
    }
    void enqueue(K cl, unsigned p, unsigned cost, T item) override final {
      normal.insert(p, cl, cost, item);
    }
    void enqueue_front(K cl, unsigned p, unsigned cost, T item) override final {
      normal.insert(p, cl, cost, item, true);
    }
    T dequeue() {
      assert(strict.size + normal.size > 0);
      if (!strict.empty()) {
	return strict.pop(true);
      }
      return normal.pop();
    }
    void dump(ceph::Formatter *f) const {
      f->open_array_section("high_queues");
      strict.dump(f);
      f->close_section();
      f->open_array_section("queues");
      normal.dump(f);
      f->close_section();
    }
};

#endif
