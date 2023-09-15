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

#include "include/ceph_assert.h"

namespace bi = boost::intrusive;

template <typename T, typename S>
class MapKey
{
  public:
  bool operator()(const S i, const T &k) const
  {
    return i < k.key;
  }
  bool operator()(const T &k, const S i) const
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
        unsigned cost;
        T item;
        ListPair(unsigned c, T&& i) :
          cost(c),
          item(std::move(i))
	{}
    };
    class Klass : public bi::set_base_hook<>
    {
      typedef bi::list<ListPair> ListPairs;
      typedef typename ListPairs::iterator Lit;
      public:
        K key;		// klass
        ListPairs lp;
        Klass(K& k) :
          key(k) {
        }
        ~Klass() {
          lp.clear_and_dispose(DelItem<ListPair>());
        }
      friend bool operator< (const Klass &a, const Klass &b)
        { return a.key < b.key; }
      friend bool operator> (const Klass &a, const Klass &b)
        { return a.key > b.key; }
      friend bool operator== (const Klass &a, const Klass &b)
        { return a.key == b.key; }
      void insert(unsigned cost, T&& item, bool front) {
        if (front) {
          lp.push_front(*new ListPair(cost, std::move(item)));
        } else {
          lp.push_back(*new ListPair(cost, std::move(item)));
        }
      }
      //Get the cost of the next item to dequeue
      unsigned get_cost() const {
        ceph_assert(!empty());
        return lp.begin()->cost;
      }
      T pop() {
	ceph_assert(!lp.empty());
	T ret = std::move(lp.begin()->item);
        lp.erase_and_dispose(lp.begin(), DelItem<ListPair>());
        return ret;
      }
      bool empty() const {
        return lp.empty();
      }
      unsigned get_size() const {
	return lp.size();
      }
      void filter_class(std::list<T>* out) {
        for (Lit i = --lp.end();; --i) {
          if (out) {
            out->push_front(std::move(i->item));
          }
          i = lp.erase_and_dispose(i, DelItem<ListPair>());
          if (i == lp.begin()) {
            break;
          }
        }
      }
    };
    class SubQueue : public bi::set_base_hook<>
    {
      typedef bi::rbtree<Klass> Klasses;
      typedef typename Klasses::iterator Kit;
      void check_end() {
        if (next == klasses.end()) {
          next = klasses.begin();
        }
      }
      public:
	unsigned key;	// priority
        Klasses klasses;
	Kit next;
	SubQueue(unsigned& p) :
	  key(p),
	  next(klasses.begin()) {
	}
	~SubQueue() {
	  klasses.clear_and_dispose(DelItem<Klass>());
	}
      friend bool operator< (const SubQueue &a, const SubQueue &b)
        { return a.key < b.key; }
      friend bool operator> (const SubQueue &a, const SubQueue &b)
        { return a.key > b.key; }
      friend bool operator== (const SubQueue &a, const SubQueue &b)
        { return a.key == b.key; }
      bool empty() const {
        return klasses.empty();
      }
      void insert(K cl, unsigned cost, T&& item, bool front = false) {
        typename Klasses::insert_commit_data insert_data;
      	std::pair<Kit, bool> ret =
          klasses.insert_unique_check(cl, MapKey<Klass, K>(), insert_data);
      	if (ret.second) {
      	  ret.first = klasses.insert_unique_commit(*new Klass(cl), insert_data);
          check_end();
	}
	ret.first->insert(cost, std::move(item), front);
      }
      unsigned get_cost() const {
        ceph_assert(!empty());
        return next->get_cost();
      }
      T pop() {
        T ret = next->pop();
        if (next->empty()) {
          next = klasses.erase_and_dispose(next, DelItem<Klass>());
        } else {
	  ++next;
	}
        check_end();
	return ret;
      }
      void filter_class(K& cl, std::list<T>* out) {
        Kit i = klasses.find(cl, MapKey<Klass, K>());
        if (i != klasses.end()) {
          i->filter_class(out);
	  Kit tmp = klasses.erase_and_dispose(i, DelItem<Klass>());
	  if (next == i) {
            next = tmp;
          }
          check_end();
        }
      }
      // this is intended for unit tests and should be never used on hot paths
      unsigned get_size_slow() const {
	unsigned count = 0;
	for (const auto& klass : klasses) {
	  count += klass.get_size();
	}
	return count;
      }
      void dump(ceph::Formatter *f) const {
        f->dump_int("num_keys", next->get_size());
        if (!empty()) {
          f->dump_int("first_item_cost", next->get_cost());
        }
      }
    };
    class Queue {
      typedef bi::rbtree<SubQueue> SubQueues;
      typedef typename SubQueues::iterator Sit;
      SubQueues queues;
      unsigned total_prio;
      unsigned max_cost;
      public:
	Queue() :
	  total_prio(0),
	  max_cost(0) {
	}
	~Queue() {
	  queues.clear_and_dispose(DelItem<SubQueue>());
	}
	bool empty() const {
	  return queues.empty();
	}
	void insert(unsigned p, K cl, unsigned cost, T&& item, bool front = false) {
	  typename SubQueues::insert_commit_data insert_data;
      	  std::pair<typename SubQueues::iterator, bool> ret =
      	    queues.insert_unique_check(p, MapKey<SubQueue, unsigned>(), insert_data);
      	  if (ret.second) {
      	    ret.first = queues.insert_unique_commit(*new SubQueue(p), insert_data);
	    total_prio += p;
      	  }
	  ret.first->insert(cl, cost, std::move(item), front);
	  if (cost > max_cost) {
	    max_cost = cost;
	  }
	}
	T pop(bool strict = false) {
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
	      // Find the priority corresponding to the picked number.
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
	void filter_class(K& cl, std::list<T>* out) {
	  for (Sit i = queues.begin(); i != queues.end();) {
	    i->filter_class(cl, out);
	    if (i->empty()) {
	      total_prio -= i->key;
	      i = queues.erase_and_dispose(i, DelItem<SubQueue>());
	    } else {
	      ++i;
	    }
	  }
	}
	// this is intended for unit tests and should be never used on hot paths
	unsigned get_size_slow() const {
	  unsigned count = 0;
	  for (const auto& queue : queues) {
	    count += queue.get_size_slow();
	  }
	  return count;
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
    void remove_by_class(K cl, std::list<T>* removed = 0) final {
      strict.filter_class(cl, removed);
      normal.filter_class(cl, removed);
    }
    bool empty() const final {
      return strict.empty() && normal.empty();
    }
    void enqueue_strict(K cl, unsigned p, T&& item) final {
      strict.insert(p, cl, 0, std::move(item));
    }
    void enqueue_strict_front(K cl, unsigned p, T&& item) final {
      strict.insert(p, cl, 0, std::move(item), true);
    }
    void enqueue(K cl, unsigned p, unsigned cost, T&& item) final {
      normal.insert(p, cl, cost, std::move(item));
    }
    void enqueue_front(K cl, unsigned p, unsigned cost, T&& item) final {
      normal.insert(p, cl, cost, std::move(item), true);
    }
    T dequeue() override {
      ceph_assert(!empty());
      if (!strict.empty()) {
	return strict.pop(true);
      }
      return normal.pop();
    }
    unsigned get_size_slow() {
      return strict.get_size_slow() + normal.get_size_slow();
    }
    void dump(ceph::Formatter *f) const override {
      f->open_array_section("high_queues");
      strict.dump(f);
      f->close_section();
      f->open_array_section("queues");
      normal.dump(f);
      f->close_section();
    }

    void print(std::ostream &ostream) const final {
      ostream << get_op_queue_type_name(get_type());
    }

    op_queue_type_t get_type() const final {
      return op_queue_type_t::WeightedPriorityQueue;
    }
};

#endif
