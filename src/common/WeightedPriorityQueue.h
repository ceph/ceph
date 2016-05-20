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
        ListPair(unsigned c, T& i) :
          cost(c),
          item(i)
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
          key(k)
          {}
      friend bool operator< (const Klass &a, const Klass &b)
        { return a.key < b.key; }
      friend bool operator> (const Klass &a, const Klass &b)
        { return a.key > b.key; }
      friend bool operator== (const Klass &a, const Klass &b)
        { return a.key == b.key; }
      void insert(unsigned cost, T& item, bool front) {
        if (front) {
          lp.push_front(*new ListPair(cost, item));
        } else {
          lp.push_back(*new ListPair(cost, item));
        }
      }
      //Get the cost of the next item to dequeue
      unsigned get_cost() const {
	return lp.begin()->cost;
      }
      T pop() {
	assert(!lp.empty());
	T ret = lp.begin()->item;
        lp.erase_and_dispose(lp.begin(), DelItem<ListPair>());
        return ret;
      }
      bool empty() const {
        return lp.empty();
      }
      unsigned get_size() const {
	return lp.size();
      }
      unsigned filter_list_pairs(std::function<bool (T)>& f) {
        unsigned count = 0;
        // intrusive containers can't erase with a reverse_iterator
        // so we have to walk backwards on our own. Since there is
        // no iterator before begin, we have to test at the end.
        for (Lit i = --lp.end();; --i) {
          if (f(i->item)) {
            i = lp.erase_and_dispose(i, DelItem<ListPair>());
            ++count;
          }
          if (i == lp.begin()) {
            break;
          }
        }
        return count;
      }
      unsigned filter_class(std::list<T>* out) {
        unsigned count = 0;
        for (Lit i = --lp.end();; --i) {
          if (out) {
            out->push_front(i->item);
          }
          i = lp.erase_and_dispose(i, DelItem<ListPair>());
          ++count;
          if (i == lp.begin()) {
            break;
          }
        }
        return count;
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
	  next(klasses.begin())
	  {}
      friend bool operator< (const SubQueue &a, const SubQueue &b)
        { return a.key < b.key; }
      friend bool operator> (const SubQueue &a, const SubQueue &b)
        { return a.key > b.key; }
      friend bool operator== (const SubQueue &a, const SubQueue &b)
        { return a.key == b.key; }
      bool empty() const {
        return klasses.empty();
      }
      void insert(K cl, unsigned cost, T& item, bool front = false) {
        typename Klasses::insert_commit_data insert_data;
      	std::pair<Kit, bool> ret =
          klasses.insert_unique_check(cl, MapKey<Klass, K>(), insert_data);
      	if (ret.second) {
      	  ret.first = klasses.insert_unique_commit(*new Klass(cl), insert_data);
          check_end();
	}
	ret.first->insert(cost, item, front);
      }
      unsigned get_cost() const {
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
      unsigned filter_list_pairs(std::function<bool (T)>& f) {
	unsigned count = 0;
        // intrusive containers can't erase with a reverse_iterator
        // so we have to walk backwards on our own. Since there is
        // no iterator before begin, we have to test at the end.
        for (Kit i = klasses.begin(); i != klasses.end();) {
          count += i->filter_list_pairs(f);
          if (i->empty()) {
	    if (next == i) {
	      ++next;
	    }
            i = klasses.erase_and_dispose(i, DelItem<Klass>());
          } else {
            ++i;
          }
        }
        check_end();
	return count;
      }
      unsigned filter_class(K& cl, std::list<T>* out) {
	unsigned count = 0;
        Kit i = klasses.find(cl, MapKey<Klass, K>());
        if (i != klasses.end()) {
          count = i->filter_class(out);
	  Kit tmp = klasses.erase_and_dispose(i, DelItem<Klass>());
	  if (next == i) {
            next = tmp;
          }
          check_end();
        }
	return count;
      }
      void dump(ceph::Formatter *f) const {
	f->dump_int("num_keys", next->get_size());
	f->dump_int("first_item_cost", next->get_cost());
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
	void insert(unsigned p, K cl, unsigned cost, T& item, bool front = false) {
	  typename SubQueues::insert_commit_data insert_data;
      	  std::pair<typename SubQueues::iterator, bool> ret =
      	    queues.insert_unique_check(p, MapKey<SubQueue, unsigned>(), insert_data);
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
       void filter_list_pairs(std::function<bool (T)>& f) {
	  for (Sit i = queues.begin(); i != queues.end();) {
            size -= i->filter_list_pairs(f);
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
    void remove_by_filter(std::function<bool (T)> f) override final {
      strict.filter_list_pairs(f);
      normal.filter_list_pairs(f);
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
