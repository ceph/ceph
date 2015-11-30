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

#include <thread>

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
    unsigned _size;
  public:
    SubQueue(const SubQueue &other)
      : q(other.q),
	cur(q.begin()),
	_size(0) {}
    SubQueue()
      :	cur(q.begin()),
	_size(0) {}
    void enqueue(K cl, unsigned cost, T item,
		 bool front = CEPH_OP_QUEUE_BACK) {
      if (front == CEPH_OP_QUEUE_FRONT) 
	q[cl].push_front(std::make_pair(cost, item));
      else
	q[cl].push_back(std::make_pair(cost, item));
      if (cur == q.end())
	cur = q.begin();
      ++_size;
    }
//    void enqueue_front(K cl, unsigned cost, T item) {
//      q[cl].push_front(std::make_pair(cost, item));
//      if (cur == q.end())
//	cur = q.begin();
//    }
    std::pair<unsigned, T> front() const {
      assert(!(q.empty()));
      assert(cur != q.end());
      //cerr << std::hex << std::this_thread::get_id() << " front()-> class " << std::dec << cur->first << "has # ops: " << cur->second.size() << " _size: " << _size << "." << std::endl;
      assert(!(cur->second.empty()));
      return cur->second.front();
    }
    void pop_front() {
      //cur = q.begin();
      assert(!(q.empty()));
      assert(cur != q.end());
      for (typename Classes::iterator i = q.begin(); i != q.end(); ++i) {
	//cerr << std::hex << std::this_thread::get_id() << " pop_front()->Classes -- class " << std::dec << i->first << " has " << i->second.size() << " ops in the class" << std::endl;
      }
      assert(!(cur->second.empty()));
      cur->second.pop_front();
      if (cur->second.empty())
	q.erase(cur++);
      else
	++cur;
      if (cur == q.end())
	cur = q.begin();
      --_size;
    }
    unsigned size() const {
      return _size;
    }
    //unsigned length() const {
    //  assert(size >= 0);
    //  return size;
    //}
    bool empty() const {
      return (_size == 0) ? true : false;
    }
    unsigned remove_by_filter(std::function<bool (T)> f, std::list<T> *out) {
      unsigned count = 0;
      for (typename Classes::iterator i = q.begin();
	   i != q.end();
	   ) {
	count += filter_list_pairs(&(i->second), f, out);
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
      _size -= count;
      return count;
    }
    unsigned remove_by_class(K k, std::list<T> *out) {
      //cerr << std::hex << std::this_thread::get_id() << " In remove_by_class." << std::endl;
      //unsigned count = q.find(k)->second.size();
      //_size -= count;
      //cerr << std::hex << std::this_thread::get_id() << " remove_by_class find class." << std::endl;
      typename Classes::iterator i = q.find(k);
      if (i == q.end())
	return 0;
      //cerr << std::hex << std::this_thread::get_id() << " remove_by_class, decrement removed ops, current _size: " << std::dec << _size << "." << std::endl;
      unsigned count = i->second.size();
      //cerr << std::hex << std::this_thread::get_id() << " remove_by_class, going to decrement " << std::dec << count << " ops." << std::endl;
      _size -= count;
      if (out) {
	//cerr << std::hex << std::this_thread::get_id() << " remove_by_class populate return ops." << std::endl;
	for (typename ListPairs::reverse_iterator j =
	       i->second.rbegin();
	     j != i->second.rend();
	     ++j) {
	  out->push_front(j->second);
	}
      }
      //cerr << std::hex << std::this_thread::get_id() << " remove_by_class update cur." << std::endl;
      if (i == cur)
	++cur;
      //cerr << std::hex << std::this_thread::get_id() << " remove_by_class doing the delete now." << std::endl;
      q.erase(i);
      if (cur == q.end())
	cur = q.begin();
      //i->second.clear();
      //cerr << std::hex << std::this_thread::get_id() << " remove_by_class returning." << std::endl;
      return count;
    }

    void dump(Formatter *f) const {
      f->dump_int("num_keys", q.size());
      if (!empty())
	f->dump_int("first_item_cost", front().first);
    }
  };

  // For benchmarks only
  //std::chrono::time_point<std::chrono::system_clock> start, end;
  typename OpQueue<T, K>::RunningStat eq_stat, dq_stat;
  // End benchmarks

  unsigned _high_size, _wrr_size;
  //bool _fp_enable; // flag to turn on/off fast path queueing
  unsigned _max_cost;

  typedef std::map<unsigned, SubQueue> SubQueues;
  SubQueues high_queue;
  //SubQueues fifo_queue;
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
      _high_size(0),
      _wrr_size(0),
      //_fp_enable(true),
      _max_cost(0),
      dq(queue.rbegin())
  {}

  std::string get_queue() {
    std::stringstream os;
    //os << "Wrr len: " << high_queue.size << ":" << fifo_queue.size() << ":" << _wrr_size <<
    os << std::hex << std::this_thread::get_id() << " Wrr len: " << std::dec << _high_size << ":" << _wrr_size <<
      " Prios:(";
    if (high_queue.empty())
      os << "X";
    else
      for (typename SubQueues::reverse_iterator i = high_queue.rbegin();
	  i != high_queue.rend(); ++i) {
	os << i->first << ":" << i->second.size() << ", ";
      }
    os << ")(";
    //if (fifo_queue.empty())
    //  os << "-";
    //else
    //  for (typename SubQueues::reverse_iterator i = fifo_queue.rbegin();
//	  i != fifo_queue.rend(); ++i) {
//	os << i->first << "-" << i->second.size();
    //  }
    //os << ")(";
    if (queue.empty())
      os << "X";
    else
      for (typename SubQueues::reverse_iterator i = queue.rbegin();
	  i != queue.rend(); ++i) {
	os << i->first << ":" << i->second.size() << ", ";
      }
    os << ")";
    return os.str();
  }

  void eq_push(double val) {
    eq_stat.Push(val);
  }
  void dq_push(double val) {
    dq_stat.Push(val);
  }
  std::string eq_stats() {
    std::stringstream out;
    out << std::hex << std::this_thread::get_id() << " Mean: " << std::dec << eq_stat.Mean() << " ns, Std. Dev.: " << eq_stat.StandardDeviation() << " ns";
    return out.str();
  }
  std::string dq_stats() {
    std::stringstream out;
    out << std::hex << std::this_thread::get_id() << " Mean: " << std::dec << dq_stat.Mean() << " ns, Std. Dev.: " << dq_stat.StandardDeviation() << " ns";
    return out.str();
  }

  unsigned length() const final {
//    unsigned total = 0;
//    for (typename SubQueues::const_iterator i = queue.begin();
//         i != queue.end();
//         ++i) {
//      total += i->second.length();
//    }
//    for (typename SubQueues::const_iterator i = high_queue.begin();
//         i != high_queue.end();
//         ++i) {
//      total += i->second.length();
//    }
//    return total;
    //return _wrr_size + fifo_queue.size() + high_queue.size();
    return _high_size + _wrr_size;
  }

  void remove_by_filter(std::function<bool (T)> f, std::list<T> *removed = 0) final {
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ) {
      _wrr_size -= i->second.remove_by_filter(f, removed);
      ++i;
    }
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      _high_size -= i->second.remove_by_filter(f, removed);
      if (i->second.empty()) {
	high_queue.erase(i++);
      } else {
	++i;
      }
    }
  }

  void remove_by_class(K k, std::list<T> *out = 0) final {
    //cerr << std::hex << std::this_thread::get_id() << " Going to remove by class wrr_queue " << std::dec << k << " size: " << _wrr_size << "." << std::endl;
    for (typename SubQueues::iterator i = queue.begin();
	 i != queue.end();
	 ) {
      _wrr_size -= i->second.remove_by_class(k, out);
      ++i;
    }
    //cerr << std::hex << std::this_thread::get_id() << " Going to remove by class high_queue with size: " << std::dec << _high_size << "." << std::endl;
    for (typename SubQueues::iterator i = high_queue.begin();
	 i != high_queue.end();
	 ) {
      _high_size -= i->second.remove_by_class(k, out);
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
    //start = std::chrono::system_clock::now();
    //if (_fp_enable && _wrr_size + fifo_queue.size() + high_queue.size() >= 5)
    //  _fp_enable = false;
    switch (opclass){
      case CEPH_OP_CLASS_NORMAL :
	if (cost > _max_cost)
	  _max_cost = cost;
	//if (_fp_enable) {
	//  fifo_queue[priority].enqueue(cl, cost, item, front);
	//} else {
	create_queue(priority)->enqueue(cl, cost, item, front);
	++_wrr_size;
	//}
	break;
      case CEPH_OP_CLASS_STRICT :
	high_queue[priority].enqueue(cl, 0, item, front);
	++_high_size;
	break;
      default :
	assert(1);
    }
    //end = std::chrono::system_clock::now();
    //eq_stat.Push(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    //cerr << "Wrr ( " << std::this_thread::get_id() << ") enqueue stats: Mean=" << std::dec << eq_stat.Mean() << "ns, Std dev.=" << eq_stat.StandardDeviation() << " ns\n";
  }

  bool empty() const final {
    assert(total_priority >= 0);
    assert((total_priority == 0) || !(queue.empty()));
    //return (_wrr_size + fifo_queue.size() + high_queue.size()  == 0) ? true : false;
    return (_high_size + _wrr_size  == 0) ? true : false;
  }

  void print_queue() {
    cerr << std::hex << std::this_thread::get_id() << " Number of queues: " << std::dec << queue.size() <<
      ", number of ops: " << _wrr_size << " -:- ";
    for (typename SubQueues::reverse_iterator ri = queue.rbegin();
	ri != queue.rend(); ri++){
      cerr << ri->first << " {" << ri->second.size() << "}, ";
    }
    cerr << std::endl;
    cerr << std::hex << std::this_thread::get_id() << " Queue pointer currently at priority " << std::dec << dq->first << "." << std::endl;
  }

  void inc_dq() {
    ++dq;
    if (dq == queue.rend())
      dq = queue.rbegin();
  }

  T dequeue() final {
    //cerr << "Queue size: " << std::dec << _wrr_size << " == " << std::dec << length() << "\n";
    //start = std::chrono::system_clock::now();
    //cerr << std::hex << std::this_thread::get_id() << " dq->start." << std::endl;
    assert(!empty());

    if (!(high_queue.empty())) {
      T ret = high_queue.rbegin()->second.front().second;
      high_queue.rbegin()->second.pop_front();
      if (high_queue.rbegin()->second.empty())
	high_queue.erase(high_queue.rbegin()->first);
      --_high_size;
      return ret;
    }
    //if (!(fifo_queue.empty())) {
    //  T ret = fifo_queue.rbegin()->second.front().second;
    //  fifo_queue.rbegin()->second.pop_front();
    //  if (fifo_queue.rbegin()->second.empty())
    //    fifo_queue.erase(fifo_queue.rbegin()->first);
    //  return ret;
    //}
	  
    //print_queue();
    while (true) {
      //cerr << std::hex << std::this_thread::get_id() << " dq->at prio " << std::dec << dq->first << ", dq->second.size(): " << dq->second.size() << std::endl;
      if (dq->second.size() != 0) {
	//cerr << std::hex << std::this_thread::get_id() << " dq->second.size() not 0." << std::endl;
	if (dq->second.size() == _wrr_size) {
	  //cerr << std::hex << std::this_thread::get_id() << " dq->second.size() == _wrr_size (" << dq->second.size() << "==" << _wrr_size << "), going to dequeue." << std::endl;
	  break;
	}
	else {
	  //cerr << std::hex << std::this_thread::get_id() << " dq->Going to check if we should run this prio again." << std::endl;
	  // flip coin to see if the previous prioirty gets to run again.
	  if ((rand() % total_priority) <= dq->first) {
	    //cerr << std::hex << std::this_thread::get_id() << " dq->Yeah! I get to run again. The cost of the op is " <<
	      //dq->second.front().first << "." << std::endl;
	    if (dq->second.front().first == 0) {
	      //cerr << std::hex << std::this_thread::get_id() << " dq->No cost so going to dequeue." << std::endl;
	      break;
	    }
	    else {
	      // flip a coin to see if this priority gets to run based on cost
	      //cerr << std::hex << std::this_thread::get_id() << " dq->Going to see if we should run based on weight." << std::endl;
	      if ((rand() % _max_cost) <=
		  (_max_cost - (unsigned)((double)dq->second.front().first * 0.9))){
		//cerr << std::hex << std::this_thread::get_id() << " dq->Won the weight coin toss, going to dequeue." << std::endl;
		break;
	      }
	    }
	  }
	}
      }
      //cerr << std::hex << std::this_thread::get_id() << " dq->Did not find an op to run, going to next prio." << std::endl;
      inc_dq();
    }
    //cerr << std::hex << std::this_thread::get_id() << " dq->Going to front() op." << std::endl;
    T ret = dq->second.front().second;
    //cerr << std::hex << std::this_thread::get_id() << " dq->Going to pop() op." << std::endl;
    dq->second.pop_front();
    
    --_wrr_size;
    //cerr << std::hex << std::this_thread::get_id() << " After poping the op (" << std::hex << &ret << ")" << std::endl;
    //print_queue();
    //if (!_fp_enable && _wrr_size == 0)
    //  _fp_enable = true;
    //end = std::chrono::system_clock::now();
    //dq_stat.Push(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    //cerr << "Wrr (" << std::this_thread::get_id() << ") dequeue stats: Mean=" << std::dec << dq_stat.Mean() << "ns, Std dev.=" << dq_stat.StandardDeviation() << " ns\n";
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
