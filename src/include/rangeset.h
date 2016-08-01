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


#ifndef CEPH_RANGESET_H
#define CEPH_RANGESET_H

/*
 *
 * my first container with iterator!   it's pretty ugly.
 *
 */

#include <map>
using namespace std;

//typedef int T;

template <class T>
struct _rangeset_base {
  map<T,T> ranges;  // pair(first,last) (inclusive, e.g. [first,last])
                    
  typedef typename map<T,T>::iterator mapit;

  // get iterator for range including val.  or ranges.end().
  mapit get_range_for(T val) {
    mapit it = ranges.lower_bound(val);
    if (it == ranges.end()) {
      // search backwards
      typename map<T,T>::reverse_iterator it = ranges.rbegin();
      if (it == ranges.rend()) return ranges.end();
      if (it->first <= val && it->second >= val)
        return ranges.find(it->first);
      return ranges.end();
    } else {
      if (it->first == val) return 
      it--;
      if (it->first <= val && it->second >= val)
        return it;
      return ranges.end();
    }
  }

};


template <class T>
class rangeset_iterator :
  public std::iterator<std::input_iterator_tag, T>
{
  //typedef typename map<T,T>::iterator mapit;

  map<T,T> ranges;
  typename map<T,T>::iterator it;
  T current;

public:
  // cons
  rangeset_iterator() {}

  rangeset_iterator(typename map<T,T>::iterator& it, map<T,T>& ranges) {
    this->ranges = ranges;
    this->it = it;
    if (this->it != ranges.end())
      current = it->first;
  }

  bool operator==(rangeset_iterator<T> rit) {
    return (it == rit.it && rit.current == current);
  }
  bool operator!=(rangeset_iterator<T> rit) {
    return (it != rit.it) || (rit.current != current);
  }
  
  T& operator*() {
    return current;
  }

  rangeset_iterator<T> operator++(int) {
    if (current < it->second)
      current++;
    else {
      it++;
      if (it != ranges.end())
        current = it->first;
    }
    
    return *this;
  }
};


template <class T>
class rangeset
{
  typedef typename map<T,T>::iterator map_iterator;

  _rangeset_base<T> theset;
  inodeno_t _size;

public:
  rangeset() { _size = 0; }
  typedef rangeset_iterator<T> iterator;

  iterator begin() {
    map_iterator it = theset.ranges.begin();
    return iterator(it, theset.ranges);
  }

  iterator end() {
    map_iterator it = theset.ranges.end();
    return iterator(it, theset.ranges);
  }

  map_iterator map_begin() {
    return theset.ranges.begin();
  }
  map_iterator map_end() {
    return theset.ranges.end();
  }
  int map_size() {
    return theset.ranges.size();
  }

  void map_insert(T v1, T v2) {
    theset.ranges.insert(pair<T,T>(v1,v2));
    _size += v2 - v1+1;
  }


  // ...
  bool contains(T val) {
    if (theset.get_range_for(val) == theset.ranges.end()) return false;
    assert(!empty());
    return true;
  }
  
  void insert(T val) {
    assert(!contains(val));

    map_iterator left = theset.get_range_for(val-1);
    map_iterator right = theset.get_range_for(val+1);

    if (left != theset.ranges.end() &&
        right != theset.ranges.end()) {
      // join!
      left->second = right->second;
      theset.ranges.erase(right);
      _size++;
      return;
    }

    if (left != theset.ranges.end()) {
      // add to left range
      left->second = val;
      _size++;
      return;
    }

    if (right != theset.ranges.end()) {
      // add to right range
      theset.ranges.insert(pair<T,T>(val, right->second));
      theset.ranges.erase(val+1);
      _size++;
      return;
    }

    // new range
    theset.ranges.insert(pair<T,T>(val,val));
    _size++;
    return;
  }

  unsigned size() {
    return size();
  }

  bool empty() {
    if (theset.ranges.empty()) {
      assert(_size == 0);
      return true;
    }
    assert(_size>0);
    return false;
  }

  
  T first() {
    assert(!empty());
    map_iterator it = theset.ranges.begin();
    return it->first;
  }
  
  void erase(T val) {
    assert(contains(val));
    map_iterator it = theset.get_range_for(val);
    assert(it != theset.ranges.end());
    
    // entire range
    if (val == it->first && val == it->second) {
      theset.ranges.erase(it);
      _size--;
      return;
    }

    // beginning
    if (val == it->first) {
      theset.ranges.insert(pair<T,T>(val+1, it->second));
      theset.ranges.erase(it);
      _size--;
      return;      
    }

    // end
    if (val == it->second) {
      it->second = val-1;
      _size--;
      return;
    }

    // middle split
    theset.ranges.insert(pair<T,T>(it->first, val-1));
    theset.ranges.insert(pair<T,T>(val+1, it->second));
    theset.ranges.erase(it);
    _size--;
    return;
  }

  void dump() {
    for (typename map<T,T>::iterator it = theset.ranges.begin();
         it != theset.ranges.end();
         it++) {
      cout << " " << it->first << "-" << it->second << endl;
    }
  }

};


#endif
