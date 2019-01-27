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

#ifndef CEPH_XLIST_H
#define CEPH_XLIST_H

#include <iterator>
#include <cstdlib>
#include <ostream>

#include "include/ceph_assert.h"

template<typename T>
class xlist {
public:
  class item {
  public:
    item(T i) : _item(i) {}
    ~item() { 
      ceph_assert(!is_on_list());
    }

    item(const item& other) = delete;
    item(item&& other) = delete;
    const item& operator= (const item& right) = delete;
    item& operator= (item&& right) = delete;

    xlist* get_list() { return _list; }
    bool is_on_list() const { return _list ? true:false; }
    bool remove_myself() {
      if (_list) {
	_list->remove(this);
	ceph_assert(_list == 0);
	return true;
      } else
	return false;
    }
    void move_to_front() {
      ceph_assert(_list);
      _list->push_front(this);
    }
    void move_to_back() {
      ceph_assert(_list);
      _list->push_back(this);
    }

  private:
    friend xlist;
    T _item;
    item *_prev = nullptr, *_next = nullptr;
    xlist *_list = nullptr;
  };

  typedef item* value_type;
  typedef item* const_reference;

private:
  item *_front, *_back;
  size_t _size;

public:
  xlist(const xlist& other) {
    _front = other._front;
    _back = other._back;
    _size = other._size;
  }

  xlist() : _front(0), _back(0), _size(0) {}
  ~xlist() { 
    ceph_assert(_size == 0);
    ceph_assert(_front == 0);
    ceph_assert(_back == 0);
  }

  size_t size() const {
    ceph_assert((bool)_front == (bool)_size);
    return _size;
  }
  bool empty() const { 
    ceph_assert((bool)_front == (bool)_size);
    return _front == 0; 
  }

  void clear() {
    while (_front)
      remove(_front);
    ceph_assert((bool)_front == (bool)_size);
  }

  void push_front(item *i) {
    if (i->_list) 
      i->_list->remove(i);

    i->_list = this;
    i->_next = _front;
    i->_prev = 0;
    if (_front) 
      _front->_prev = i;
    else
      _back = i;
    _front = i;
    _size++;
  }
  void push_back(item *i) {
    if (i->_list) 
      i->_list->remove(i);

    i->_list = this;
    i->_next = 0;
    i->_prev = _back;
    if (_back) 
      _back->_next = i;
    else
      _front = i;
    _back = i;
    _size++;
  }
  void remove(item *i) {
    ceph_assert(i->_list == this);
    
    if (i->_prev)
      i->_prev->_next = i->_next;
    else
      _front = i->_next;
    if (i->_next)
      i->_next->_prev = i->_prev;
    else
      _back = i->_prev;
    _size--;

    i->_list = 0;
    i->_next = i->_prev = 0;
    ceph_assert((bool)_front == (bool)_size);
  }

  T front() { return static_cast<T>(_front->_item); }
  const T front() const { return static_cast<const T>(_front->_item); }

  T back() { return static_cast<T>(_back->_item); }
  const T back() const { return static_cast<const T>(_back->_item); }

  void pop_front() {
    ceph_assert(!empty());
    remove(_front);
  }
  void pop_back() {
    ceph_assert(!empty());
    remove(_back);
  }

  class iterator: std::iterator<std::forward_iterator_tag, T> {
  private:
    item *cur;
  public:
    iterator(item *i = 0) : cur(i) {}
    T operator*() { return static_cast<T>(cur->_item); }
    iterator& operator++() {
      ceph_assert(cur);
      ceph_assert(cur->_list);
      cur = cur->_next;
      return *this;
    }
    bool end() const { return cur == 0; }
    bool operator==(const iterator& rhs) const {
      return cur == rhs.cur;
    }
    bool operator!=(const iterator& rhs) const {
      return cur != rhs.cur;
    }
  };

  iterator begin() { return iterator(_front); }
  iterator end() { return iterator(NULL); }

  class const_iterator: std::iterator<std::forward_iterator_tag, T> {
  private:
    item *cur;
  public:
    const_iterator(item *i = 0) : cur(i) {}
    const T operator*() { return static_cast<const T>(cur->_item); }
    const_iterator& operator++() {
      ceph_assert(cur);
      ceph_assert(cur->_list);
      cur = cur->_next;
      return *this;
    }
    bool end() const { return cur == 0; }
    bool operator==(const_iterator& rhs) const {
      return cur == rhs.cur;
    }
    bool operator!=(const_iterator& rhs) const {
      return cur != rhs.cur;
    }
  };

  const_iterator begin() const { return const_iterator(_front); }
  const_iterator end() const { return const_iterator(NULL); }

  friend std::ostream &operator<<(std::ostream &oss, const xlist<T> &list) {
    bool first = true;
    for (const auto &item : list) {
      if (!first) {
        oss << ", ";
      }
      oss << *item; /* item should be a pointer */
      first = false;
    }
    return oss;
  }
};


#endif
