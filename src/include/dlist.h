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

#ifndef __DLIST_H
#define __DLIST_H

template<typename T>
class dlist {
public:
  struct item {
    T _item;
    item *_prev, *_next;
    
    item(T i) : _item(i), _prev(this), _next(this) {}
    ~item() { 
      assert(!is_on_dlist());
    }
    // no copying!
    item(const item& other);
    const item& operator= (const item& right);

    
    bool empty() { return _prev == this; }
    bool is_on_dlist() { return !empty(); }
    bool remove_myself() {
      if (_next == this) {
	assert(_prev == this);
	return false;
      }
      _next->_prev = _prev;
      _prev->_next = _next;
      _prev = _next = this;
      return true;
    }

    void insert_after(item *other) {
      assert(other->empty());
      other->_prev = this;
      other->_next = _next;
      _next->_prev = other;
      _next = other;
    }
    void insert_before(item *other) {
      assert(other->empty());
      other->_next = this;
      other->_prev = _prev;
      _prev->_next = other;
      _prev = other;
    }
  };

private:
  item _head;

public:
  dlist() : _head(NULL) {}
  ~dlist() { 
    assert(_head.empty());
  }

  bool empty() { 
    return _head.empty();
  }

  void clear() {
    while (!_head.empty())
      remove(front());
  }

  void push_front(item *item) {
    if (!item->empty()) 
      item->remove_myself();
    _head.insert_after(item);
  }
  void push_back(item *item) {
    if (!item->empty()) 
      item->remove_myself();
    _head.insert_before(item);
  }

  T front() { return (T)_head._next->_item; }
  T back() { return (T)_head._prev->_item; }

  void pop_front() {
    assert(!empty());
    _head->_next->remove_myself();
  }
  void pop_back() {
    assert(!empty());
    _head->_prev->remove_myself();
  }

  class iterator {
  private:
    item *cur;
  public:
    iterator(item *i = 0) : cur(i) {}
    T operator*() { return (T)cur->_item; }
    iterator& operator++() {
      assert(cur);
      cur = cur->_next;
      return *this;
    }
    bool end() { return cur->_item == 0; }
  };

  iterator begin() { return iterator(_head._next); }
};


#endif
