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

#ifndef __XLIST_H
#define __XLIST_H

template<typename T>
class xlist {
public:
  struct item {
    T _item;
    item *_prev, *_next;
    xlist *_list;
    
    item(T i) : _item(i), _prev(0), _next(0), _list(0) {}
    ~item() { 
      assert(!is_on_list());
      //remove_myself();
    }
    // no copying!
    item(const item& other);
    const item& operator= (const item& right);

    
    xlist* get_list() { return _list; }
    bool is_on_list() { return _list ? true:false; }
    bool remove_myself() {
      if (_list) {
	_list->remove(this);
	assert(_list == 0);
	return true;
      } else
	return false;
    }
    void move_to_front() {
      assert(_list);
      _list->push_front(this);
    }
    void move_to_back() {
      assert(_list);
      _list->push_back(this);
    }
  };

private:
  item *_front, *_back;
  int _size;

public:
  xlist() : _front(0), _back(0), _size(0) {}
  ~xlist() { 
    assert(_size == 0);
    assert(_front == 0);
    assert(_back == 0);
  }

  int size() { return _size; }
  bool empty() { 
    assert((bool)_front == (bool)_size);
    return _front == 0; 
  }

  void clear() {
    while (_front) remove(_front);
  }

  void push_front(item *item) {
    if (item->_list) 
      item->_list->remove(item);

    item->_list = this;
    item->_next = _front;
    item->_prev = 0;
    if (_front) 
      _front->_prev = item;
    else
      _back = item;
    _front = item;
    _size++;
  }
  void push_back(item *item) {
    if (item->_list) 
      item->_list->remove(item);

    item->_list = this;
    item->_next = 0;
    item->_prev = _back;
    if (_back) 
      _back->_next = item;
    else
      _front = item;
    _back = item;
    _size++;
  }
  void remove(item *item) {
    assert(item->_list == this);
    
    if (item->_prev)
      item->_prev->_next = item->_next;
    else
      _front = item->_next;
    if (item->_next)
      item->_next->_prev = item->_prev;
    else
      _back = item->_prev;
    _size--;

    item->_list = 0;
    item->_next = item->_prev = 0;
  }

  T front() { return (T)_front->_item; }
  T back() { return (T)_back->_item; }

  void pop_front() {
    assert(!empty());
    remove(_front);
  }
  void pop_back() {
    assert(!empty());
    remove(_back);
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
    bool end() { return cur == 0; }
  };

  iterator begin() { return iterator(_front); }
  iterator end() { return iterator(NULL); }
};


#endif
