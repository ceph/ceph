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

/*
class xlist_head;

class xlist_item {
 private:
  xlist_item *_prev, *_next;
  xlist_head *_head;
  friend class xlist_head;

 public:
  xlist_item() : _prev(0), _next(0), _head(0) {}
  xlist_head* _get_containing_xlist() { return _head; }
};

class xlist_head {
 private:
  xlist_item *_front, *_back;
  int _size;

  friend class xlist_item;

 public:
  int size() { return _size; }
  bool empty() { return _front == 0; }

  void push_back(xlist_item *item) {
    if (item->_head) item->_head->remove(item);

    item->_head = this;
    item->_next = 0;
    item->_prev = _back;
    if (_back) _back->_next = item;
    _back = item;
    _size++;
  }
  void remove(xlist_item *item) {
    assert(item->_head == this);

    if (item->_prev)
      item->_prev->_next = item->_next;
    else
      _front = item->_next;
    if (item->_next)
      item->_next->_prev = item->_prev;
    else
      _back = item->_prev;
    _size--;

    item->_head = 0;
    item->_next = item->_prev = 0;
  }

};
*/



template<typename T>
class xlist {
public:
  struct item {
    T _item;
    item *_prev, *_next;
    xlist *_head;
    
    item(T i) : _item(i), _prev(0), _next(0), _head(0) {}
    
    xlist* get_xlist() { return _head; }
    void remove_myself() {
      if (_head) {
	_head->remove(this);
      }
    }
  };

private:
  item *_front, *_back;
  int _size;

public:
  int size() { return _size; }
  bool empty() { return _front == 0; }

  void push_back(item *item) {
    if (item->_head) 
      item->_head->remove(item);

    item->_head = this;
    item->_next = 0;
    item->_prev = _back;
    if (_back) _back->_next = item;
    _back = item;
    _size++;
  }
  void remove(item *item) {
    assert(item->_head == this);
    
    if (item->_prev)
      item->_prev->_next = item->_next;
    else
      _front = item->_next;
    if (item->_next)
      item->_next->_prev = item->_prev;
    else
      _back = item->_prev;
    _size--;

    item->_head = 0;
    item->_next = item->_prev = 0;
  }

  T front() { return (T)_front->_item; }
  T back() { return (T)_back->_item; }

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
