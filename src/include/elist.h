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

#ifndef CEPH_ELIST_H
#define CEPH_ELIST_H

/*
 * elist: embedded list.
 *
 * requirements:
 *   - elist<T>::item be embedded in the parent class
 *   - items are _always_ added to the list via the same elist<T>::item at the same
 *     fixed offset in the class.
 *   - begin(), front(), back() methods take the member offset as an argument for traversal.
 *
 */

#define member_offset(cls, member) ((size_t)(&((cls*)1)->member) - 1)

template<typename T>
class elist {
public:
  struct item {
  private:
    elist *_list = NULL;

  public:
    item *_prev, *_next;

    item(T i=0) : _prev(this), _next(this) {}
    item(elist *l) : _list(l), _prev(this), _next(this) {}
    ~item() {
      ceph_assert(!is_on_list());
    }

    item(const item& other) = delete;
    const item& operator= (const item& right) = delete;

    bool empty() const { return _prev == this; }
    bool is_on_list() const { return !empty(); }

    void set_list(elist *l) {
      _list = l;
    }

    bool remove_myself() {
      if (_next == this) {
	ceph_assert(_prev == this);
	return false;
      }
      _next->_prev = _prev;
      _prev->_next = _next;
      _prev = _next = this;
      ceph_assert(_list);
      _list->items--;
      _list = NULL;
      return true;
    }

    void insert_after(elist *l, item *other) {
      ceph_assert(other->empty());
      other->set_list(l);
      other->_prev = this;
      other->_next = _next;
      _next->_prev = other;
      _next = other;
    }
    void insert_before(elist *l, item *other) {
      ceph_assert(other->empty());
      other->set_list(l);
      other->_next = this;
      other->_prev = _prev;
      _prev->_next = other;
      _prev = other;
    }

    T get_item(size_t offset) {
      ceph_assert(offset);
      return (T)(((char *)this) - offset); 
    }
  };

private:
  item _head;
  size_t item_offset;
  uint64_t items = 0;

public:
  elist(const elist& other);
  const elist& operator=(const elist& other);

  elist(size_t o) : _head(this), item_offset(o) {}
  ~elist() {
    ceph_assert(_head.empty());
  }

  size_t size() const {
    return items;
  }

  bool empty() const {
    return _head.empty();
  }

  void clear() {
    while (!_head.empty())
      pop_front();
  }

  void push_front(item *i) {
    if (!i->empty()) 
      i->remove_myself();
    _head.insert_after(this, i);
    items++;
  }
  void push_back(item *i) {
    if (!i->empty()) 
      i->remove_myself();
    _head.insert_before(this, i);
    items++;
  }

  T front(size_t o=0) {
    ceph_assert(!_head.empty());
    return _head._next->get_item(o ? o : item_offset);
  }
  T back(size_t o=0) {
    ceph_assert(!_head.empty());
    return _head._prev->get_item(o ? o : item_offset);
  }

  void pop_front() {
    ceph_assert(!empty());
    _head._next->remove_myself();
  }
  void pop_back() {
    ceph_assert(!empty());
    _head._prev->remove_myself();
  }

  void clear_list() {
    while (!empty())
      pop_front();
    ceph_assert(items == 0);
  }

  enum mode_t {
    MAGIC, CURRENT, CACHE_NEXT
  };

  class iterator {
  private:
    item *head;
    item *cur, *next;
    size_t item_offset;
    mode_t mode;
  public:
    iterator(item *h, size_t o, mode_t m) :
      head(h), cur(h->_next), next(cur->_next), item_offset(o),
      mode(m) {
      ceph_assert(item_offset > 0);
    }
    T operator*() {
      return cur->get_item(item_offset);
    }
    iterator& operator++() {
      ceph_assert(cur);
      ceph_assert(cur != head);
      if (mode == MAGIC) {
	// if 'cur' appears to be valid, use that.  otherwise,
	// use cached 'next'.
	// this is a bit magic, and probably a bad idea... :/
	if (cur->empty())
	  cur = next;
	else
	  cur = cur->_next;
      } else if (mode == CURRENT)
	cur = cur->_next;
      else if (mode == CACHE_NEXT)
	cur = next;
      else
	ceph_abort();
      next = cur->_next;
      return *this;
    }
    bool end() const {
      return cur == head;
    }
  };

  iterator begin(size_t o=0) {
    return iterator(&_head, o ? o : item_offset, MAGIC);
  }
  iterator begin_use_current(size_t o=0) {
    return iterator(&_head, o ? o : item_offset, CURRENT);
  }
  iterator begin_cache_next(size_t o=0) {
    return iterator(&_head, o ? o : item_offset, CACHE_NEXT);
  }
};


#endif
