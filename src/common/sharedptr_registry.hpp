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

#ifndef CEPH_SHAREDPTR_REGISTRY_H
#define CEPH_SHAREDPTR_REGISTRY_H

#include <map>
#include <memory>
#include "common/ceph_mutex.h"

/**
 * Provides a registry of shared_ptr<V> indexed by K while
 * the references are alive.
 */
template <class K, class V, class C = std::less<K> >
class SharedPtrRegistry {
public:
  typedef std::shared_ptr<V> VPtr;
  typedef std::weak_ptr<V> WeakVPtr;
  int waiting;
private:
  ceph::mutex lock = ceph::make_mutex("SharedPtrRegistry::lock");
  ceph::condition_variable cond;
  std::map<K, std::pair<WeakVPtr, V*>, C> contents;

  class OnRemoval {
    SharedPtrRegistry<K,V,C> *parent;
    K key;
  public:
    OnRemoval(SharedPtrRegistry<K,V,C> *parent, K key) :
      parent(parent), key(key) {}
    void operator()(V *to_remove) {
      {
	std::lock_guard l(parent->lock);
	typename std::map<K, std::pair<WeakVPtr, V*>, C>::iterator i =
	  parent->contents.find(key);
	if (i != parent->contents.end() &&
	    i->second.second == to_remove) {
	  parent->contents.erase(i);
	  parent->cond.notify_all();
	}
      }
      delete to_remove;
    }
  };
  friend class OnRemoval;

public:
  SharedPtrRegistry() :
    waiting(0)
  {}

  bool empty() {
    std::lock_guard l(lock);
    return contents.empty();
  }

  bool get_next(const K &key, std::pair<K, VPtr> *next) {
    std::pair<K, VPtr> r;
    {
      std::lock_guard l(lock);
      VPtr next_val;
      typename std::map<K, std::pair<WeakVPtr, V*>, C>::iterator i =
	contents.upper_bound(key);
      while (i != contents.end() &&
	     !(next_val = i->second.first.lock()))
	++i;
      if (i == contents.end())
	return false;
      if (next)
	r = std::make_pair(i->first, next_val);
    }
    if (next)
      *next = r;
    return true;
  }

  
  bool get_next(const K &key, std::pair<K, V> *next) {
    VPtr next_val;
    std::lock_guard l(lock);
    typename std::map<K, std::pair<WeakVPtr, V*>, C>::iterator i =
      contents.upper_bound(key);
    while (i != contents.end() &&
	   !(next_val = i->second.first.lock()))
      ++i;
    if (i == contents.end())
      return false;
    if (next)
      *next = std::make_pair(i->first, *next_val);
    return true;
  }

  VPtr lookup(const K &key) {
    std::unique_lock l(lock);
    waiting++;
    while (1) {
      typename std::map<K, std::pair<WeakVPtr, V*>, C>::iterator i =
	contents.find(key);
      if (i != contents.end()) {
	VPtr retval = i->second.first.lock();
	if (retval) {
	  waiting--;
	  return retval;
	}
      } else {
	break;
      }
      cond.wait(l);
    }
    waiting--;
    return VPtr();
  }

  VPtr lookup_or_create(const K &key) {
    std::unique_lock l(lock);
    waiting++;
    while (1) {
      typename std::map<K, std::pair<WeakVPtr, V*>, C>::iterator i =
	contents.find(key);
      if (i != contents.end()) {
	VPtr retval = i->second.first.lock();
	if (retval) {
	  waiting--;
	  return retval;
	}
      } else {
	break;
      }
      cond.wait(l);
    }
    V *ptr = new V();
    VPtr retval(ptr, OnRemoval(this, key));
    contents.insert(std::make_pair(key, make_pair(retval, ptr)));
    waiting--;
    return retval;
  }

  unsigned size() {
    std::lock_guard l(lock);
    return contents.size();
  }

  void remove(const K &key) {
    std::lock_guard l(lock);
    contents.erase(key);
    cond.notify_all();
  }

  template<class A>
  VPtr lookup_or_create(const K &key, const A &arg) {
    std::unique_lock l(lock);
    waiting++;
    while (1) {
      typename std::map<K, std::pair<WeakVPtr, V*>, C>::iterator i =
	contents.find(key);
      if (i != contents.end()) {
	VPtr retval = i->second.first.lock();
	if (retval) {
	  waiting--;
	  return retval;
	}
      } else {
	break;
      }
      cond.wait(l);
    }
    V *ptr = new V(arg);
    VPtr retval(ptr, OnRemoval(this, key));
    contents.insert(std::make_pair(key, make_pair(retval, ptr)));
    waiting--;
    return retval;
  }

  friend class SharedPtrRegistryTest;
};

#endif
