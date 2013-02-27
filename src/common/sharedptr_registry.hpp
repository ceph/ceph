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
#include "common/Mutex.h"
#include "common/Cond.h"

/**
 * Provides a registry of shared_ptr<V> indexed by K while
 * the references are alive.
 */
template <class K, class V>
class SharedPtrRegistry {
public:
  typedef std::tr1::shared_ptr<V> VPtr;
  typedef std::tr1::weak_ptr<V> WeakVPtr;
private:
  Mutex lock;
  Cond cond;
  map<K, WeakVPtr> contents;

  class OnRemoval {
    SharedPtrRegistry<K,V> *parent;
    K key;
  public:
    OnRemoval(SharedPtrRegistry<K,V> *parent, K key) :
      parent(parent), key(key) {}
    void operator()(V *to_remove) {
      {
	Mutex::Locker l(parent->lock);
	parent->contents.erase(key);
	parent->cond.Signal();
      }
      delete to_remove;
    }
  };
  friend class OnRemoval;

public:
  SharedPtrRegistry() : lock("SharedPtrRegistry::lock") {}

  bool get_next(const K &key, pair<K, V> *next) {
    VPtr next_val;
    Mutex::Locker l(lock);
    typename map<K, WeakVPtr>::iterator i = contents.upper_bound(key);
    while (i != contents.end() &&
	   !(next_val = i->second.lock()))
      ++i;
    if (i == contents.end())
      return false;
    if (next)
      *next = make_pair(i->first, *next_val);
    return true;
  }

  VPtr lookup(const K &key) {
    Mutex::Locker l(lock);
    while (1) {
      if (contents.count(key)) {
	VPtr retval = contents[key].lock();
	if (retval)
	  return retval;
      } else {
	break;
      }
      cond.Wait(lock);
    }
    return VPtr();
  }

  VPtr lookup_or_create(const K &key) {
    Mutex::Locker l(lock);
    while (1) {
      if (contents.count(key)) {
	VPtr retval = contents[key].lock();
	if (retval)
	  return retval;
      } else {
	break;
      }
      cond.Wait(lock);
    }
    VPtr retval(new V(), OnRemoval(this, key));
    contents[key] = retval;
    return retval;
  }

  template<class A>
  VPtr lookup_or_create(const K &key, const A &arg) {
    Mutex::Locker l(lock);
    while (1) {
      if (contents.count(key)) {
	VPtr retval = contents[key].lock();
	if (retval)
	  return retval;
      } else {
	break;
      }
      cond.Wait(lock);
    }
    VPtr retval(new V(arg), OnRemoval(this, key));
    contents[key] = retval;
    return retval;
  }
};

#endif
