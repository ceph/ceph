// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef MAPCACHER_H
#define MAPCACHER_H

#include "include/Context.h"
#include "common/sharedptr_registry.hpp"

namespace MapCacher {
/**
 * Abstraction for ordering key updates
 */
template<typename K, typename V>
class Transaction {
public:
  /// Std::set keys according to map
  virtual void set_keys(
    const std::map<K, V> &keys ///< [in] keys/values to std::set
    ) = 0;

  /// Remove keys
  virtual void remove_keys(
    const std::set<K> &to_remove ///< [in] keys to remove
    ) = 0;

  /// Add context to fire when data is readable
  virtual void add_callback(
    Context *c ///< [in] Context to fire on readable
    ) = 0;
  virtual ~Transaction() {}
};

/**
 * Abstraction for fetching keys
 */
template<typename K, typename V>
class StoreDriver {
public:
  /// Returns requested key values
  virtual int get_keys(
    const std::set<K> &keys,   ///< [in] keys requested
    std::map<K, V> *got  ///< [out] values for keys obtained
    ) = 0; ///< @return error value

  /// Returns next key
  virtual int get_next(
    const K &key,       ///< [in] key after which to get next
    std::pair<K, V> *next    ///< [out] first key after key
    ) = 0; ///< @return 0 on success, -ENOENT if there is no next

  virtual int get_next_or_current(
    const K &key,       ///< [in] key at-which-or-after to get
    std::pair<K, V> *next_or_current
    ) = 0; ///< @return 0 on success, -ENOENT if there is no next

  virtual ~StoreDriver() {}
};

/**
 * Uses SharedPtrRegistry to cache objects of in progress writes
 * allowing the user to read/write a consistent view of the map
 * without flushing writes.
 */
template<typename K, typename V>
class MapCacher {
private:
  StoreDriver<K, V> *driver;

  SharedPtrRegistry<K, boost::optional<V> > in_progress;
  typedef typename SharedPtrRegistry<K, boost::optional<V> >::VPtr VPtr;
  typedef ContainerContext<std::set<VPtr> > TransHolder;

public:
  MapCacher(StoreDriver<K, V> *driver) : driver(driver) {}

  void reset() {
    in_progress.reset();
  }

  /// Fetch first key/value std::pair after specified key
  int get_next(
    K key,               ///< [in] key after which to get next
    std::pair<K, V> *next     ///< [out] next key
    ) {
    while (true) {
      std::pair<K, boost::optional<V> > cached;
      std::pair<K, V> store;
      bool got_cached = in_progress.get_next(key, &cached);

      bool got_store = false;
      int r = driver->get_next(key, &store);
      if (r < 0 && r != -ENOENT) {
	return r;
      } else if (r == 0) {
	got_store = true;
      }

      if (!got_cached && !got_store) {
	return -ENOENT;
      } else if (
	got_cached &&
	(!got_store || store.first >= cached.first)) {
	if (cached.second) {
	  if (next)
	    *next = make_pair(cached.first, cached.second.get());
	  return 0;
	} else {
	  key = cached.first;
	  continue; // value was cached as removed, recurse
	}
      } else {
	if (next)
	  *next = store;
	return 0;
      }
    }
    ceph_abort(); // not reachable
    return -EINVAL;
  } ///< @return error value, 0 on success, -ENOENT if no more entries

  /// Adds operation setting keys to Transaction
  void set_keys(
    const std::map<K, V> &keys,  ///< [in] keys/values to std::set
    Transaction<K, V> *t    ///< [out] transaction to use
    ) {
    std::set<VPtr> vptrs;
    for (auto i = keys.begin(); i != keys.end(); ++i) {
      VPtr ip = in_progress.lookup_or_create(i->first, i->second);
      *ip = i->second;
      vptrs.insert(ip);
    }
    t->set_keys(keys);
    t->add_callback(new TransHolder(vptrs));
  }

  /// Adds operation removing keys to Transaction
  void remove_keys(
    const std::set<K> &keys,  ///< [in]
    Transaction<K, V> *t ///< [out] transaction to use
    ) {
    std::set<VPtr> vptrs;
    for (auto i = keys.begin(); i != keys.end(); ++i) {
      boost::optional<V> empty;
      VPtr ip = in_progress.lookup_or_create(*i, empty);
      *ip = empty;
      vptrs.insert(ip);
    }
    t->remove_keys(keys);
    t->add_callback(new TransHolder(vptrs));
  }

  /// Gets keys, uses cached values for unstable keys
  int get_keys(
    const std::set<K> &keys_to_get, ///< [in] std::set of keys to fetch
    std::map<K, V> *got             ///< [out] keys gotten
    ) {
    std::set<K> to_get;
    std::map<K, V> _got;
    for (auto i = keys_to_get.begin();
	 i != keys_to_get.end();
	 ++i) {
      VPtr val = in_progress.lookup(*i);
      if (val) {
	if (*val)
	  got->insert(make_pair(*i, val->get()));
	//else: value cached is empty, key doesn't exist
      } else {
	to_get.insert(*i);
      }
    }
    int r = driver->get_keys(to_get, &_got);
    if (r < 0)
      return r;
    for (auto i = _got.begin(); i != _got.end(); ++i) {
      got->insert(*i);
    }
    return 0;
  } ///< @return error value, 0 on success
};
} // namespace

#endif
