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

#ifndef OS_KEYVALUESTORE_H
#define OS_KEYVALUESTORE_H

#include <string>
#include <vector>
#include <tr1/memory>

#include "IndexManager.h"

/**
 * Encapsulates the FileStore key value store
 *
 * Implementations of this interface will be used to implement TMAP
 */
class ObjectMap {
public:
  /// Set keys and values from specified map
  virtual int set_keys(
    const hobject_t &hoid,              ///< [in] object containing map
    Index index,                        ///< [in] Path to hoid
    const map<string, bufferlist> &set   ///< [in] key to value map to set
    ) = 0;

  /// Set header
  virtual int set_header(
    const hobject_t &hoid,              ///< [in] object containing map
    Index index,                        ///< [in] Path to hoid
    const bufferlist &bl                ///< [in] header to set
    ) = 0;

  /// Retrieve header
  virtual int get_header(
    const hobject_t &hoid,              ///< [in] object containing map
    Index index,                        ///< [in] Path to hoid
    bufferlist *bl                      ///< [out] header to set
    ) = 0;

  /// Clear all map keys and values from hoid
  virtual int clear(
    const hobject_t &hoid,              ///< [in] object containing map
    Index index                         ///< [in] Path to hoid
    ) = 0;

  /// Clear all map keys and values from hoid
  virtual int rm_keys(
    const hobject_t &hoid,              ///< [in] object containing map
    Index index,                        ///< [in] Path to hoid
    const set<string> &to_clear         ///< [in] Keys to clear
    ) = 0;

  /// Get all keys and values
  virtual int get(
    const hobject_t &hoid,             ///< [in] object containing map
    Index index,                       ///< [in] Path to hoid
    bufferlist *header,                ///< [out] Returned Header
    map<string, bufferlist> *out       ///< [out] Returned keys and values
    ) = 0;

  /// Get values for supplied keys
  virtual int get_keys(
    const hobject_t &hoid,             ///< [in] object containing map
    Index index,                       ///< [in] Path to hoid
    set<string> *keys                  ///< [out] Keys defined on hoid
    ) = 0;

  /// Get values for supplied keys
  virtual int get_values(
    const hobject_t &hoid,             ///< [in] object containing map
    Index index,                       ///< [in] Path to hoid
    const set<string> &keys,           ///< [in] Keys to get
    map<string, bufferlist> *out       ///< [out] Returned keys and values
    ) = 0;

  /// Check key existence
  virtual int check_keys(
    const hobject_t &hoid,             ///< [in] object containing map
    Index index,                       ///< [in] Path to hoid
    const set<string> &keys,           ///< [in] Keys to check
    set<string> *out                   ///< [out] Subset of keys defined on hoid
    ) = 0;

  /// Get xattrs
  virtual int get_xattrs(
    const hobject_t &hoid,             ///< [in] object
    Index index,                       ///< [in] path to hoid
    const set<string> &to_get,         ///< [in] keys to get
    map<string, bufferlist> *out       ///< [out] subset of attrs/vals defined
    ) = 0;

  /// Get all xattrs
  virtual int get_all_xattrs(
    const hobject_t &hoid,             ///< [in] object
    Index index,                       ///< [in] path to hoid
    set<string> *out       ///< [out] attrs and values
    ) = 0;

  /// set xattrs in to_set
  virtual int set_xattrs(
    const hobject_t &hoid,                ///< [in] object
    Index index,                          ///< [in] path to object
    const map<string, bufferlist> &to_set ///< [in] attrs/values to set
    ) = 0;

  /// remove xattrs in to_remove
  virtual int remove_xattrs(
    const hobject_t &hoid,               ///< [in] object
    Index index,                         ///< [in] path to hoid
    const set<string> &to_remove         ///< [in] attrs to remove
    ) = 0;


  /// Clone keys efficiently from hoid map to target map
  virtual int clone(
    const hobject_t &hoid,             ///< [in] object containing map
    Index index,                       ///< [in] Path to hoid
    const hobject_t &target,           ///< [in] target of clone
    Index target_index                 ///< [in] path to target
    ) { return 0; }

  /// Ensure all previous writes are durable
  virtual int sync() { return 0; }

  virtual bool check(std::ostream &out) { return true; }

  class ObjectMapIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int upper_bound(const string &after) = 0;
    virtual int lower_bound(const string &to) = 0;
    virtual bool valid() = 0;
    virtual int next() = 0;
    virtual string key() = 0;
    virtual bufferlist value() = 0;
    virtual int status() = 0;
    virtual ~ObjectMapIteratorImpl() {}
  };
  typedef std::tr1::shared_ptr<ObjectMapIteratorImpl> ObjectMapIterator;
  virtual ObjectMapIterator get_iterator(const hobject_t &hoid,
					 Index index) {
    return ObjectMapIterator();
  }


  virtual ~ObjectMap() {}
};

#endif
