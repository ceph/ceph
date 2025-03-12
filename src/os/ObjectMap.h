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

#include <memory>
#include <string>
#include <vector>
#include "kv/KeyValueDB.h"
#include "common/hobject.h"

class SequencerPosition;

/**
 * Encapsulates the FileStore key value store
 *
 * Implementations of this interface will be used to implement TMAP
 */
class ObjectMap {
public:
  CephContext* cct;
  boost::scoped_ptr<KeyValueDB> db;
  /// std::Set keys and values from specified map
  virtual int set_keys(
    const ghobject_t &oid,              ///< [in] object containing map
    const std::map<std::string, ceph::buffer::list> &set,  ///< [in] key to value map to set
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// std::Set header
  virtual int set_header(
    const ghobject_t &oid,              ///< [in] object containing map
    const ceph::buffer::list &bl,               ///< [in] header to set
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// Retrieve header
  virtual int get_header(
    const ghobject_t &oid,              ///< [in] object containing map
    ceph::buffer::list *bl                      ///< [out] header to set
    ) = 0;

  /// Clear all map keys and values from oid
  virtual int clear(
    const ghobject_t &oid,             ///< [in] object containing map
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// Clear all map keys and values in to_clear from oid
  virtual int rm_keys(
    const ghobject_t &oid,              ///< [in] object containing map
    const std::set<std::string> &to_clear,        ///< [in] Keys to clear
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// Clear all omap keys and the header
  virtual int clear_keys_header(
    const ghobject_t &oid,              ///< [in] oid to clear
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// Get all keys and values
  virtual int get(
    const ghobject_t &oid,             ///< [in] object containing map
    ceph::buffer::list *header,                ///< [out] Returned Header
    std::map<std::string, ceph::buffer::list> *out       ///< [out] Returned keys and values
    ) = 0;

  /// Get values for supplied keys
  virtual int get_keys(
    const ghobject_t &oid,             ///< [in] object containing map
    std::set<std::string> *keys                  ///< [out] Keys defined on oid
    ) = 0;

  /// Get values for supplied keys
  virtual int get_values(
    const ghobject_t &oid,             ///< [in] object containing map
    const std::set<std::string> &keys,           ///< [in] Keys to get
    std::map<std::string, ceph::buffer::list> *out       ///< [out] Returned keys and values
    ) = 0;

  /// Check key existence
  virtual int check_keys(
    const ghobject_t &oid,             ///< [in] object containing map
    const std::set<std::string> &keys,           ///< [in] Keys to check
    std::set<std::string> *out                   ///< [out] Subset of keys defined on oid
    ) = 0;

  /// Get xattrs
  virtual int get_xattrs(
    const ghobject_t &oid,             ///< [in] object
    const std::set<std::string> &to_get,         ///< [in] keys to get
    std::map<std::string, ceph::buffer::list> *out       ///< [out] subset of attrs/vals defined
    ) = 0;

  /// Get all xattrs
  virtual int get_all_xattrs(
    const ghobject_t &oid,             ///< [in] object
    std::set<std::string> *out                   ///< [out] attrs and values
    ) = 0;

  /// std::set xattrs in to_set
  virtual int set_xattrs(
    const ghobject_t &oid,                ///< [in] object
    const std::map<std::string, ceph::buffer::list> &to_set,///< [in] attrs/values to set
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// remove xattrs in to_remove
  virtual int remove_xattrs(
    const ghobject_t &oid,               ///< [in] object
    const std::set<std::string> &to_remove,        ///< [in] attrs to remove
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;


  /// Clone keys from oid map to target map
  virtual int clone(
    const ghobject_t &oid,             ///< [in] object containing map
    const ghobject_t &target,           ///< [in] target of clone
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) { return 0; }

  /// Rename map because of name change
  virtual int rename(
    const ghobject_t &from,             ///< [in] object containing map
    const ghobject_t &to,               ///< [in] new name
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) { return 0; }

  /// For testing clone keys from oid map to target map using faster but more complex method
  virtual int legacy_clone(
    const ghobject_t &oid,             ///< [in] object containing map
    const ghobject_t &target,           ///< [in] target of clone
    const SequencerPosition *spos=0     ///< [in] sequencer position
    ) { return 0; }

  /// Ensure all previous writes are durable
  virtual int sync(
    const ghobject_t *oid=0,          ///< [in] object
    const SequencerPosition *spos=0   ///< [in] Sequencer
    ) { return 0; }

  virtual int check(std::ostream &out, bool repair = false, bool force = false) { return 0; }

  virtual void compact() {}

  typedef KeyValueDB::SimplestIteratorImpl ObjectMapIteratorImpl;
  typedef std::shared_ptr<ObjectMapIteratorImpl> ObjectMapIterator;
  virtual ObjectMapIterator get_iterator(const ghobject_t &oid) {
    return ObjectMapIterator();
  }

  virtual KeyValueDB *get_db() { return nullptr; }

  ObjectMap(CephContext* cct, KeyValueDB *db) : cct(cct), db(db) {}
  virtual ~ObjectMap() {}
};

#endif
