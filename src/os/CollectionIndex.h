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

#ifndef OS_COLLECTIONINDEX_H
#define OS_COLLECTIONINDEX_H

#include <string>
#include <vector>
#include "include/memory.h"

#include "osd/osd_types.h"
#include "include/object.h"

/**
 * CollectionIndex provides an interface for manipulating indexed collections
 */
class CollectionIndex {
protected:
  /** 
   * Object encapsulating a returned path.
   *
   * A path to an object (existent or non-existent) becomes invalid
   * when a different object is created in the index.  Path stores
   * a shared_ptr to the CollectionIndex to keep the index alive
   * during its lifetime.
   * @see IndexManager
   * @see self_ref
   * @see set_ref
   */
  class Path {
  public:
    /// Returned path
    string full_path;
    /// Ref to parent Index
    ceph::shared_ptr<CollectionIndex> parent_ref;
    /// coll_t for parent Index
    coll_t parent_coll;

    /// Normal Constructor
    Path(
      string path,                              ///< [in] Path to return.
      ceph::weak_ptr<CollectionIndex> ref)  ///< [in] weak_ptr to parent.
      : full_path(path), parent_ref(ref), parent_coll(parent_ref->coll()) {}

    /// Debugging Constructor
    Path(
      string path,                              ///< [in] Path to return.
      coll_t coll)                              ///< [in] collection
      : full_path(path), parent_coll(coll) {}
      
    /// Getter for the stored path.
    const char *path() const { return full_path.c_str(); }

    /// Getter for collection
    coll_t coll() const { return parent_coll; }

    /// Getter for parent
    ceph::shared_ptr<CollectionIndex> get_index() const {
      return parent_ref;
    }
  };
 public:
  /// Type of returned paths
  typedef ceph::shared_ptr<Path> IndexedPath;

  static IndexedPath get_testing_path(string path, coll_t collection) {
    return IndexedPath(new Path(path, collection));
  }

  static const uint32_t FLAT_INDEX_TAG = 0;
  static const uint32_t HASH_INDEX_TAG = 1;
  static const uint32_t HASH_INDEX_TAG_2 = 2;
  static const uint32_t HOBJECT_WITH_POOL = 3;
  /**
   * For tracking Filestore collection versions.
   *
   * @return Collection version represented by the Index implementation
   */
  virtual uint32_t collection_version() = 0;

  /**
   * Returns the collection managed by this CollectionIndex
   */
  virtual coll_t coll() const = 0;

  /** 
   * For setting the internal weak_ptr to a shared_ptr to this.
   *
   * @see IndexManager
   */
  virtual void set_ref(ceph::shared_ptr<CollectionIndex> ref) = 0;

  /** 
   * Initializes the index.
   *
   * @return Error Code, 0 for success
   */
  virtual int init() = 0;

  /**
   * Cleanup before replaying journal
   *
   * Index implemenations may need to perform compound operations
   * which may leave the collection unstable if interupted.  cleanup
   * is called on mount to allow the CollectionIndex implementation
   * to stabilize.
   *
   * @see HashIndex
   * @return Error Code, 0 for success
   */
  virtual int cleanup() = 0;

  /**
   * Call when a file is created using a path returned from lookup.
   *
   * @return Error Code, 0 for success
   */
  virtual int created(
    const ghobject_t &oid, ///< [in] Created object.
    const char *path       ///< [in] Path to created object.
    ) = 0;

  /**
   * Removes oid from the collection
   *
   * @return Error Code, 0 for success
   */
  virtual int unlink(
    const ghobject_t &oid ///< [in] Object to remove
    ) = 0;

  /**
   * Gets the IndexedPath for oid.
   *
   * @return Error Code, 0 for success
   */
  virtual int lookup(
    const ghobject_t &oid, ///< [in] Object to lookup
    IndexedPath *path,	   ///< [out] Path to object
    int *exist	           ///< [out] True if the object exists, else false
    ) = 0;

  /**
   * Moves objects matching <match> in the lsb <bits>
   *
   * dest and this must be the same subclass
   *
   * @return Error Code, 0 for success
   */
  virtual int split(
    uint32_t match,                             //< [in] value to match
    uint32_t bits,                              //< [in] bits to check
    ceph::shared_ptr<CollectionIndex> dest  //< [in] destination index
    ) { assert(0); return 0; }


  /// List contents of collection by hash
  virtual int collection_list_partial(
    const ghobject_t &start, ///< [in] object at which to start
    int min_count,          ///< [in] get at least min_count objects
    int max_count,          ///< [in] return at most max_count objects
    snapid_t seq,           ///< [in] list only objects with snap >= seq
    vector<ghobject_t> *ls,  ///< [out] Listed objects
    ghobject_t *next         ///< [out] Next object to list
    ) = 0;

  /// List contents of collection.
  virtual int collection_list(
    vector<ghobject_t> *ls ///< [out] Listed Objects
    ) = 0;

  /// Call prior to removing directory
  virtual int prep_delete() { return 0; }

  /// Virtual destructor
  virtual ~CollectionIndex() {}
};

#endif
