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
#include <tr1/memory>

#include "osd/osd_types.h"
#include "include/object.h"
#include "ObjectStore.h"

/**
 * CollectionIndex provides an interface for manipulating indexed colelctions
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
    std::tr1::shared_ptr<CollectionIndex> parent_ref;
    /// Constructor
    Path(
      string path,                              ///< [in] Path to return.
      std::tr1::weak_ptr<CollectionIndex> ref)  ///< [in] weak_ptr to parent.
      : full_path(path), parent_ref(ref) {}
      
    /// Getter for the stored path.
    const char *path() const { return full_path.c_str(); }
  };
 public:
  /// Type of returned paths
  typedef std::tr1::shared_ptr<Path> IndexedPath;

  static const uint32_t FLAT_INDEX_TAG = 0;
  static const uint32_t HASH_INDEX_TAG = 1;
  static const uint32_t HASH_INDEX_TAG_2 = 2;
  /**
   * For tracking Filestore collection versions.
   *
   * @return Collection version represented by the Index implementation
   */
  virtual uint32_t collection_version() = 0;

  /** 
   * For setting the internal weak_ptr to a shared_ptr to this.
   *
   * @see IndexManager
   */
  virtual void set_ref(std::tr1::shared_ptr<CollectionIndex> ref) = 0;

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
    const hobject_t &hoid, ///< [in] Created object.
    const char *path       ///< [in] Path to created object.
    ) = 0;

  /**
   * Removes hoid from the collection
   *
   * @return Error Code, 0 for success
   */
  virtual int unlink(
    const hobject_t &hoid ///< [in] Object to remove
    ) = 0;

  /**
   * Gets the IndexedPath for hoid.
   *
   * @return Error Code, 0 for success
   */
  virtual int lookup(
    const hobject_t &hoid, ///< [in] Object to lookup
    IndexedPath *path,	   ///< [out] Path to object
    int *exist	           ///< [out] True if the object exists, else false
    ) = 0;

  /**
   * List contents of the collection
   *
   * @param [in] seq Snapid to list.
   * @param [in] max_count Max number to list (0 for no limit).
   * @param [out] ls Container for listed objects.
   * @param [in,out] last List handle.  0 for beginning.  Passing the same
   * cookie location will cause the next max_count to be listed.
   * @return Error code.  0 on success.
   */
  virtual int collection_list_partial(
    snapid_t seq,
    int max_count,
    vector<hobject_t> *ls, 
    collection_list_handle_t *last
    ) = 0;

  /// List contents of collection by hash
  virtual int collection_list_partial(
    const hobject_t &start, ///< [in] object at which to start
    int min_count,          ///< [in] get at least min_count objects
    int max_count,          ///< [in] return at most max_count objects
    vector<hobject_t> *ls,  ///< [out] Listed objects
    hobject_t *next         ///< [out] Next object to list
    ) = 0;

  /// List contents of collection.
  virtual int collection_list(
    vector<hobject_t> *ls ///< [out] Listed Objects
    ) = 0;

  /// Virtual destructor
  virtual ~CollectionIndex() {}
};

#endif
