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
#ifndef OS_INDEXMANAGER_H
#define OS_INDEXMANAGER_H

#include "include/memory.h"
#include <map>

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/config.h"
#include "common/debug.h"

#include "CollectionIndex.h"
#include "HashIndex.h"
#include "FlatIndex.h"


/// Public type for Index
typedef ceph::shared_ptr<CollectionIndex> Index;
/**
 * Encapsulates mutual exclusion for CollectionIndexes.
 *
 * Allowing a modification (removal or addition of an object) to occur
 * while a read is occuring (lookup of an object's path and use of
 * that path) may result in the path becoming invalid.  Thus, during
 * the lifetime of a CollectionIndex object and any paths returned
 * by it, no other concurrent accesses may be allowed.
 *
 * This is enforced using shared_ptr.  A shared_ptr<CollectionIndex>
 * is returned from get_index.  Any paths generated using that object
 * carry a reference to the parrent index.  Once all
 * shared_ptr<CollectionIndex> references have expired, the destructor
 * removes the weak_ptr from col_indices and wakes waiters.
 */
class IndexManager {
  Mutex lock; ///< Lock for Index Manager
  Cond cond;  ///< Cond for waiters on col_indices
  bool upgrade;

  /// Currently in use CollectionIndices
  map<coll_t,ceph::weak_ptr<CollectionIndex> > col_indices;

  /// Cleans up state for c @see RemoveOnDelete
  void put_index(
    coll_t c ///< Put the index for c
    );

  /// Callback for shared_ptr release @see get_index
  class RemoveOnDelete {
  public:
    coll_t c;
    IndexManager *manager;
    RemoveOnDelete(coll_t c, IndexManager *manager) : 
      c(c), manager(manager) {}

    void operator()(CollectionIndex *index) {
      manager->put_index(c);
      delete index;
    }
  };

  /**
   * Index factory
   *
   * Encapsulates logic for handling legacy FileStore
   * layouts
   *
   * @param [in] c Collection for which to get index
   * @param [in] path Path to collection
   * @param [out] index Index for c
   * @return error code
   */
  int build_index(coll_t c, const char *path, Index *index);
public:
  /// Constructor
  IndexManager(bool upgrade) : lock("IndexManager lock"),
			       upgrade(upgrade) {}

  /**
   * Reserve and return index for c
   *
   * @param [in] c Collection for which to get index
   * @param [in] path Path to collection
   * @param [out] index Index for c
   * @return error code
   */
  int get_index(coll_t c, const char *path, Index *index);

  /**
   * Initialize index for collection c at path
   *
   * @param [in] c Collection for which to init Index
   * @param [in] path Path to collection
   * @param [in] filestore_version version of containing FileStore
   * @return error code
   */
  int init_index(coll_t c, const char *path, uint32_t filestore_version);
};

#endif
