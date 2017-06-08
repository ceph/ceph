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

#include "include/memory.h"
#include "include/unordered_map.h"

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include <errno.h>

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/config.h"
#include "common/debug.h"
#include "include/buffer.h"

#include "IndexManager.h"
#include "HashIndex.h"
#include "CollectionIndex.h"

#include "chain_xattr.h"

static int set_version(const char *path, uint32_t version) {
  bufferlist bl;
  ::encode(version, bl);
  return chain_setxattr(path, "user.cephos.collection_version", bl.c_str(),
		     bl.length(), true);
}

static int get_version(const char *path, uint32_t *version) {
  bufferptr bp(PATH_MAX);
  int r = chain_getxattr(path, "user.cephos.collection_version",
		      bp.c_str(), bp.length());
  if (r < 0) {
    if (r != -ENOENT) {
      *version = 0;
      return 0;
    } else {
      return r;
    }
  }
  bp.set_length(r);
  bufferlist bl;
  bl.push_back(bp);
  bufferlist::iterator i = bl.begin();
  ::decode(*version, i);
  return 0;
}

IndexManager::~IndexManager() {

  for (ceph::unordered_map<coll_t, CollectionIndex* > ::iterator it = col_indices.begin();
       it != col_indices.end(); ++it) {

    delete it->second;
    it->second = NULL;
  }
  col_indices.clear();
}


int IndexManager::init_index(coll_t c, const char *path, uint32_t version) {
  Mutex::Locker l(lock);
  int r = set_version(path, version);
  if (r < 0)
    return r;
  HashIndex index(c, path, g_conf->filestore_merge_threshold,
		  g_conf->filestore_split_multiple,
		  version,
		  g_conf->filestore_index_retry_probability);
  return index.init();
}

int IndexManager::build_index(coll_t c, const char *path, CollectionIndex **index) {
  if (upgrade) {
    // Need to check the collection generation
    int r;
    uint32_t version = 0;
    r = get_version(path, &version);
    if (r < 0)
      return r;

    switch (version) {
    case CollectionIndex::FLAT_INDEX_TAG:
    case CollectionIndex::HASH_INDEX_TAG: // fall through
    case CollectionIndex::HASH_INDEX_TAG_2: // fall through
    case CollectionIndex::HOBJECT_WITH_POOL: {
      // Must be a HashIndex
      *index = new HashIndex(c, path, g_conf->filestore_merge_threshold,
				   g_conf->filestore_split_multiple, version);
      return 0;
    }
    default: assert(0);
    }

  } else {
    // No need to check
    *index = new HashIndex(c, path, g_conf->filestore_merge_threshold,
				 g_conf->filestore_split_multiple,
				 CollectionIndex::HOBJECT_WITH_POOL,
				 g_conf->filestore_index_retry_probability);
    return 0;
  }
}

int IndexManager::get_index(coll_t c, const string& baseDir, Index *index) {

  Mutex::Locker l(lock);
  ceph::unordered_map<coll_t, CollectionIndex* > ::iterator it = col_indices.find(c);
  if (it == col_indices.end()) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/current/%s", baseDir.c_str(), c.to_str().c_str());
    CollectionIndex* colIndex = NULL;
    int r = build_index(c, path, &colIndex);
    if (r < 0)
      return r;
    col_indices[c] = colIndex;
    index->index = colIndex;
  } else {
    index->index = it->second;
  }
  return 0;
}
