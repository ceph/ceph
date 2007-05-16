// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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



#ifndef __MDSTORE_H
#define __MDSTORE_H

#include "include/types.h"
#include "include/buffer.h"

class MDS;
class CDir;
class Context;

class MDStore {
 protected:
  MDS *mds;


 public:
  MDStore(MDS *m) {
    mds = m;
  }

  
  // fetch
 public:
  void fetch_dir( CDir *dir, Context *c );
 protected:
  void fetch_dir_2( int result, inodeno_t ino );
  
  void fetch_dir_hash( CDir *dir,
                       Context *c,
                       int hashcode = -1);
  void fetch_dir_hash_2( bufferlist &bl,
                         inode_t& inode,
                         Context *c,
                         int which);
  friend class C_MDS_Fetch;
  friend class C_MDS_FetchHash;

  // commit
 public:
  void commit_dir( CDir *dir, Context *c );                      // commit current dir version to disk.
  void commit_dir( CDir *dir, __uint64_t version, Context *c );  // commit specified version to disk
 protected:
  void commit_dir_2( int result, CDir *dir, __uint64_t committed_version );
  
  // low level committers
  void commit_dir_slice( CDir *dir,
                         Context *c,
                         int hashcode = -1);
  void commit_dir_slice_2( int result,
                           CDir *dir,
                           Context *c,
                           __uint64_t version,
                           int hashcode );
  
  friend class C_MDS_CommitDirFinish;
  friend class C_MDS_CommitSlice;
};


#endif
