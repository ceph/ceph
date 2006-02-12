// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */


#ifndef __MDSTORE_H
#define __MDSTORE_H

#include "include/types.h"
#include "include/bufferlist.h"

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
