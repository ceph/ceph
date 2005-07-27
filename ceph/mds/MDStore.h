
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
						 inodeno_t ino,
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
