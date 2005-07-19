
#ifndef __MDSTORE_H
#define __MDSTORE_H

#include "include/types.h"
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;

#include "include/bufferlist.h"

class MDS;
class CDir;
class Context;
class Message;

class MDStore {
 protected:
  MDS *mds;
  

 public:
  MDStore(MDS *m) {
	mds = m;
  }

  // basic interface (normal or unhashed)
  void fetch_dir( CDir *dir,
				  Context *c );
  void fetch_dir_2( int result, 
					inodeno_t ino );
  
  void commit_dir( CDir *dir, Context *c );                      // commit current dir version to disk.
  void commit_dir( CDir *dir, __uint64_t version, Context *c );  // commit specified version to disk
  void commit_dir_2( int result, CDir *dir, __uint64_t committed_version );
  
  // low level committer
  void do_commit_dir( CDir *dir,
					  Context *c,
					  int hashcode = -1);
  void do_commit_dir_2( int result,
						CDir *dir,
						Context *c,
						__uint64_t version,
						int hashcode );
  
  void do_fetch_dir( CDir *dir,
					 Context *c,
					 int hashcode = -1);
  void do_fetch_dir_2( bufferlist &bl,
					   inodeno_t ino,
					   Context *c,
					   int which);
  
  /*
  // hashing
  void hash_dir( CInode *in, Context *c );
  void unhash_dir( CInode *in, Context *c );
  */

  // process a message
  void proc_message( Message *m );
  
};

#endif
