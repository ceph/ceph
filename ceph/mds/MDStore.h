
#ifndef __MDSTORE_H
#define __MDSTORE_H

#include "include/types.h"
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;

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
  bool fetch_dir( CDir *dir,
				  Context *c );
  bool fetch_dir_2( int result, 
					inodeno_t ino );
  
  bool commit_dir( CDir *dir,
				   Context *c );
  bool commit_dir_2( int result,
					 CDir *dir,
					 __uint64_t committed_version );
  
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
  void do_fetch_dir_2( int result, 
					   crope buffer,
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
