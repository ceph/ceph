
#ifndef __MDSTORE_H
#define __MDSTORE_H

#include "include/types.h"
#include <ext/rope>
using namespace std;

class MDS;
class CInode;
class Context;
class Message;

class MDStore {
 protected:
  MDS *mds;
  

 public:
  MDStore(MDS *m) {
	mds = m;
  }

  // i/o
  bool fetch_dir( CInode *in,
				  Context *c );
  bool fetch_dir_2( int result, 
					crope buffer,
					inodeno_t ino );
  
  
  
  bool commit_dir( CInode *in,
				   Context *c );
  bool commit_dir_2( int result,
					 CInode *in,
					 Context *c,
					 __uint64_t committed_version );
  
  
  // process a message
  void proc_message( Message *m );
  
};

#endif
