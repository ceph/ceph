
#ifndef __MDSTORE_H
#define __MDSTORE_H

#include "MDCache.h"
#include "Context.h"
#include "Message.h"

class MDS;

class MDStore {
 protected:
  MDS *mds;
  

 public:
  MDStore(MDS *m) {
	mds = m;
  }

  // i/o
  /*
bool fetch_inode( mdloc_t where,
					inodeno_t ino,
					Context *c );
  */
  bool fetch_dir( CInode *in,
				  Context *c );
  bool fetch_dir_2( int result, 
					char *buf, 
					size_t buflen, 
					CInode *in, 
					Context *c );
  
  
  
  bool commit_dir( CInode *in,
				   Context *c );
  bool commit_dir_2( int result,
					 CInode *in,
					 Context *c,
					 __uint64_t committed_version );
  
  
  // process a message
  bool proc_message( Message *m ) { }
  
};

#endif
