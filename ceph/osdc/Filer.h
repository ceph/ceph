#ifndef __FILER_H
#define __FILER_H

/*** Filer
 *
 * client/mds interface to access "files" in OSD cluster
 *
 * generic non-blocking interface for reading/writing to osds, using
 * the file-to-object mappings defined by OSDCluster.
 *
 * "files" are identified by ino. 
 */

#include "include/Context.h"

#include "OSDCluster.h"

#include <set>
#include <map>
using namespace std;

#include <ext/hash_map>
#include <ext/rope>
using namespace __gnu_cxx;

typedef __uint64_t tid_t;


typedef struct {
  set<tid_t>           outstanding_ops;
  map<size_t, crope*>  finished_reads;
  crope               *buffer;           // final result goes here
  Context             *onfinish;
} PendingOSDRead_t;

typedef struct {
  set<tid_t>  outstanding_ops;
  Context    *onfinish;
} PendingOSDWrite_t;



class Filer {
  OSDCluster *osdcluster;     // what osds am i dealing with?
  
  __uint64_t         last_tid;
  hash_map<tid_t,PendingOSDRead_t*>  osd_reads;
  hash_map<tid_t,PendingOSDWrite_t*> osd_writes;   

 public:
  Filer(OSDCluster *osdcluster);
  ~Filer();


  // osd fun
 public:
  int read(inodeno_t ino,
		   size_t len, 
		   size_t offset, 
		   crope *buffer, 
		   Context *c);
 protected:
  void handle_osd_read_reply(class MOSDReadReply *m);
  
 public:
  int write(inodeno_t ino,
			size_t len, 
			size_t offset, 
			crope& buffer, 
			int flags, 
			Context *c);
 protected:
  void handle_osd_write_reply(class MOSDWriteReply *m);


 public:
  int zero(inodeno_t ino, size_t len, size_t offset, Context *c);   // delete, if len==offset==0
  
};



#endif
