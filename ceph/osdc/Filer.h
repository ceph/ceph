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

#include <set>
#include <map>
using namespace std;

#include <ext/hash_map>
#include <ext/rope>
using namespace __gnu_cxx;

#include "include/types.h"
#include "msg/Dispatcher.h"

class Context;
class Messenger;
class OSDCluster;

typedef __uint64_t tid_t;


/*** track pending operations ***/
typedef struct {
  set<tid_t>           outstanding_ops;
  map<size_t, crope*>  finished_reads;
  crope               *buffer;           // final result goes here
  Context             *onfinish;
} PendingOSDRead_t;

typedef struct {
  set<tid_t>  outstanding_ops;
  Context    *onfinish;
} PendingOSDOp_t;


/**** Filer interface ***/

class Filer : public Dispatcher {
  OSDCluster *osdcluster;     // what osds am i dealing with?
  Messenger *messenger;
  
  __uint64_t         last_tid;
  hash_map<tid_t,PendingOSDRead_t*>  osd_reads;
  hash_map<tid_t,PendingOSDOp_t*>    osd_writes;   
  hash_map<tid_t,PendingOSDOp_t*>    osd_zeros;   

 public:
  Filer(Messenger *m, OSDCluster *o);
  ~Filer() {}

  void dispatch(Message *m);

  // osd fun
  int read(inodeno_t ino,
		   size_t len, 
		   size_t offset, 
		   crope *buffer, 
		   Context *c);

  
  int write(inodeno_t ino,
			size_t len, 
			size_t offset, 
			crope& buffer, 
			int flags, 
			Context *c);

  int probe_size(inodeno_t ino, size_t *size, Context *c);
  int remove(inodeno_t ino, size_t size, Context *c);

  //int zero(inodeno_t ino, size_t len, size_t offset, Context *c);   

  void handle_osd_read_reply(class MOSDReadReply *m);
  void handle_osd_write_reply(class MOSDWriteReply *m);
  void handle_osd_op_reply(class MOSDOpReply *m);
  
};



#endif
