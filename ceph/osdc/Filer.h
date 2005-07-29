#ifndef __FILER_H
#define __FILER_H

/*** Filer
 *
 * client/mds interface to access "files" in OSD cluster.
 *
 * generic non-blocking interface for reading/writing to osds, using
 * the file-to-object mappings defined by OSDMap.
 *
 * Filer also handles details of replication on OSDs (to the extent that 
 * it affects OSD clients)
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
#include "OSDMap.h"

class Context;
class Messenger;
class OSDMap;

/*** types ***/
typedef __uint64_t tid_t;

#define FILER_FLAG_TRUNCATE_AFTER_WRITE  1


/*** track pending operations ***/
typedef struct {
  set<tid_t>           outstanding_ops;
  size_t               orig_offset;
  list<OSDExtent>      extents;
  map<object_t, bufferlist*> read_data;  // bits of data as they come back

  bufferlist          *read_result;      // eventaully condensed into here.

  size_t               bytes_read;
  Context             *onfinish;
} PendingOSDRead_t;

typedef struct {
  set<tid_t>  outstanding_ops;
  Context    *onfinish;
} PendingOSDOp_t;

typedef struct {
  size_t     *final_size;
  size_t     cur_offset;
  Context    *onfinish;
} PendingOSDProbe_t;


/**** Filer interface ***/

class Filer : public Dispatcher {
  OSDMap *osdmap;     // what osds am i dealing with?
  Messenger  *messenger;
  
  __uint64_t         last_tid;
  hash_map<tid_t,PendingOSDRead_t*>  op_reads;
  hash_map<tid_t,PendingOSDOp_t*>    op_writes;   
  hash_map<tid_t,PendingOSDOp_t*>    op_removes;   
  hash_map<tid_t,PendingOSDOp_t*>    op_zeros;   
  hash_map<tid_t,PendingOSDProbe_t*> op_probes;
  hash_map<tid_t,PendingOSDOp_t*>    op_mkfs;

 public:
  Filer(Messenger *m, OSDMap *o);
  ~Filer();

  void dispatch(Message *m);

  bool is_active() {
	if (!op_reads.empty() ||
		!op_writes.empty() ||
		!op_zeros.empty() ||
		!op_probes.empty() ||
		!op_removes.empty()) return true;
	return false;
  }

  // osd fun
  int read(inodeno_t ino,
		   OSDFileLayout& layout,
		   size_t len, 
		   size_t offset, 
		   bufferlist *bl,   // ptr to data
		   Context *c);

  int write(inodeno_t ino,
			OSDFileLayout& layout,
			size_t len, 
			size_t offset, 
			bufferlist& bl,
			int flags, 
			Context *c);

  int probe_size(inodeno_t ino, 
				 OSDFileLayout& layout,
				 size_t *size, Context *c);

  int remove(inodeno_t ino,
			 OSDFileLayout& layout,
			 size_t old_size,
			 Context *c) {
	return truncate(ino, layout, 0, old_size, c);
  }
  int truncate(inodeno_t ino, 
			   OSDFileLayout& layout,
			   size_t new_size, size_t old_size, 
			   Context *c);

  //int zero(inodeno_t ino, size_t len, size_t offset, Context *c);   

  int mkfs(Context *c);
  
  void handle_osd_read_reply(class MOSDOpReply *m);
  void handle_osd_write_reply(class MOSDOpReply *m);
  void handle_osd_op_reply(class MOSDOpReply *m);
  
};



#endif
