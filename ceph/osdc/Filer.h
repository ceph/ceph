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

//#define FILER_FLAG_TRUNCATE_AFTER_WRITE  1


// from LSB to MSB
#define OID_ONO_BITS       30       // 1mb * 10^9 = 1 petabyte files
#define OID_INO_BITS       (64-30)  // 2^34 =~ 16 billion files


/** OSDExtent
 * for mapping (ino, offset, len) to a (list of) byte extents in objects on osds
 */
class OSDExtent {
 public:
  int         osd;       // (acting) primary osd
  object_t    oid;       // object id
  pg_t        pg;        // placement group
  size_t      offset, len;   // extent within the object
  map<size_t, size_t>  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)

  OSDExtent() : osd(0), oid(0), pg(0), offset(0), len(0) { }
};




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
  set<tid_t>  waitfor_ack;
  Context    *onack;
  set<tid_t>  waitfor_safe;
  Context    *onsafe;
} PendingOSDOp_t;

typedef struct {
  size_t     *final_size;
  size_t     cur_offset;
  Context    *onfinish;
} PendingOSDProbe_t;


/**** Filer interface ***/

class Filer : public Dispatcher {
  OSDMap     *osdmap;     // what osds am i dealing with?
  Messenger  *messenger;
  
  __uint64_t         last_tid;
  hash_map<tid_t,PendingOSDRead_t*>  op_reads;
  hash_map<tid_t,PendingOSDOp_t*>    op_modify;

  hash_map<tid_t,PendingOSDOp_t*>    op_probes;   

  set<int>   pending_mkfs;
  Context    *waiting_for_mkfs;

 public:
  Filer(Messenger *m, OSDMap *o);
  ~Filer();

  void dispatch(Message *m);

  bool is_active();

  // osd fun
  int read(inode_t& inode,
		   size_t len, 
		   size_t offset, 
		   bufferlist *bl,   // ptr to data
		   Context *c);

  int probe_size(inode_t& inode, 
				 size_t *size, Context *c);

  int write(inode_t& inode,
			size_t len, 
			size_t offset, 
			bufferlist& bl,
			int flags, 
			Context *onack,
			Context *onsafe);

  int remove(inode_t& inode,
			 size_t old_size,
			 Context *onack,
			 Context *onsafe) {
	return truncate(inode, 0, old_size, onack, onsafe);
  }
  int truncate(inode_t& ino, 
			   size_t new_size, size_t old_size, 
			   Context *onack,
			   Context *onsafe);

  //int zero(inodeno_t ino, size_t len, size_t offset, Context *c);   

  int mkfs(Context *c);
  void handle_osd_mkfs_ack(Message *m);

  void handle_osd_op_reply(class MOSDOpReply *m);
  void handle_osd_read_reply(class MOSDOpReply *m);
  void handle_osd_modify_reply(class MOSDOpReply *m);

  void handle_osd_map(class MOSDMap *m);
  


  /***** mapping *****/

  /* map (ino, ono) to an object name
	 (to be used on any osd in the proper replica group) */
  object_t file_to_object(inodeno_t ino,
						  size_t    ono) {  
	assert(ino < (1LL<<OID_INO_BITS));       // legal ino can't be too big
	assert(ono < (1LL<<OID_ONO_BITS));
	return (ino << OID_INO_BITS) + ono;
  }

  pg_t file_to_pg(inode_t& inode, size_t ono) {
	return osdmap->ps_nrep_to_pg( osdmap->object_to_ps( file_to_object(inode.ino, ono) ),
								  inode.layout.num_rep );
  }


  /* map (ino, offset, len) to a (list of) OSDExtents 
	 (byte ranges in objects on (primary) osds) */
  void file_to_extents(inode_t inode,
					   size_t len,
					   size_t offset,
					   list<OSDExtent>& extents) {
	/* we want only one extent per object!
	 * this means that each extent we read may map into different bits of the 
	 * final read buffer.. hence OSDExtent.buffer_extents
	 */
	map< object_t, OSDExtent > object_extents;

	// RUSHSTRIPE?
	if (inode.layout.policy == FILE_LAYOUT_CRUSH) {
	  // layout constant
	  size_t stripes_per_object = inode.layout.object_size / inode.layout.stripe_size;
	  
	  size_t cur = offset;
	  size_t left = len;
	  while (left > 0) {
		// layout into objects
		size_t blockno = cur / inode.layout.stripe_size;
		size_t stripeno = blockno / inode.layout.stripe_count;
		size_t stripepos = blockno % inode.layout.stripe_count;
		size_t objectsetno = stripeno / stripes_per_object;
		size_t objectno = objectsetno * inode.layout.stripe_count + stripepos;
		
		// find oid, extent
		OSDExtent *ex = 0;
		object_t oid = file_to_object( inode.ino, objectno );
		if (object_extents.count(oid)) 
		  ex = &object_extents[oid];
		else {
		  ex = &object_extents[oid];
		  ex->oid = oid;
		  ex->pg = file_to_pg( inode, objectno );
		  ex->osd = osdmap->get_pg_acting_primary( ex->pg );
		}
		
		// map range into object
		size_t block_start = (stripeno % stripes_per_object)*inode.layout.stripe_size;
		size_t block_off = cur % inode.layout.stripe_size;
		size_t max = inode.layout.stripe_size - block_off;
		
		size_t x_offset = block_start + block_off;
		size_t x_len;
		if (left > max)
		  x_len = max;
		else
		  x_len = left;
		
		if (ex->offset + ex->len == x_offset) {
		  // add to extent
		  ex->len += x_len;
		} else {
		  // new extent
		  assert(ex->len == 0);
		  assert(ex->offset == 0);
		  ex->offset = x_offset;
		  ex->len = x_len;
		}
		ex->buffer_extents[cur-offset] = x_len;
		
		//cout << "map: ino " << ino << " oid " << ex.oid << " osd " << ex.osd << " offset " << ex.offset << " len " << ex.len << " ... left " << left << endl;
		
		left -= x_len;
		cur += x_len;
	  }
	  
	  // make final list
	  for (map<object_t, OSDExtent>::iterator it = object_extents.begin();
		   it != object_extents.end();
		   it++) {
		extents.push_back(it->second);
	  }
	}
	/*else if (inode.layout.policy == FILE_LAYOUT_OSDLOCAL) {
	  // all in one object, on a specific OSD.
	  OSDExtent ex;
	  ex.osd = inode.layout.osd;
	  ex.oid = file_to_object( inode.ino, 0 );
	  ex.pg = PG_NONE;
	  ex.len = len;
	  ex.offset = offset;
	  ex.buffer_extents[0] = len;

	  extents.push_back(ex);
	  }*/
	else {
	  assert(0);
	}
  }

};



#endif
