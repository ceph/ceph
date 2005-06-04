
#include <assert.h>

#include "Filer.h"
#include "OSDCluster.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDWriteReply.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "msg/Messenger.h"

#include "include/Context.h"

#include "include/config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "filer: "





Filer::Filer(Messenger *m, OSDCluster *o)
{
  last_tid = 0;
  messenger = m;
  osdcluster = o;
}


void Filer::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_OSD_READREPLY:
	handle_osd_read_reply((MOSDReadReply*)m);
	break;
	
  case MSG_OSD_WRITEREPLY:
	handle_osd_write_reply((MOSDWriteReply*)m);
	break;

  case MSG_OSD_OPREPLY:
	handle_osd_op_reply((MOSDOpReply*)m);
	break;
	
  default:
	dout(1) << "don't know message type " << m->get_type() << endl;
	assert(0);
  }
}


// read

int 
Filer::read(inodeno_t ino,
			size_t len, 
			size_t offset, 
			bufferlist *bl,
			Context *onfinish) 
{
  // pending read record
  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->read_result = bl;
  p->orig_offset = offset;
  p->bytes_read = 0;
  p->onfinish = onfinish;

  int num_rep = 1;          // FIXME

  // find data
  list<OSDExtent> extents;
  osdcluster->file_to_extents(ino, len, offset, num_rep, extents);

  dout(7) << "osd read ino " << ino << " len " << len << " off " << offset << " in " << extents.size() << " extents on " << num_rep << " replicas" << endl;

  int nfrag = 0;
  off_t off = 0;
  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	int r = 0;   // pick a replica
	last_tid++;
	
	// issue read
	MOSDRead *m = new MOSDRead(last_tid, it->oid, it->len, it->offset);
	messenger->send_message(m, MSG_ADDR_OSD(it->osds[r]), 0);

	// note offset into read buffer
	p->read_off[it->oid] = off;
	off += it->len;

	// add to gather set
	p->outstanding_ops.insert(last_tid);
	op_reads[last_tid] = p;
	nfrag++;
  }

  return nfrag;
}


void
Filer::handle_osd_read_reply(MOSDReadReply *m) 
{
  // get pio
  tid_t tid = m->get_tid();
  assert(op_reads.count(tid));
  PendingOSDRead_t *p = op_reads[ tid ];
  op_reads.erase( tid );

  // copy result into buffer
  size_t off = p->read_off[m->get_oid()];
  dout(7) << "filer: got frag at " << off << " len " << m->get_len() << endl;
  
  // our op finished
  p->outstanding_ops.erase(tid);
  
  if (p->outstanding_ops.empty()) {
	// all done
	p->read_result->clear();
	if (p->read_data.size()) {
	  // we have other fragments, assemble them all... blech!
	  p->read_data[off] = new bufferlist;
	  p->read_data[off]->claim( m->get_data() );

	  // sort and string them together
	  for (map<off_t, bufferlist*>::iterator it = p->read_data.begin();
		   it != p->read_data.end();
		   it++) {
		p->read_result->claim_append(*(it->second));
	  }
	} else {
	  // only one fragment, easy
	  p->read_result->claim( m->get_data() );
	}

	// finish, clean up
	Context *onfinish = p->onfinish;
	delete p;   // del pendingOsdRead_t
	
	int result = p->read_result->length(); // assume success

	// done
	if (onfinish) {
	  onfinish->finish(result);
	  delete onfinish;
	}
  } else {
	// store my bufferlist for later assembling
	p->read_data[off] = new bufferlist;
	p->read_data[off]->claim( m->get_data() );
  }

  delete m;
}



// write

int 
Filer::write(inodeno_t ino,
			 size_t len, 
			 size_t offset, 
			 bufferlist& bl,
			 int flags, 
			 Context *onfinish)
{
  last_tid++;
  int num_rep = 1;

  // pending write record
  PendingOSDOp_t *p = new PendingOSDOp_t;
  p->onfinish = onfinish;
  
  // find data
  list<OSDExtent> extents;
  osdcluster->file_to_extents(ino, len, offset, num_rep, extents);

  dout(7) << "osd write ino " << ino << " len " << len << " off " << offset << " in " << extents.size() << " extents on " << num_rep << " replicas" << endl;

  size_t off = 0;  // ptr into buffer

  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	int r = 0;   // pick a replica
	last_tid++;
	
	// issue write
	MOSDWrite *m = new MOSDWrite(last_tid, it->oid, it->len, it->offset);

	bufferlist cur;
	cur.substr_of(bl, off, it->len);
	m->set_data(cur);

	off += it->len;

	messenger->send_message(m, MSG_ADDR_OSD(it->osds[r]), 0);
	
	// add to gather set
	p->outstanding_ops.insert(last_tid);
	op_writes[last_tid] = p;
  }
}


void
Filer::handle_osd_write_reply(MOSDWriteReply *m)
{
  // get pio
  tid_t tid = m->get_tid();
  assert(op_writes.count(tid));
  PendingOSDOp_t *p = op_writes[ tid ];
  op_writes.erase( tid );

  // our op finished
  p->outstanding_ops.erase(tid);

  if (p->outstanding_ops.empty()) {
	// all done
	Context *onfinish = p->onfinish;
	delete p;

	if (onfinish) {
	  onfinish->finish(0);
	  delete onfinish;
	}
  }
  delete m;
}



// ....... 

void
Filer::handle_osd_op_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();

  PendingOSDOp_t *p = 0;

  if (op_removes.count(tid)) {
	// remove op
	p = op_removes[tid];
	op_removes.erase(tid);
  }
  else if (op_mkfs.count(tid)) {
	// mkfs op
	p = op_mkfs[tid];
	op_mkfs.erase(tid);
  }
  else
	assert(0); // why did i get this message?

  if (p) {  
	// our op finished
	p->outstanding_ops.erase(tid);
	
	if (p->outstanding_ops.empty()) {
	  dout(12) << "gather done, finishing" << endl;
	  
	  // all done
	  Context *onfinish = p->onfinish;
	  delete p;
	  
	  if (onfinish) {
		onfinish->finish(0);
		delete onfinish;
	  }
	} else {
	  dout(12) << "still waiting on tids " << p->outstanding_ops << endl;
	}

  }
  delete m;
}


int Filer::remove(inodeno_t ino, size_t size, Context *onfinish)
{
  int num_rep = 1;

  // pending write record
  PendingOSDOp_t *p = new PendingOSDOp_t;
  p->onfinish = onfinish;
  
  // find data
  list<OSDExtent> extents;
  osdcluster->file_to_extents(ino, size, 0, num_rep, extents);

  dout(7) << "osd remove ino " << ino << " size " << size << " in " << extents.size() << " extents on " << num_rep << " replicas" << endl;

  size_t off = 0;  // ptr into buffer

  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	int r = 0;   // pick a replica
	last_tid++;
	
	for (int r=0;r<num_rep; r++) {
	  // issue delete
	  MOSDOp *m = new MOSDOp(last_tid, it->oid, OSD_OP_DELETE);
	  messenger->send_message(m, MSG_ADDR_OSD(it->osds[r]), 0);
	
	  // add to gather set
	  p->outstanding_ops.insert(last_tid);
	  op_removes[last_tid] = p;
	}
  }
}


int Filer::probe_size(inodeno_t ino, size_t *size, Context *onfinish)
{
  PendingOSDProbe_t *p = new PendingOSDProbe_t;
  p->final_size = size;
  p->onfinish = onfinish;

  p->cur_offset = 0;   // start at the beginning

  last_tid++;
  op_probes[ last_tid ] = p;

  // stat first object
  


}



// mkfs on all osds, wipe everything.

int Filer::mkfs(Context *onfinish)
{
  int num_rep = 1;
  
  dout(7) << "mkfs, wiping all OSDs" << endl;

  // pending write record
  PendingOSDOp_t *p = new PendingOSDOp_t;
  p->onfinish = onfinish;
  
  // send MKFS to osds
  set<int> ls;
  osdcluster->get_all_osds(ls);
  
  for (set<int>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {

	++last_tid;

	// issue mkfs
	MOSDOp *m = new MOSDOp(last_tid, 0, OSD_OP_MKFS);
	messenger->send_message(m, MSG_ADDR_OSD(*it), 0);
	
	// add to gather set
	p->outstanding_ops.insert(last_tid);
	op_mkfs[last_tid] = p;
  }
}



/*


int Filer::zero(inodeno_t ino, 
				size_t len, 
				size_t offset, 
				Context *onfinish)
{

  // search and destroy, if len==offset==0
  if (len == 0 && offset == 0) {
	// search and destroy

	assert(0);
  }

  // zero/delete
  last_tid++;
  int num_rep = 1;
  
  // pending write record
  PendingOSDOp_t *p = new PendingOSDOp_t;
  p->onfinish = onfinish;
  
  // find data
  list<OSDExtent> extents;
  osdcluster->file_to_extents(ino, len, offset, num_rep, extents);
  
  dout(7) << "osd zero ino " << ino << " len " << len << " off " << offset << " in " << extents.size() << " extents on " << num_rep << " replicas" << endl;
  
  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	int r = 0;   // pick a replica
	last_tid++;
	
	// issue zero
	MOSDOp *m;
	if (it->len == new MOSDOp(last_tid, it->oid, OSD_OP_DELETE);
	it->len, it->offset);
	messenger->send_message(m, MSG_ADDR_OSD(it->osds[r]), 0);
	
	// add to gather set
	p->outstanding_ops.insert(last_tid);
	op_zeros[last_tid] = p;
  }
}


void
Filer::handle_osd_op_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();

  
  if (PendingOSDOp_t *p = op_zeros[ tid ])
  op_zeros.erase( tid );

  // our op finished
  p->outstanding_ops.erase(tid);

  if (p->outstanding_ops.empty()) {
	// all done
	Context *onfinish = p->onfinish;
	delete p;

	if (onfinish) {
	  onfinish->finish(0);
	  delete onfinish;
	}
  }
  delete m;
}

*/
