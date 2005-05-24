
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
			char *buffer,
			Context *onfinish) 
{
  // pending read record
  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->buffer = buffer;                // this way
  p->dataptr = 0; p->freeptr = 0;    // not this way
  p->orig_offset = offset;
  p->bytes_read = 0;
  p->onfinish = onfinish;

  issue_read(ino, len, offset, p);
}

int
Filer::read(inodeno_t ino,
			size_t len, 
			size_t offset, 
			char **dataptr,   // ptr to data
			char **freeptr,   // ptr to delete
			Context *onfinish) 
{
  // pending read record
  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->orig_offset = offset;
  p->bytes_read = 0;
  p->onfinish = onfinish;

  int nfrag = issue_read(ino, len, offset, p);

  if (nfrag == 1) {
	// one fragment, we can re-use message buffer.
	p->dataptr = dataptr; p->freeptr = freeptr;  // this way
	p->buffer = 0;                               // not this way
  } 
  else if (nfrag > 1) {
	// we need a new buffer, cuz we have multiple bits!
	p->buffer = new char[len];
	p->dataptr = 0; p->freeptr = 0;    // not this way

	*dataptr = p->buffer;
	*freeptr = p->buffer;
  }
  else {
	assert(len == 0);
	assert(0);   // wtf, zero byte read?
  }
}


int Filer::issue_read(inodeno_t ino, size_t len, size_t offset, PendingOSDRead_t *p)
{  
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
  PendingOSDRead_t *p = op_reads[ tid ];
  op_reads.erase( tid );

  if (!p->buffer) {
	// claim message buffer
	*p->freeptr = m->get_raw_message();
	*p->dataptr = m->get_buffer();
	m->clear_raw_message();
  } else {
	// copy result into buffer
	size_t off = p->read_off[m->get_oid()];
	dout(7) << "filer: got frag at " << off << " len " << m->get_len() << endl;
	memcpy(p->buffer + off, m->get_buffer(), m->get_len());
  }

  p->bytes_read += m->get_len();
  
  // FIXME this braeks for fragmented reads and holes
  /*
	if (m->get_len() < m->get_origlen()) {
	dout(7) << "zeroing gap" << endl;
	memset(p->buffer + off + m->get_len(), m->get_origlen() - m->get_len(), 0);
	}
  */

  delete m;

  // our op finished
  p->outstanding_ops.erase(tid);

  if (p->outstanding_ops.empty()) {
	// all done
	long result = p->bytes_read;

	// finish, clean up
	Context *onfinish = p->onfinish;
	delete p;   // del pendingOsdRead_t
	
	// done
	if (onfinish) {
	  onfinish->finish(result);
	  delete onfinish;
	}
  }
}



// write

int 
Filer::write(inodeno_t ino,
			 size_t len, 
			 size_t offset, 
			 const char *buffer,
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
	MOSDWrite *m = new MOSDWrite(last_tid, it->oid, it->len, it->offset,
								 buffer + off);

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
