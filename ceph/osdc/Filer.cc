
#include <assert.h>

#include "Filer.h"
#include "OSDCluster.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDWriteReply.h"
#include "messages/MOSDOp.h"

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
			crope *buffer, 
			Context *onfinish) 
{
  int num_rep = 1;          // FIXME

  // pending read record
  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->buffer = buffer;
  p->onfinish = onfinish;
  
  // find data
  list<OSDExtent> extents;
  osdcluster->file_to_extents(ino, len, offset, num_rep, extents);

  dout(7) << "osd read ino " << ino << " len " << len << " off " << offset << " in " << extents.size() << " extents on " << num_rep << " replicas" << endl;

  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	int r = 0;   // pick a replica
	last_tid++;
	
	// issue read
	MOSDRead *m = new MOSDRead(last_tid, it->oid, it->len, it->offset);
	messenger->send_message(m, MSG_ADDR_OSD(it->osds[r]), 0);

	// add to gather set
	p->outstanding_ops.insert(last_tid);
	osd_reads[last_tid] = p;
  }
}


void
Filer::handle_osd_read_reply(MOSDReadReply *m) 
{
  // get pio
  tid_t tid = m->get_tid();
  PendingOSDRead_t *p = osd_reads[ tid ];
  osd_reads.erase( tid );

  // our op finished
  p->outstanding_ops.erase(tid);

  if (p->outstanding_ops.empty()) {
	// all done

	// assemble result, append to buffer
	if (p->finished_reads.empty()) {
	  // single read, easy
	  p->buffer->append( m->get_buffer() );
	} else {
	  // multiple reads
	  crope *partial = new crope;
	  *partial = m->get_buffer();
	  p->finished_reads[ m->get_offset() ] = partial;
	  
	  for (map<size_t, crope*>::iterator it = p->finished_reads.begin();
		   it != p->finished_reads.end();
		   it++) {
		p->buffer->append( *it->second );    // FIXME: fill in holes!
		delete it->second;
	  }
	}

	long result = p->buffer->length();

	// finish, clean up
	Context *onfinish = p->onfinish;
	delete p;   // del pendingOsdRead_t
	delete m;

	// done
	if (onfinish) {
	  onfinish->finish(result);
	  delete onfinish;
	}
  } else {
	// partial result
	crope *partial = new crope;
	*partial = m->get_buffer();
	p->finished_reads[ m->get_offset() ] = partial;
	delete m;
  }
}



// write

int 
Filer::write(inodeno_t ino,
				 size_t len, 
				 size_t offset, 
				 crope& buffer, 
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
	MOSDWrite *m;
	if (off == 0 && it->len == len) {
	  // one shot
	  m = new MOSDWrite(last_tid, it->oid, it->len, it->offset,
						buffer);
	} else {
	  // partial
	  crope partial = buffer.substr(off, it->len);
	  off += it->len;
	  m = new MOSDWrite(last_tid, it->oid, it->len, it->offset,
						partial);
	}
	messenger->send_message(m, MSG_ADDR_OSD(it->osds[r]), 0);
	
	// add to gather set
	p->outstanding_ops.insert(last_tid);
	osd_writes[last_tid] = p;
  }
}


void
Filer::handle_osd_write_reply(MOSDWriteReply *m)
{
  // get pio
  tid_t tid = m->get_tid();
  PendingOSDOp_t *p = osd_writes[ tid ];
  osd_writes.erase( tid );

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
	osd_zeros[last_tid] = p;
  }
}


void
Filer::handle_osd_op_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();
  PendingOSDOp_t *p = osd_zeros[ tid ];
  osd_zeros.erase( tid );

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
