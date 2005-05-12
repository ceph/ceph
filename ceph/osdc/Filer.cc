
#include "Filer.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDWriteReply.h"
#include "messages/MOSDOp.h"


// read


int 
Filer::read(inodeno_t ino,
			size_t len, 
			size_t offset, 
			crope *buffer, 
			Context *c) 
{
  int num_rep = 1;          // FIXME

  // pending read record
  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->buffer = buffer;
  p->context = c;
  
  // find data
  list<OSDExtent> extents;
  osdcluster->file_to_extents(ino, len, offset, num_rep, extents);

  dout(7) << "osd read in " << extents.size() << " object extents on " << num_rep << " replicas" << endl;

  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	int r = 0;   // pick a replica
	last_tid++;
	
	// issue read
	MOSDRead *m = new MOSDRead(last_tid, it->oid, it->len, it->offset);
	messenger->send_message(m, MSG_ADDR_OSD(it->osd[r]), 0);

	// add to gather set
	p->outstanding_ops.insert(last_tid);
	osd_reads[last_tid] = p;
  }
}


int 
Filer::handle_osd_read_reply(MOSDReadReply *m) 
{

  // get pio
  tid_t = m->get_tid();
  PendingOSDRead_t *p = osd_reads[ tid ];
  osd_reads.erase( tid );

  // our op finished
  p->outstanding_ops.erase(tid);

  if (p->outstanding_ops.empty()) {
	// all done
	// assemble result

	p->buffer->clear();
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
  PendingOSDWrite_t *p = new PendingOSDWrite_t;
  p->onfinish = onfinish;
  
  // find data
  list<OSDExtent> extents;
  osdcluster->file_to_extents(ino, len, offset, num_rep, extents);

  dout(7) << "osd write in " << extents.size() << " object extents on " << num_rep << " replicas" << endl;

  size_t off = offset;  // ptr into buffer

  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	int r = 0;   // pick a replica
	last_tid++;
	
	// issue write
	crope partial = buffer.substr(off, it->len);
	off += it->len;
	
	MOSDWrite *m = new MOSDWrite(last_tid, it->oid, it->len, it->offset,
								 partial);
	messenger->send_message(m, MSG_ADDR_OSD(it->osd[r]), 0);
	
	// add to gather set
	p->outstanding_ops.insert(last_tid);
	osd_writes[last_tid] = p;
  }
}


int Filer::handle_osd_write_reply(MOSDWriteReply *m)
{
  // get pio
  tid_t = m->get_tid();
  PendingOSDWrite_t *p = osd_writes[ tid ];
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
