
#include "Filer.h"


// OSD fun ------------------------

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

	// issue read
	MOSDRead *m = new MOSDRead(++last_tid, it->oid, it->len, it->offset);
	messenger->send_message(m, MSG_ADDR_OSD(it->osd[r]), 0);

	// add to gather set
	p->outstanding_ops.insert(last_tid);
	osd_reads[last_tid] = p;
  }
}


int Filer::handle_osd_read_reply(MOSDReadReply *m) 
{
  MOSDReadReply *m = (MOSDReadReply*)rawm;
  
  assert(m->get_result() >= 0);

  // get pio
  tid_t = m->get_tid();
  PendingOSDRead_t *p = osd_reads[ tid ];
  osd_reads.erase( tid );

  // our op finished
  p->outstanding_ops.erase(tid);

  if (p->outstanding_ops.empty()) {
	// all done
	p->buffer->clear();

	// assemble result
	p->buffer->append( m->get_buffer() );
	p->buffer = 0;
	long result = m->get_len();

	// finish, clean up
	Context *onfinish = p->onfinish;
	delete p;   // del pendingOsdRead_t
	delete m;   // del message

	if (onfinish) {
	  onfinish->finish(result);
	  delete onfinish;
	}
  } else {
	// partial result
	crope *partial = new crope;
	*partial = m->get_buffer();
	p->finished_reads[ m->get_offset() ] = buffer;
  }
}



// -- osd_write
int 
MDS::osd_write(int osd, 
			   object_t oid, 
			   size_t len, 
			   size_t offset, 
			   crope& buffer, 
			   int flags, 
			   Context *c)
{
  osd_last_tid++;

  MOSDWrite *m = new MOSDWrite(osd_last_tid,
							   oid,
							   len, offset,
							   buffer, flags);
  osd_writes[ osd_last_tid ] = c;

  dout(10) << "sending MOSDWrite " << m->get_type() << endl;
  messenger->send_message(m,
						  MSG_ADDR_OSD(osd),
						  0, MDS_PORT_MAIN);
}


int MDS::osd_write_finish(Message *rawm)
{
  MOSDWriteReply *m = (MOSDWriteReply *)rawm;

  Context *c = osd_writes[ m->get_tid() ];
  osd_writes.erase(m->get_tid());

  long result = m->get_result();
  delete m;

  dout(10) << " finishing osd_write" << endl;

  if (c) {
	c->finish(result);
	delete c;
  }
}
