
#include <assert.h>

#include "Filer.h"
#include "OSDMap.h"

//#include "messages/MOSDRead.h"
//#include "messages/MOSDReadReply.h"
//#include "messages/MOSDWrite.h"
//#include "messages/MOSDWriteReply.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "msg/Messenger.h"

#include "include/Context.h"

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_filer) cout << "filer: "





Filer::Filer(Messenger *m, OSDMap *o)
{
  last_tid = 0;
  messenger = m;
  osdmap = o;
}

Filer::~Filer()
{
}

void Filer::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_OSD_OPREPLY:
	handle_osd_op_reply((MOSDOpReply*)m);
	break;
	
  default:
	dout(1) << "don't know message type " << m->get_type() << endl;
	assert(0);
  }
}



void Filer::handle_osd_map(MOSDMap *m)
{
  if (!osdmap ||
	  m->get_version() > osdmap->get_version()) {
	if (osdmap) {
	  dout(3) << "handle_osd_map got osd map version " << m->get_version() << " > " << osdmap->get_version() << endl;
	} else {
	  dout(3) << "handle_osd_map got osd map version " << m->get_version() << endl;
	}
	
	osdmap->decode(m->get_osdmap());
	
	// kick requests who might be timing out on the wrong osds
	// ** FIXME **

  } else {
	dout(3) << "handle_osd_map ignoring osd map version " << m->get_version() << " <= " << osdmap->get_version() << endl;
  }
}




/*
void Filer::queue_outgoing(Message *m, int osd)
{
  outgoing.push_back(pair<Message*,int>(m,osd));
}

void Filer::send_outgoing() 
{
  // send messages AFTER all my structs are ready (locking can make replies appear to arrive quickly!)
  for (list< pair<Message*,int> >::iterator it = outgoing.begin();
	   it != outgoing.end();
	   it++) {
	messenger->send_message(it->first, MSG_ADDR_OSD(it->second), 0);
  }
}
*/


// read

int
Filer::read(inode_t& inode,
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

  // map buffer into OSD extents
  osdmap->file_to_extents(inode, len, offset, p->extents);

  dout(7) << "osd read ino " << inode.ino << " len " << len << " off " << offset << " in " << p->extents.size() << " object extents" << endl;

  // issue reads
  for (list<OSDExtent>::iterator it = p->extents.begin();
	   it != p->extents.end();
	   it++) {
	last_tid++;
	
	// issue read
	MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						   it->oid, it->rg, osdmap->get_version(), 
						   OSD_OP_READ);
	m->set_length(it->len);
	m->set_offset(it->offset);
	dout(15) << " read on " << last_tid << " from oid " << it->oid << " off " << it->offset << " len " << it->len << " (" << it->buffer_extents.size() << " buffer bits)" << endl;
	messenger->send_message(m, MSG_ADDR_OSD(it->osd), 0);

	// add to gather set
	p->outstanding_ops.insert(last_tid);
	op_reads[last_tid] = p;
  }

  return 0;
}


void
Filer::handle_osd_read_reply(MOSDOpReply *m) 
{
  // get pio
  tid_t tid = m->get_tid();
  dout(15) << "handle_osd_read_reply on " << tid << endl;

  assert(op_reads.count(tid));
  PendingOSDRead_t *p = op_reads[ tid ];
  op_reads.erase( tid );

  // our op finished
  p->outstanding_ops.erase(tid);
  
  // what buffer offset are we?
  dout(7) << "got frag from " << m->get_oid() << " len " << m->get_length() << ", still have " << p->outstanding_ops.size() << " more ops" << endl;
  
  if (p->outstanding_ops.empty()) {
	// all done
	p->bytes_read = 0;
	p->read_result->clear();
	if (p->read_data.size()) {
	  dout(15) << "assembling frags" << endl;

	  /** FIXME This doesn't handle holes efficiently.
	   * It allocates zero buffers to fill whole buffer, and
	   * then discards trailing ones at the end.
	   *
	   * Actually, this whole thing is pretty messy with temporary bufferlist*'s all over
	   * the heap. 
	   */

	  // we have other fragments, assemble them all... blech!
	  p->read_data[m->get_oid()] = new bufferlist;
	  p->read_data[m->get_oid()]->claim( m->get_data() );

	  // map extents back into buffer
	  map<off_t, bufferlist*> by_off;  // buffer offset -> bufferlist

	  // for each object extent...
	  for (list<OSDExtent>::iterator eit = p->extents.begin();
		   eit != p->extents.end();
		   eit++) {
		bufferlist *ox_buf = p->read_data[eit->oid];
		unsigned ox_len = ox_buf->length();
		unsigned ox_off = 0;
		assert(ox_len <= eit->len);           

		// for each buffer extent we're mapping into...
		for (map<size_t,size_t>::iterator bit = eit->buffer_extents.begin();
			 bit != eit->buffer_extents.end();
			 bit++) {
		  dout(21) << "object " << eit->oid << " extent " << eit->offset << " len " << eit->len << " : ox offset " << ox_off << " -> buffer extent " << bit->first << " len " << bit->second << endl;
		  by_off[bit->first] = new bufferlist;

		  if (ox_off + bit->second <= ox_len) {
			// we got the whole bx
			by_off[bit->first]->substr_of(*ox_buf, ox_off, bit->second);
			if (p->bytes_read < bit->first + bit->second) 
			  p->bytes_read = bit->first + bit->second;
		  } else if (ox_off + bit->second > ox_len && ox_off < ox_len) {
			// we got part of this bx
			by_off[bit->first]->substr_of(*ox_buf, ox_off, (ox_len-ox_off));
			if (p->bytes_read < bit->first + ox_len-ox_off) 
			  p->bytes_read = bit->first + ox_len-ox_off;

			// zero end of bx
			dout(21) << "  adding some zeros to the end " << ox_off + bit->second-ox_len << endl;
			bufferptr z = new buffer(ox_off + bit->second - ox_len);
			memset(z.c_str(), 0, z.length());
			by_off[bit->first]->append( z );
		  } else {
			// we got none of this bx.  zero whole thing.
			assert(ox_off >= ox_len);
			dout(21) << "  adding all zeros for this bit " << bit->second << endl;
			bufferptr z = new buffer(bit->second);
			assert(z.length() == bit->second);
			memset(z.c_str(), 0, z.length());
			by_off[bit->first]->append( z );
		  }
		  ox_off += bit->second;
		}
		assert(ox_off == eit->len);
	  }

	  // sort and string bits together
	  for (map<off_t, bufferlist*>::iterator it = by_off.begin();
		   it != by_off.end();
		   it++) {
		assert(it->second->length());
		if (it->first < p->bytes_read) {
		  dout(21) << "  concat buffer frag off " << it->first << " len " << it->second->length() << endl;
		  p->read_result->claim_append(*(it->second));
		} else {
		  dout(21) << "  NO concat zero buffer frag off " << it->first << " len " << it->second->length() << endl;		  
		}
		delete it->second;
	  }

	  // trim trailing zeros?
	  if (p->read_result->length() > p->bytes_read) {
		dout(10) << " trimming off trailing zeros . bytes_read=" << p->bytes_read << " len=" << p->read_result->length() << endl;
		p->read_result->splice(p->bytes_read, p->read_result->length() - p->bytes_read);
		assert(p->bytes_read == p->read_result->length());
	  }
	  
	  // hose p->read_data bufferlist*'s
	  for (map<object_t, bufferlist*>::iterator it = p->read_data.begin();
		   it != p->read_data.end();
		   it++) {
		delete it->second;
	  }
	} else {
	  dout(15) << "  only one frag" << endl;

	  // only one fragment, easy
	  p->read_result->claim( m->get_data() );
	  p->bytes_read = p->read_result->length();
	}

	// finish, clean up
	Context *onfinish = p->onfinish;
	int result = p->bytes_read;

	dout(7) << "read " << result << " bytes " << p->read_result->length() << endl;

	// done
	delete p;   // del pendingOsdRead_t
	if (onfinish) {
	  onfinish->finish(result);
	  delete onfinish;
	}
  } else {
	// store my bufferlist for later assembling
	p->read_data[m->get_oid()] = new bufferlist;
	p->read_data[m->get_oid()]->claim( m->get_data() );
  }

  delete m;
}



// write

int 
Filer::write(inode_t& inode,
			 size_t len, 
			 size_t offset, 
			 bufferlist& bl,
			 int flags, 
			 Context *onfinish)
{
  last_tid++;

  // pending write record
  PendingOSDOp_t *p = new PendingOSDOp_t;
  p->onfinish = onfinish;
  
  // find data
  list<OSDExtent> extents;
  osdmap->file_to_extents(inode, len, offset, extents);

  dout(7) << "osd write ino " << inode.ino << " len " << len << " off " << offset << " in " << extents.size() << " extents" << endl;

  size_t off = 0;  // ptr into buffer

  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	last_tid++;
	
	// issue write
	MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						   it->oid, it->rg, osdmap->get_version(),
						   OSD_OP_WRITE);
	m->set_length(it->len);
	m->set_offset(it->offset);
	
	// map buffer segments into this extent
	// (may be fragmented bc of striping)
	bufferlist cur;
	for (map<size_t,size_t>::iterator bit = it->buffer_extents.begin();
		 bit != it->buffer_extents.end();
		 bit++) {
	  bufferlist thisbit;
	  thisbit.substr_of(bl, bit->first, bit->second);
	  cur.claim_append(thisbit);
	}
	assert(cur.length() == it->len);
	m->set_data(cur);//.claim(cur);

	off += it->len;

	// add to gather set
	p->outstanding_ops.insert(last_tid);
	op_writes[last_tid] = p;

	// send
	dout(15) << " write on " << last_tid << endl;
	messenger->send_message(m, MSG_ADDR_OSD(it->osd), 0);
  }

  return 0;
}


void
Filer::handle_osd_write_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();
  dout(15) << "handle_osd_write_reply on " << tid << endl;

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
  // updated cluster info?
  if (m->get_map_version() && 
	  m->get_map_version() > osdmap->get_version()) {
	dout(3) << "op reply has newer map " << m->get_map_version() << " > " << osdmap->get_version() << endl;
	osdmap->decode( m->get_osdmap() );
  }


  // read or write?
  switch (m->get_op()) {
  case OSD_OP_READ:
	handle_osd_read_reply(m);
	return;
  case OSD_OP_WRITE:
	handle_osd_write_reply(m);
	return;
  }


  // get pio
  tid_t tid = m->get_tid();
  dout(15) << "handle_osd_op_reply on " << tid << endl;

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


int Filer::truncate(inode_t& inode, 
					size_t new_size, size_t old_size,
					Context *onfinish)
{
  // pending write record
  PendingOSDOp_t *p = new PendingOSDOp_t;
  p->onfinish = onfinish;
  
  // find data
  list<OSDExtent> extents;
  osdmap->file_to_extents(inode, old_size, new_size, extents);

  dout(7) << "osd truncate ino " << inode.ino << " to new size " << new_size << " from old_size " << old_size << " in " << extents.size() << " extents" << endl;

  int n = 0;
  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	last_tid++;
	
	MOSDOp *m;
	if (it->offset == 0) {
	  // issue delete
	  m = new MOSDOp(last_tid, messenger->get_myaddr(),
					 it->oid, it->rg, osdmap->get_version(),
					 OSD_OP_DELETE);
	} else {
	  // issue a truncate
	  m = new MOSDOp(last_tid, messenger->get_myaddr(),
					 it->oid, it->rg, osdmap->get_version(),
					 OSD_OP_TRUNCATE);
	  m->set_length( it->offset );
	}
	messenger->send_message(m, MSG_ADDR_OSD(it->osd), 0);
	
	// add to gather set
	p->outstanding_ops.insert(last_tid);
	op_removes[last_tid] = p;
	n++;
  }

  if (n == 0) {
	delete p;
	if (onfinish) {
	  onfinish->finish(0);
	  delete onfinish;
	}
  }

  return 0;
}


/*
int Filer::probe_size(inodeno_t ino, 
					  OSDFileLayout& layout,
					  size_t *size, 
					  Context *onfinish)
{
  PendingOSDProbe_t *p = new PendingOSDProbe_t;
  p->final_size = size;
  p->onfinish = onfinish;

  p->cur_offset = 0;   // start at the beginning

  last_tid++;
  op_probes[ last_tid ] = p;

  // stat first object
  

  return 0;
}
*/


// mkfs on all osds, wipe everything.

int Filer::mkfs(Context *onfinish)
{
  dout(7) << "mkfs, wiping all OSDs" << endl;

  // pending write record
  PendingOSDOp_t *p = new PendingOSDOp_t;
  p->onfinish = onfinish;
  
  // send MKFS to osds
  set<int> ls;
  osdmap->get_all_osds(ls);
  
  for (set<int>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {

	++last_tid;

	// issue mkfs
	MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						   0, 0, osdmap->get_version(), 
						   OSD_OP_MKFS);
	messenger->send_message(m, MSG_ADDR_OSD(*it), 0);
	
	// add to gather set
	p->outstanding_ops.insert(last_tid);
	op_mkfs[last_tid] = p;
  }

  return 0;
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
  osdmap->file_to_extents(ino, len, offset, num_rep, extents);
  
  dout(7) << "osd zero ino " << ino << " len " << len << " off " << offset << " in " << extents.size() << " extents on " << num_rep << " replicas" << endl;
  
  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	int r = 0;   // pick a replica
	last_tid++;
	
	// issue zero
	MOSDOp *m;
	//if (it->len == 
	m = new MOSDOp(last_tid, messenger->get_myaddr(),
	it->oid, it->rg, osdmap->get_version(), 
				   OSD_OP_DELETE);
	it->len, it->offset);
	messenger->send_message(m, MSG_ADDR_OSD(it->osd), 0);
	
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
