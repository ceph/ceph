
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
  case MSG_OSD_MKFS_ACK:
	handle_osd_mkfs_ack(m);
	break;
	
  case MSG_OSD_OPREPLY:
	handle_osd_op_reply((MOSDOpReply*)m);
	break;
	
  default:
	dout(1) << "don't know message type " << m->get_type() << endl;
	assert(0);
  }
}


bool Filer::is_active() 
{
  if (!op_reads.empty() ||
	  !op_modify.empty() ||
	  !op_probes.empty()) {
	for (hash_map<tid_t,PendingOSDRead_t*>::iterator it = op_reads.begin();
		 it != op_reads.end();
		 it++) dout(10) << " pending read op " << it->first << endl;
	for (hash_map<tid_t,PendingOSDOp_t*>::iterator it = op_modify.begin();
		 it != op_modify.end();
		 it++) dout(10) << " pending modify op " << it->first << endl;
	return true;
  }
  return false;
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


// ------------------------------------------------------------
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
  file_to_extents(inode, len, offset, p->extents);

  dout(7) << "osd read ino " << hex << inode.ino << dec << " len " << len << " off " << offset << " in " << p->extents.size() << " object extents" << endl;

  // issue reads
  for (list<OSDExtent>::iterator it = p->extents.begin();
	   it != p->extents.end();
	   it++) {
	last_tid++;
	
	// issue read
	MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						   it->oid, it->pg, osdmap->get_version(), 
						   OSD_OP_READ);
	m->set_length(it->len);
	m->set_offset(it->offset);
	dout(15) << " read tid " << last_tid << " from osd" << it->osd 
			 << " oid " << hex << it->oid << dec  << " off " << it->offset << " len " << it->len 
			 << " (" << it->buffer_extents.size() << " buffer fragments)" << endl;
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
  dout(7) << "got frag from " << hex << m->get_oid() << dec << " len " << m->get_length() << ", still have " << p->outstanding_ops.size() << " more ops" << endl;
  
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
		  dout(21) << "object " << hex << eit->oid << dec << " extent " << eit->offset << " len " << eit->len << " : ox offset " << ox_off << " -> buffer extent " << bit->first << " len " << bit->second << endl;
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




// ------------------------------------------------------------
// modify ops

// write

int 
Filer::write(inode_t& inode,
			 size_t len, 
			 size_t offset, 
			 bufferlist& bl,
			 int flags, 
			 Context *onack,
			 Context *onsafe)
{
  last_tid++;

  // pending write record
  PendingOSDOp_t *p = new PendingOSDOp_t;
  p->onack = onack;
  p->onsafe = onsafe;
  
  // find data
  list<OSDExtent> extents;
  file_to_extents(inode, len, offset, extents);

  dout(7) << "osd write ino " << hex << inode.ino << dec << " len " << len << " off " << offset << " in " << extents.size() << " extents" << endl;

  size_t off = 0;  // ptr into buffer

  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	last_tid++;
	
	// issue write
	MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						   it->oid, it->pg, osdmap->get_version(),
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
	if (true||onack)          // not impl. on OSD yet.
	  p->waitfor_ack.insert(last_tid);
	else
	  m->set_want_ack(false);

	if (onsafe) 
	  p->waitfor_safe.insert(last_tid);
	else
	  m->set_want_safe(false);

	op_modify[last_tid] = p;

	// send
	dout(15) << " write tid " << last_tid << " osd" << it->osd 
			 << "  oid " << hex << it->oid << dec << " off " << it->offset << " len " << it->len << endl;
	messenger->send_message(m, MSG_ADDR_OSD(it->osd), 0);
  }

  return 0;
}


void
Filer::handle_osd_modify_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();
  assert(op_modify.count(tid));
  PendingOSDOp_t *p = op_modify[ tid ];

  Context *onack = 0;
  Context *onsafe = 0;

  // ack or safe?
  if (m->get_safe()) {
	// safe.
	dout(15) << "handle_osd_modify_reply safe on " << tid << endl;
	op_modify.erase( tid );
	p->waitfor_ack.erase(tid);
	p->waitfor_safe.erase(tid);

	if (p->waitfor_safe.empty()) {
	  onack = p->onack;
	  onsafe = p->onsafe;
	  delete p;
	}
  } else {
	// ack.
	dout(15) << "handle_osd_modify_reply ack on " << tid << endl;
	p->waitfor_ack.erase(tid);
	if (p->waitfor_safe.empty())
	  op_modify.erase( tid );      // not safe requested

	if (p->waitfor_ack.empty()) {
	  onack = p->onack;
	  p->onack = 0;
	  if (p->waitfor_safe.empty()) {
		assert(p->onsafe == 0);   // should be null.. no safe requested    (FIXME unless ooo delivery!)
		delete p;
	  }
	}
  }

  // do callbacks
  if (onack) {
	onack->finish(0);
	delete onack;
  }
  if (onsafe) {
	onsafe->finish(0);
	delete onsafe;
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
  case OSD_OP_TRUNCATE:
  case OSD_OP_DELETE:
	handle_osd_modify_reply(m);
	return;

  default:
	assert(0);
  }
  
}


int Filer::truncate(inode_t& inode, 
					size_t new_size, size_t old_size,
					Context *onack,
					Context *onsafe)
{
  // pending write record
  PendingOSDOp_t *p = new PendingOSDOp_t;
  p->onack = onack;
  p->onsafe = onsafe;
  
  // find data
  list<OSDExtent> extents;
  file_to_extents(inode, old_size, new_size, extents);

  dout(7) << "osd truncate ino " << hex << inode.ino << dec << " to new size " << new_size << " from old_size " << old_size << " in " << extents.size() << " extents" << endl;

  int n = 0;
  for (list<OSDExtent>::iterator it = extents.begin();
	   it != extents.end();
	   it++) {
	last_tid++;
	
	MOSDOp *m;
	if (it->offset == 0) {
	  // issue delete
	  m = new MOSDOp(last_tid, messenger->get_myaddr(),
					 it->oid, it->pg, osdmap->get_version(),
					 OSD_OP_DELETE);
	} else {
	  // issue a truncate
	  m = new MOSDOp(last_tid, messenger->get_myaddr(),
					 it->oid, it->pg, osdmap->get_version(),
					 OSD_OP_TRUNCATE);
	  m->set_length( it->offset );
	}
	messenger->send_message(m, MSG_ADDR_OSD(it->osd), 0);
	
	// add to gather set
	p->waitfor_ack.insert(last_tid);
	p->waitfor_safe.insert(last_tid);
	op_modify[last_tid] = p;
	n++;
  }

  if (n == 0) {
	delete p;
	if (onack) {
	  onack->finish(0);
	  delete onack;
	}
	if (onsafe) {
	  onsafe->finish(0);
	  delete onsafe;
	}
  }

  return 0;
}



// -------------------------------------------
// mkfs on all osds, wipe everything.

int Filer::mkfs(Context *onfinish)
{
  dout(7) << "mkfs, wiping all OSDs" << endl;

  // send MKFS to osds
  set<int> ls;
  osdmap->get_all_osds(ls);
  
  for (set<int>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {

	++last_tid;

	// issue mkfs
	messenger->send_message(new MOSDMap(osdmap, true),
							MSG_ADDR_OSD(*it));
	pending_mkfs.insert(*it);
  }

  waiting_for_mkfs = onfinish;
  return 0;
}


void Filer::handle_osd_mkfs_ack(Message *m)
{
  int from = MSG_ADDR_NUM(m->get_source());

  assert(pending_mkfs.count(from));
  pending_mkfs.erase(from);

  if (pending_mkfs.empty()) {
	dout(2) << "done with mkfs" << endl;
	waiting_for_mkfs->finish(0);
	delete waiting_for_mkfs;
	waiting_for_mkfs = 0;
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
  file_to_extents(ino, len, offset, num_rep, extents);
  
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
	it->oid, it->pg, osdmap->get_version(), 
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
