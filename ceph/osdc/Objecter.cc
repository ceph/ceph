
#include "Objecter.h"
#include "OSDMap.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"


#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_filer) cout << MSG_ADDR_NICE(messenger->get_myaddr()) << ".objecter "


// messages ------------------------------

void Objecter::dispatch(Message *m)
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

void Objecter::handle_osd_map(MOSDMap *m)
{
  if (!osdmap ||
	  m->get_epoch() > osdmap->get_epoch()) {
	if (osdmap) {
	  dout(3) << "handle_osd_map got osd map epoch " << m->get_epoch() 
			  << " > " << osdmap->get_epoch() << endl;
	} else {
	  dout(3) << "handle_osd_map got osd map epoch " << m->get_epoch() 
			  << endl;
	}
	
	osdmap->decode(m->get_osdmap());
	
	// kick requests who might be timing out on the wrong osds
	// ** FIXME **
	
  } else {
	dout(3) << "handle_osd_map ignoring osd map epoch " << m->get_epoch() 
			<< " <= " << osdmap->get_epoch() << endl;
  }
}


void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  // updated cluster info?
  if (m->get_map_epoch() && 
	  m->get_map_epoch() > osdmap->get_epoch()) {
	dout(3) << "op reply has newer map " << m->get_map_epoch() << " > " << osdmap->get_epoch() << endl;
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

  case OSD_OP_ZERO:
	handle_osd_zero_reply(m);
	return;
	
  default:
	assert(0);
  }
  
}


// read -----------------------------------


int Objecter::read(object_t oid, off_t off, size_t len, bufferlist *bl, 
				   Context *onfinish)
{
  OSDRead *rd = new OSDRead(bl);
  rd->extents.push_back(ObjectExtent(oid, off, len));
  rd->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  return readx(rd, onfinish);
}

int Objecter::readx(OSDRead *rd, Context *onfinish)
{
  rd->onfinish = onfinish;
  
  // issue reads
  for (list<ObjectExtent>::iterator it = rd->extents.begin();
	   it != rd->extents.end();
	   it++) {
	// find OSD
	int osd = osdmap->get_pg_acting_primary( it->pgid );

	// send
	last_tid++;
	MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						   it->oid, it->pgid, osdmap->get_epoch(), 
						   OSD_OP_READ);
	m->set_length(it->length);
	m->set_offset(it->start);
	dout(15) << " read tid " << last_tid << " from osd" << osd 
			 << " oid " << hex << it->oid << dec  << " " << it->start << "~" << it->length
			 << " (" << it->buffer_extents.size() << " buffer fragments)" << endl;
	messenger->send_message(m, MSG_ADDR_OSD(osd), 0);
	
	// add to gather set
	rd->ops[last_tid] = *it;
	op_read[last_tid] = rd;	
  }
  
  return 0;
}

void Objecter::handle_osd_read_reply(MOSDOpReply *m) 
{
  // get pio
  tid_t tid = m->get_tid();
  dout(15) << "handle_osd_read_reply on " << tid << endl;
  
  assert(op_read.count(tid));
  OSDRead *rd = op_read[ tid ];
  op_read.erase( tid );

  // our op finished
  rd->ops.erase(tid);
  
  // what buffer offset are we?
  dout(7) << " got frag from " << hex << m->get_oid() << dec << " len " << m->get_length() << ", still have " << rd->ops.size() << " more ops" << endl;
  
  if (rd->ops.empty()) {
	// all done
	size_t bytes_read = 0;
	rd->bl->clear();
	
	if (rd->read_data.size()) {
	  dout(15) << " assembling frags" << endl;

	  /** FIXME This doesn't handle holes efficiently.
	   * It allocates zero buffers to fill whole buffer, and
	   * then discards trailing ones at the end.
	   *
	   * Actually, this whole thing is pretty messy with temporary bufferlist*'s all over
	   * the heap. 
	   */

	  // we have other fragments, assemble them all... blech!
	  rd->read_data[m->get_oid()] = new bufferlist;
	  rd->read_data[m->get_oid()]->claim( m->get_data() );

	  // map extents back into buffer
	  map<off_t, bufferlist*> by_off;  // buffer offset -> bufferlist

	  // for each object extent...
	  for (list<ObjectExtent>::iterator eit = rd->extents.begin();
		   eit != rd->extents.end();
		   eit++) {
		bufferlist *ox_buf = rd->read_data[eit->oid];
		unsigned ox_len = ox_buf->length();
		unsigned ox_off = 0;
		assert(ox_len <= eit->length);           

		// for each buffer extent we're mapping into...
		for (map<size_t,size_t>::iterator bit = eit->buffer_extents.begin();
			 bit != eit->buffer_extents.end();
			 bit++) {
		  dout(21) << " object " << hex << eit->oid << dec << " extent " << eit->start << "~" << eit->length << " : ox offset " << ox_off << " -> buffer extent " << bit->first << "~" << bit->second << endl;
		  by_off[bit->first] = new bufferlist;

		  if (ox_off + bit->second <= ox_len) {
			// we got the whole bx
			by_off[bit->first]->substr_of(*ox_buf, ox_off, bit->second);
			if (bytes_read < bit->first + bit->second) 
			  bytes_read = bit->first + bit->second;
		  } else if (ox_off + bit->second > ox_len && ox_off < ox_len) {
			// we got part of this bx
			by_off[bit->first]->substr_of(*ox_buf, ox_off, (ox_len-ox_off));
			if (bytes_read < bit->first + ox_len-ox_off) 
			  bytes_read = bit->first + ox_len-ox_off;

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
		assert(ox_off == eit->length);
	  }

	  // sort and string bits together
	  for (map<off_t, bufferlist*>::iterator it = by_off.begin();
		   it != by_off.end();
		   it++) {
		assert(it->second->length());
		if (it->first < bytes_read) {
		  dout(21) << "  concat buffer frag off " << it->first << " len " << it->second->length() << endl;
		  rd->bl->claim_append(*(it->second));
		} else {
		  dout(21) << "  NO concat zero buffer frag off " << it->first << " len " << it->second->length() << endl;		  
		}
		delete it->second;
	  }

	  // trim trailing zeros?
	  if (rd->bl->length() > bytes_read) {
		dout(10) << " trimming off trailing zeros . bytes_read=" << bytes_read 
				 << " len=" << rd->bl->length() << endl;
		rd->bl->splice(bytes_read, rd->bl->length() - bytes_read);
		assert(bytes_read == rd->bl->length());
	  }
	  
	  // hose p->read_data bufferlist*'s
	  for (map<object_t, bufferlist*>::iterator it = rd->read_data.begin();
		   it != rd->read_data.end();
		   it++) {
		delete it->second;
	  }
	} else {
	  dout(15) << "  only one frag" << endl;

	  // only one fragment, easy
	  rd->bl->claim( m->get_data() );
	  bytes_read = rd->bl->length();
	}

	// finish, clean up
	Context *onfinish = rd->onfinish;

	dout(7) << " " << bytes_read << " bytes " << rd->bl->length() << endl;
	
	// done
	delete rd;
	if (onfinish) {
	  onfinish->finish(bytes_read);
	  delete onfinish;
	}
  } else {
	// store my bufferlist for later assembling
	rd->read_data[m->get_oid()] = new bufferlist;
	rd->read_data[m->get_oid()]->claim( m->get_data() );
  }

  delete m;
}



// write ------------------------------------

int Objecter::write(object_t oid, off_t off, size_t len, bufferlist &bl, 
				   Context *onack, Context *oncommit)
{
  OSDWrite *wr = new OSDWrite(bl);
  wr->extents.push_back(ObjectExtent(oid, off, len));
  wr->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  return writex(wr, onack, oncommit);
}

int Objecter::writex(OSDWrite *wr, Context *onack, Context *oncommit)
{
  wr->onack = onack;
  wr->oncommit = oncommit;

  size_t off = 0;  // ptr into buffer

  // issue writes
  for (list<ObjectExtent>::iterator it = wr->extents.begin();
	   it != wr->extents.end();
	   it++) {
	// find
	int osd = osdmap->get_pg_acting_primary( it->pgid );
	
	// send
	last_tid++;
	MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						   it->oid, it->pgid, osdmap->get_epoch(),
						   OSD_OP_WRITE);
	m->set_length(it->length);
	m->set_offset(it->start);
	
	// map buffer segments into this extent
	// (may be fragmented bc of striping)
	bufferlist cur;
	for (map<size_t,size_t>::iterator bit = it->buffer_extents.begin();
		 bit != it->buffer_extents.end();
		 bit++) {
	  bufferlist thisbit;
	  thisbit.substr_of(wr->bl, bit->first, bit->second);
	  cur.claim_append(thisbit);
	}
	assert(cur.length() == it->length);
	m->set_data(cur);//.claim(cur);

	off += it->length;

	// add to gather set
	if (onack)
	  wr->waitfor_ack[last_tid] = *it;
	else
	  m->set_want_ack(false);

	if (oncommit || !onack)    // wait for commit if neither callback is provided (sloppy user)           
	  wr->waitfor_commit[last_tid] = *it;
	else
	  m->set_want_commit(false);

	op_write[last_tid] = wr;

	// send
	dout(10) << " write tid " << last_tid << " osd" << osd 
			 << "  oid " << hex << it->oid << dec << " " << it->start << "~" << it->length << endl;
	messenger->send_message(m, MSG_ADDR_OSD(osd), 0);
  }

  return 0;
}



void Objecter::handle_osd_write_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();
  dout(7) << "handle_osd_write_reply " << tid << " commit " << m->get_commit() << endl;
  assert(op_write.count(tid));
  OSDWrite *wr = op_write[ tid ];

  Context *onack = 0;
  Context *oncommit = 0;

  // ack or commit?
  if (m->get_commit()) {
	// commit.
	//dout(15) << " handle_osd_write_reply commit on " << tid << endl;
	op_write.erase( tid );
	wr->waitfor_ack.erase(tid);
	wr->waitfor_commit.erase(tid);

	if (wr->waitfor_commit.empty()) {
	  onack = wr->onack;
	  oncommit = wr->oncommit;
	  delete wr;
	}
  } else {
	// ack.
	//dout(15) << " handle_osd_write_reply ack on " << tid << endl;
	assert(wr->waitfor_ack.count(tid));
	wr->waitfor_ack.erase(tid);

	if (wr->waitfor_commit.empty()) {
	  op_write.erase( tid );      // no commit requested  (FIXME or ooo delivery)
	  assert(wr->oncommit == 0);
	}
	
	if (wr->waitfor_ack.empty()) {
	  onack = wr->onack;
	  wr->onack = 0;
	  if (wr->waitfor_commit.empty()) 
		delete wr;
	}
  }
  
  // do callbacks
  if (onack) {
	onack->finish(0);
	delete onack;
  }
  if (oncommit) {
	oncommit->finish(0);
	delete oncommit;
  }

  delete m;
}



// zero ---------------------------------------------------

int Objecter::zero(object_t oid, off_t off, size_t len,  
				   Context *onack, Context *oncommit)
{
  OSDZero *z = new OSDZero;
  z->extents.push_back(ObjectExtent(oid, off, len));
  z->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  return zerox(z, onack, oncommit);
}

int Objecter::zerox(OSDZero *z, Context *onack, Context *oncommit)
{
  z->onack = onack;
  z->oncommit = oncommit;

  size_t off = 0;  // ptr into buffer

  // issue writes
  for (list<ObjectExtent>::iterator it = z->extents.begin();
	   it != z->extents.end();
	   it++) {
	// find
	int osd = osdmap->get_pg_acting_primary( it->pgid );
	
	// send
	last_tid++;
	MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						   it->oid, it->pgid, osdmap->get_epoch(),
						   OSD_OP_ZERO);
	m->set_length(it->length);
	m->set_offset(it->start);
	
	off += it->length;

	// add to gather set
	if (onack)
	  z->waitfor_ack[last_tid] = *it;
	else
	  m->set_want_ack(false);

	if (oncommit || !onack)    // wait for commit if neither callback is provided (sloppy user)           
	  z->waitfor_commit[last_tid] = *it;
	else
	  m->set_want_commit(false);

	op_zero[last_tid] = z;

	// send
	dout(10) << " zero tid " << last_tid << " osd" << osd 
			 << "  oid " << hex << it->oid << dec << " " << it->start << "~" << it->length << endl;
	messenger->send_message(m, MSG_ADDR_OSD(osd), 0);
  }

  return 0;
}


void Objecter::handle_osd_zero_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();
  dout(7) << "handle_osd_zero_reply " << tid << " commit " << m->get_commit() << endl;
  assert(op_write.count(tid));
  OSDZero *z = op_zero[ tid ];

  Context *onack = 0;
  Context *oncommit = 0;

  // ack or commit?
  if (m->get_commit()) {
	// commit.
	//dout(15) << " handle_osd_write_reply commit on " << tid << endl;
	op_write.erase( tid );
	z->waitfor_ack.erase(tid);
	z->waitfor_commit.erase(tid);

	if (z->waitfor_commit.empty()) {
	  onack = z->onack;
	  oncommit = z->oncommit;
	  delete z;
	}
  } else {
	// ack.
	//dout(15) << " handle_osd_write_reply ack on " << tid << endl;
	assert(z->waitfor_ack.count(tid));
	z->waitfor_ack.erase(tid);

	if (z->waitfor_commit.empty()) {
	  op_write.erase( tid );      // no commit requested  (FIXME or ooo delivery)
	  assert(z->oncommit == 0);
	}
	
	if (z->waitfor_ack.empty()) {
	  onack = z->onack;
	  z->onack = 0;
	  if (z->waitfor_commit.empty()) 
		delete z;
	}
  }
  
  // do callbacks
  if (onack) {
	onack->finish(0);
	delete onack;
  }
  if (oncommit) {
	oncommit->finish(0);
	delete oncommit;
  }

  delete m;
}
