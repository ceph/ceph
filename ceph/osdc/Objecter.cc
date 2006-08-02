
#include "Objecter.h"
#include "osd/OSDMap.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"

#include <errno.h>

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_filer) cout << messenger->get_myaddr() << ".objecter "


// messages ------------------------------

void Objecter::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_OSD_OPREPLY:
	handle_osd_op_reply((MOSDOpReply*)m);
	break;
	
  case MSG_OSD_MAP:
	handle_osd_map((MOSDMap*)m);
	break;

  default:
	dout(1) << "don't know message type " << m->get_type() << endl;
	assert(0);
  }
}

void Objecter::handle_osd_map(MOSDMap *m)
{
  assert(osdmap); 

  if (m->get_last() <= osdmap->get_epoch()) {
	dout(3) << "handle_osd_map ignoring epochs [" 
			<< m->get_first() << "," << m->get_last() 
			<< "] <= " << osdmap->get_epoch() << endl;
  } 
  else {
	dout(3) << "handle_osd_map got epochs [" 
			<< m->get_first() << "," << m->get_last() 
			<< "] > " << osdmap->get_epoch()
			<< endl;

	set<pg_t> changed_pgs;
	set<pg_t> down_pgs;

	for (epoch_t e = osdmap->get_epoch() + 1;
		 e <= m->get_last();
		 e++) {
	  if (m->incremental_maps.count(e)) {
		dout(3) << "handle_osd_map decoding incremental epoch " << e << endl;
		OSDMap::Incremental inc;
		int off = 0;
		inc.decode(m->incremental_maps[e], off);
		osdmap->apply_incremental(inc);
	
		// notify messenger
		for (map<int,entity_inst_t>::iterator i = inc.new_down.begin();
			 i != inc.new_down.end();
			 i++) 
		  messenger->mark_down(MSG_ADDR_OSD(i->first), i->second);
		for (map<int,entity_inst_t>::iterator i = inc.new_up.begin();
			 i != inc.new_up.end();
			 i++) 
		  messenger->mark_up(MSG_ADDR_OSD(i->first), i->second);
		
	  }
	  else if (m->maps.count(e)) {
		dout(3) << "handle_osd_map decoding full epoch " << e << endl;
		osdmap->decode(m->maps[e]);
	  }
	  else {
		dout(3) << "handle_osd_map requesting missing epoch " << osdmap->get_epoch()+1 << endl;
		messenger->send_message(new MOSDGetMap(osdmap->get_epoch()), MSG_ADDR_MON(0));
		break;
	  }
	  
	  // scan pgs for changes
	  scan_pgs(changed_pgs, down_pgs);
		
	  assert(e == osdmap->get_epoch());
	}

	// kick requests who might be timing out on the wrong osds
	kick_requests(changed_pgs, down_pgs);
  }
  
  delete m;
}

void Objecter::scan_pgs(set<pg_t>& changed_pgs, set<pg_t>& down_pgs)
{
  for (hash_map<pg_t,PG>::iterator i = pg_map.begin();
	   i != pg_map.end();
	   i++) {
	pg_t pgid = i->first;
	PG& pg = i->second;

	int old = pg.primary;
	if (pg.calc_primary(pgid, osdmap)) {
	  if (osdmap->is_down(old)) {
		dout(10) << "scan_pgs pg " << hex << pgid << dec << " went down" << endl;
		down_pgs.insert(pgid);
	  } else {
		dout(10) << "scan_pgs pg " << hex << pgid << dec << " changed" << endl;
	  }
	  changed_pgs.insert(pgid);
	}
  }
}

void Objecter::kick_requests(set<pg_t>& changed_pgs, set<pg_t>& down_pgs) 
{
  for (set<pg_t>::iterator i = changed_pgs.begin();
	   i != changed_pgs.end();
	   i++) {
	pg_t pgid = *i;
	PG& pg = pg_map[pgid];

	// resubmit ops!
	set<tid_t> tids;
	tids.swap( pg.active_tids );
	close_pg( pgid );  // will pbly reopen, unless it's just commits we're missing
	
	for (set<tid_t>::iterator p = tids.begin();
		 p != tids.end();
		 p++) {
	  tid_t tid = *p;
	  
	  if (op_modify.count(tid)) {
		OSDModify *wr = op_modify[tid];
		op_modify.erase(tid);
		
		// WRITE
		if (wr->waitfor_ack.count(tid)) {
		  // resubmit
		  if (down_pgs.count(pgid) == 0) {
			dout(0) << "kick_requests resub WRNOOP " << tid << endl;
			modifyx_submit(wr, wr->waitfor_ack[tid], true);
		  } else {
			dout(0) << "kick_requests resub write " << tid << endl;
			modifyx_submit(wr, wr->waitfor_ack[tid]);
		  }
		  wr->waitfor_ack.erase(tid);
		  wr->waitfor_commit.erase(tid);
		}
		else if (wr->waitfor_commit.count(tid)) {
		  // fake it.  FIXME.
		  dout(0) << "kick_requests faking commit on write " << tid << endl;
		  wr->waitfor_ack.erase(tid);
		  if (wr->waitfor_ack.empty() && wr->onack) {
			wr->onack->finish(0);
			delete wr->onack;
			wr->onack = 0;
		  }
		  wr->waitfor_commit.erase(tid);
		  if (wr->waitfor_commit.empty()) {
			if (wr->oncommit) {
			  wr->oncommit->finish(0);
			  delete wr->oncommit;
			}
			delete wr;
		  }
		}
	  }

	  else if (op_read.count(tid)) {
		// READ
		OSDRead *rd = op_read[tid];
		op_read.erase(tid);
		dout(0) << "kick_requests resub read " << tid << endl;

		// resubmit
		readx_submit(rd, rd->ops[tid]);
		rd->ops.erase(tid);
	  }

	  else 
		assert(0);
	}		 
  }		 
}



void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  // read or modify?
  switch (m->get_op()) {
  case OSD_OP_READ:
	handle_osd_read_reply(m);
	return;
	
  case OSD_OP_WRITE:
  case OSD_OP_ZERO:
  case OSD_OP_WRLOCK:
  case OSD_OP_WRUNLOCK:
	handle_osd_modify_reply(m);
	return;
	
  default:
	assert(0);
  }
}


// read -----------------------------------


tid_t Objecter::read(object_t oid, off_t off, size_t len, bufferlist *bl, 
				   Context *onfinish)
{
  OSDRead *rd = new OSDRead(bl);
  rd->extents.push_back(ObjectExtent(oid, off, len));
  rd->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  readx(rd, onfinish);
  return last_tid;
}

int Objecter::readx(OSDRead *rd, Context *onfinish)
{
  rd->onfinish = onfinish;
  
  // issue reads
  for (list<ObjectExtent>::iterator it = rd->extents.begin();
	   it != rd->extents.end();
	   it++) 
	readx_submit(rd, *it);

  return 0;
}

void Objecter::readx_submit(OSDRead *rd, ObjectExtent &ex) 
{
  // find OSD
  PG &pg = get_pg( ex.pgid );

  // send
  last_tid++;
  MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						 ex.oid, ex.pgid, osdmap->get_epoch(), 
						 OSD_OP_READ);
  m->set_length(ex.length);
  m->set_offset(ex.start);
  dout(10) << "readx_submit tid " << last_tid << " to osd" << pg.primary
		   << " oid " << hex << ex.oid << dec  << " " << ex.start << "~" << ex.length
		   << " (" << ex.buffer_extents.size() << " buffer fragments)" << endl;

  if (pg.primary >= 0) 
	messenger->send_message(m, MSG_ADDR_OSD(pg.primary), 0);
	
  // add to gather set
  rd->ops[last_tid] = ex;
  op_read[last_tid] = rd;	

  pg.active_tids.insert(last_tid);
}


void Objecter::handle_osd_read_reply(MOSDOpReply *m) 
{
  // get pio
  tid_t tid = m->get_tid();
  dout(7) << "handle_osd_read_reply " << tid << endl;
  
  assert(op_read.count(tid));
  OSDRead *rd = op_read[ tid ];
  op_read.erase( tid );

  // remove from osd/tid maps
  PG& pg = get_pg( m->get_pg() );
  assert(pg.active_tids.count(tid));
  pg.active_tids.erase(tid);
  if (pg.active_tids.empty()) close_pg( m->get_pg() );
  
  // our op finished
  rd->ops.erase(tid);

  // success?
  if (m->get_result() == -EAGAIN) {
	dout(7) << " got -EAGAIN, resubmitting" << endl;
	readx_submit(rd, rd->ops[tid]);
	delete m;
	return;
  }
  assert(m->get_result() >= 0);

  // what buffer offset are we?
  dout(7) << " got frag from " << hex << m->get_oid() << dec << " "
		  << m->get_offset() << "~" << m->get_length()
		  << ", still have " << rd->ops.size() << " more ops" << endl;
  
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

	dout(7) << " " << bytes_read << " bytes " 
	  //<< rd->bl->length()
			<< endl;
	
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

tid_t Objecter::write(object_t oid, off_t off, size_t len, bufferlist &bl, 
				   Context *onack, Context *oncommit)
{
  OSDWrite *wr = new OSDWrite(bl);
  wr->extents.push_back(ObjectExtent(oid, off, len));
  wr->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  modifyx(wr, onack, oncommit);
  return last_tid;
}


// zero

tid_t Objecter::zero(object_t oid, off_t off, size_t len,  
					 Context *onack, Context *oncommit)
{
  OSDModify *z = new OSDModify(OSD_OP_ZERO);
  z->extents.push_back(ObjectExtent(oid, off, len));
  z->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  modifyx(z, onack, oncommit);
  return last_tid;
}




// generic modify -----------------------------------

int Objecter::modifyx(OSDModify *wr, Context *onack, Context *oncommit)
{
  wr->onack = onack;
  wr->oncommit = oncommit;

  // issue writes/whatevers
  for (list<ObjectExtent>::iterator it = wr->extents.begin();
	   it != wr->extents.end();
	   it++) 
	modifyx_submit(wr, *it);

  return 0;
}


void Objecter::modifyx_submit(OSDModify *wr, ObjectExtent &ex, bool wrnoop)
{
  // find
  PG &pg = get_pg( ex.pgid );
  //int osd = osdmap->get_pg_acting_primary( ex.pgid );
	
  // send
  last_tid++;
  MOSDOp *m = new MOSDOp(last_tid, messenger->get_myaddr(),
						 ex.oid, ex.pgid, osdmap->get_epoch(),
						 wrnoop ? OSD_OP_WRNOOP:wr->op);
  m->set_length(ex.length);
  m->set_offset(ex.start);
	
  // what type of op?
  switch (wr->op) {
  case OSD_OP_WRITE:
	if (!wrnoop) {
	  // map buffer segments into this extent
	  // (may be fragmented bc of striping)
	  bufferlist cur;
	  for (map<size_t,size_t>::iterator bit = ex.buffer_extents.begin();
		   bit != ex.buffer_extents.end();
		   bit++) {
		bufferlist thisbit;
		thisbit.substr_of(((OSDWrite*)wr)->bl, bit->first, bit->second);
		cur.claim_append(thisbit);
	  }
	  assert(cur.length() == ex.length);
	  m->set_data(cur);//.claim(cur);
	}
	break;
  }

  // add to gather set
  if (wr->onack)
	wr->waitfor_ack[last_tid] = ex;
  else
	m->set_want_ack(false);
  
  if (wr->oncommit || !wr->onack)    // wait for commit if neither callback is provided (sloppy user)           
	wr->waitfor_commit[last_tid] = ex;
  else
	m->set_want_commit(false);
  
  op_modify[last_tid] = wr;

  pg.active_tids.insert(last_tid);
  
  // send
  dout(10) << "modifyx_submit op " << wr->op << " tid " << last_tid
		   << " osd" << pg.primary << "  oid " << hex << ex.oid << dec 
		   << " " << ex.start << "~" << ex.length << endl;
  if (pg.primary >= 0)
	messenger->send_message(m, MSG_ADDR_OSD(pg.primary), 0);
}



void Objecter::handle_osd_modify_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();
  dout(7) << "handle_osd_modify_reply " << tid << " commit " << m->get_commit() << endl;
  assert(op_modify.count(tid));
  OSDModify *wr = op_modify[ tid ];

  Context *onack = 0;
  Context *oncommit = 0;

  PG &pg = get_pg( m->get_pg() );

  // ignore?
  if (pg.primary != m->get_source().num()) {
	dout(7) << " ignoring ack|commit from non-primary" << endl;
	delete m;
	return;
  }

  
  if (m->get_commit() ||            // commit closes out tid
	  wr->oncommit == 0 ||          // .. or ack if no commit is needed
	  m->get_result() == -EAGAIN) { // or if we got EAGAIN
	// remove from tid/osd maps
	assert(pg.active_tids.count(tid));
	pg.active_tids.erase(tid);
	if (pg.active_tids.empty()) close_pg( m->get_pg() );
  }

  /*
  // success?
  if (m->get_result() == -EAGAIN) {
	dout(7) << " got -EAGAIN, resubmitting" << endl;
	if (wr->waitfor_ack.count(tid)) {
	  modifyx_submit(wr, wr->waitfor_ack[tid]);
	  wr->waitfor_ack.erase(tid);
	} else if (wr->waitfor_commit.count(tid)) {
	  modifyx_submit(wr, wr->waitfor_commit[tid]);
	  wr->waitfor_commit.erase(tid);
	}
	else assert(0);
	delete m;
	return;
  }
  */
  assert(m->get_result() >= 0);


  // ack or commit?
  if (m->get_commit()) {
	// commit.
	//dout(15) << " handle_osd_write_reply commit on " << tid << endl;
	op_modify.erase( tid );
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
	  op_modify.erase( tid );      // no commit requested  (FIXME or ooo delivery)
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


