
#include "Objecter.h"
#include "osd/OSDMap.h"
#include "mon/MonMap.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"

#include "messages/MOSDFailure.h"

#include <errno.h>

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_objecter) cout << g_clock.now() << " " << messenger->get_myname() << ".objecter "
#define derr(x)  if (x <= g_conf.debug || x <= g_conf.debug_objecter) cerr << g_clock.now() << " " << messenger->get_myname() << ".objecter "


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
          messenger->mark_down(i->second.addr);
        
      }
      else if (m->maps.count(e)) {
        dout(3) << "handle_osd_map decoding full epoch " << e << endl;
        osdmap->decode(m->maps[e]);
      }
      else {
        dout(3) << "handle_osd_map requesting missing epoch " << osdmap->get_epoch()+1 << endl;
        int mon = monmap->pick_mon();
        messenger->send_message(new MOSDGetMap(osdmap->get_epoch()), 
                                monmap->get_inst(mon));
        break;
      }
      
      // scan pgs for changes
      scan_pgs(changed_pgs);
        
      assert(e == osdmap->get_epoch());
    }

    // kick requests who might be timing out on the wrong osds
    if (!changed_pgs.empty())
      kick_requests(changed_pgs);
  }
  
  delete m;
}

void Objecter::scan_pgs(set<pg_t>& changed_pgs)
{
  dout(10) << "scan_pgs" << endl;

  for (hash_map<pg_t,PG>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    pg_t pgid = i->first;
    PG& pg = i->second;
    
    // calc new.
    vector<int> other;
    osdmap->pg_to_acting_osds(pgid, other);

    if (other == pg.acting) 
      continue; // no change.
    
    other.swap(pg.acting);

    if (g_conf.osd_rep == OSD_REP_PRIMARY) {
      // same primary?
      if (!other.empty() &&
          !pg.acting.empty() &&
          other[0] == pg.acting[0]) 
        continue;
    }
    else if (g_conf.osd_rep == OSD_REP_SPLAY) {
      // same primary and acker?
      if (!other.empty() &&
          !pg.acting.empty() &&
          other[0] == pg.acting[0] &&
          other[other.size() > 1 ? 1:0] == pg.acting[pg.acting.size() > 1 ? 1:0]) 
        continue;
    }
    else if (g_conf.osd_rep == OSD_REP_CHAIN) {
      // any change is significant.
    }
    
    // changed significantly.
    dout(10) << "scan_pgs pg " << pgid 
             << " (" << pg.active_tids << ")"
             << " " << other << " -> " << pg.acting
             << endl;
    changed_pgs.insert(pgid);
  }
}

void Objecter::kick_requests(set<pg_t>& changed_pgs) 
{
  dout(10) << "kick_requests in pgs " << changed_pgs << endl;

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
        if (wr->tid_version.count(tid)) {
          if (wr->op == OSD_OP_WRITE &&
              !g_conf.objecter_buffer_uncommitted) {
            dout(0) << "kick_requests missing commit, cannot replay: objecter_buffer_uncommitted == FALSE" << endl;
          } else {
            dout(0) << "kick_requests missing commit, replay write " << tid
                    << " v " << wr->tid_version[tid] << endl;
            modifyx_submit(wr, wr->waitfor_commit[tid], tid);
          }
        } 
        else if (wr->waitfor_ack.count(tid)) {
          dout(0) << "kick_requests missing ack, resub write " << tid << endl;
          modifyx_submit(wr, wr->waitfor_ack[tid], tid);
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

	  else if (op_stat.count(tid)) {
		OSDStat *st = op_stat[tid];
		op_stat.erase(tid);
		
		dout(0) << "kick_requests resub stat " << tid << endl;
		
        // resubmit
        stat_submit(st);
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
    break;

  case OSD_OP_STAT:
	handle_osd_stat_reply(m);
	break;
    
  case OSD_OP_WRNOOP:
  case OSD_OP_WRITE:
  case OSD_OP_ZERO:
  case OSD_OP_DELETE:
  case OSD_OP_WRUNLOCK:
  case OSD_OP_WRLOCK:
  case OSD_OP_RDLOCK:
  case OSD_OP_RDUNLOCK:
  case OSD_OP_UPLOCK:
  case OSD_OP_DNLOCK:
    handle_osd_modify_reply(m);
    break;

  default:
    assert(0);
  }
}



// stat -----------------------------------

tid_t Objecter::stat(object_t oid, off_t *size, Context *onfinish,
					 objectrev_t rev)
{
  OSDStat *st = new OSDStat(size);
  st->extents.push_back(ObjectExtent(oid, 0, 0));
  st->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  st->extents.front().rev = rev;
  st->onfinish = onfinish;

  return stat_submit(st);
}

tid_t Objecter::stat_submit(OSDStat *st) 
{
  // find OSD
  ObjectExtent &ex = st->extents.front();
  PG &pg = get_pg( ex.pgid );

  // pick tid
  last_tid++;
  assert(client_inc >= 0);

  // add to gather set
  st->tid = last_tid;
  op_stat[last_tid] = st;    

  pg.active_tids.insert(last_tid);

  // send?
  dout(10) << "stat_submit " << st << " tid " << last_tid
           << " oid " << ex.oid
           << " pg " << ex.pgid
           << " osd" << pg.acker() 
           << endl;

  if (pg.acker() >= 0) {
	MOSDOp *m = new MOSDOp(messenger->get_myinst(), client_inc, last_tid,
						   ex.oid, ex.pgid, osdmap->get_epoch(), 
						   OSD_OP_STAT);

    messenger->send_message(m, osdmap->get_inst(pg.acker()));
  }
  
  return last_tid;
}

void Objecter::handle_osd_stat_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();

  if (op_stat.count(tid) == 0) {
    dout(7) << "handle_osd_stat_reply " << tid << " ... stray" << endl;
    delete m;
    return;
  }

  dout(7) << "handle_osd_stat_reply " << tid 
		  << " r=" << m->get_result()
		  << " size=" << m->get_object_size()
		  << endl;
  OSDStat *st = op_stat[ tid ];
  op_stat.erase( tid );

  // remove from osd/tid maps
  PG& pg = get_pg( m->get_pg() );
  assert(pg.active_tids.count(tid));
  pg.active_tids.erase(tid);
  if (pg.active_tids.empty()) close_pg( m->get_pg() );
  
  // success?
  if (m->get_result() == -EAGAIN) {
    dout(7) << " got -EAGAIN, resubmitting" << endl;
    stat_submit(st);
    delete m;
    return;
  }
  //assert(m->get_result() >= 0);

  // ok!
  if (m->get_result() < 0) {
	*st->size = -1;
  } else {
	*st->size = m->get_object_size();
  }

  // finish, clean up
  Context *onfinish = st->onfinish;

  // done
  delete st;
  if (onfinish) {
	onfinish->finish(m->get_result());
	delete onfinish;
  }

  delete m;
}


// read -----------------------------------


tid_t Objecter::read(object_t oid, off_t off, size_t len, bufferlist *bl, 
                     Context *onfinish, ExtCap* read_ext_cap,
					 objectrev_t rev)
{
  OSDRead *rd = new OSDRead(bl, read_ext_cap);

  rd->extents.push_back(ObjectExtent(oid, off, len));
  rd->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  rd->extents.front().rev = rev;
  readx(rd, onfinish);
  return last_tid;
}


tid_t Objecter::readx(OSDRead *rd, Context *onfinish)
{
  rd->onfinish = onfinish;
  
  // issue reads
  for (list<ObjectExtent>::iterator it = rd->extents.begin();
       it != rd->extents.end();
       it++) 
    readx_submit(rd, *it);

  return last_tid;
}

tid_t Objecter::readx_submit(OSDRead *rd, ObjectExtent &ex) 
{
  // find OSD
  PG &pg = get_pg( ex.pgid );

  // pick tid
  last_tid++;
  assert(client_inc >= 0);

  // add to gather set
  rd->ops[last_tid] = ex;
  op_read[last_tid] = rd;    

  pg.active_tids.insert(last_tid);

  // send?
  dout(10) << "readx_submit " << rd << " tid " << last_tid
           << " oid " << ex.oid << " " << ex.start << "~" << ex.length
           << " (" << ex.buffer_extents.size() << " buffer fragments)" 
           << " pg " << ex.pgid
           << " osd" << pg.acker() 
           << endl;

  if (pg.acker() >= 0) {
	MOSDOp *m = new MOSDOp(messenger->get_myinst(), client_inc, last_tid,
						   ex.oid, ex.pgid, osdmap->get_epoch(), 
						   OSD_OP_READ);
	m->set_length(ex.length);
	m->set_offset(ex.start);

	// set ext cap
	// FIXME mds currently is writing without caps...so we let it
	// all other (client) writes should have cap
	if (m->get_client().is_client()) {
	  m->set_user(rd->ext_cap->get_uid());
	  m->set_capability(rd->ext_cap);
	}
	
    messenger->send_message(m, osdmap->get_inst(pg.acker()));
  }
    
  return last_tid;
}


void Objecter::handle_osd_read_reply(MOSDOpReply *m) 
{
  // get pio
  tid_t tid = m->get_tid();

  if (op_read.count(tid) == 0) {
    dout(7) << "handle_osd_read_reply " << tid << " ... stray" << endl;
    delete m;
    return;
  }

  dout(7) << "handle_osd_read_reply " << tid << endl;
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
  //assert(m->get_result() >= 0);

  // what buffer offset are we?
  dout(7) << " got frag from " << m->get_oid() << " "
          << m->get_offset() << "~" << m->get_length()
          << ", still have " << rd->ops.size() << " more ops" << endl;
  
  if (rd->ops.empty()) {
    // all done
    size_t bytes_read = 0;
    
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
          dout(21) << " object " << eit->oid << " extent " << eit->start << "~" << eit->length << " : ox offset " << ox_off << " -> buffer extent " << bit->first << "~" << bit->second << endl;
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
            bufferptr z(ox_off + bit->second - ox_len);
			z.zero();
            by_off[bit->first]->append( z );
          } else {
            // we got none of this bx.  zero whole thing.
            assert(ox_off >= ox_len);
            dout(21) << "  adding all zeros for this bit " << bit->second << endl;
            bufferptr z(bit->second);
			z.zero();
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
        if (it->first < (off_t)bytes_read) {
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
            << rd->bl->length()
            << endl;
    
    // done
    delete rd;
    if (onfinish) {
      onfinish->finish(bytes_read);// > 0 ? bytes_read:m->get_result());
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
                      Context *onack, Context *oncommit,
		      ExtCap *write_ext_cap, objectrev_t rev)
{
  OSDWrite *wr = new OSDWrite(bl, write_ext_cap);

  wr->extents.push_back(ObjectExtent(oid, off, len));
  wr->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  wr->extents.front().buffer_extents[0] = len;
  wr->extents.front().rev = rev;
  modifyx(wr, onack, oncommit);
  return last_tid;
}


// zero

tid_t Objecter::zero(object_t oid, off_t off, size_t len,  
                     Context *onack, Context *oncommit,
					 objectrev_t rev)
{
  OSDModify *z = new OSDModify(OSD_OP_ZERO);
  z->extents.push_back(ObjectExtent(oid, off, len));
  z->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  z->extents.front().rev = rev;
  modifyx(z, onack, oncommit);
  return last_tid;
}


// lock ops

tid_t Objecter::lock(int op, object_t oid, 
                     Context *onack, Context *oncommit)
{
  OSDModify *l = new OSDModify(op);
  l->extents.push_back(ObjectExtent(oid, 0, 0));
  l->extents.front().pgid = osdmap->object_to_pg( oid, g_OSD_FileLayout );
  modifyx(l, onack, oncommit);
  return last_tid;
}



// generic modify -----------------------------------

tid_t Objecter::modifyx(OSDModify *wr, Context *onack, Context *oncommit)
{
  wr->onack = onack;
  wr->oncommit = oncommit;

  // issue writes/whatevers
  for (list<ObjectExtent>::iterator it = wr->extents.begin();
       it != wr->extents.end();
       it++) 
    modifyx_submit(wr, *it);

  return last_tid;
}


tid_t Objecter::modifyx_submit(OSDModify *wr, ObjectExtent &ex, tid_t usetid)
{
  // find
  PG &pg = get_pg( ex.pgid );
    
  // pick tid
  tid_t tid;
  if (usetid > 0) 
    tid = usetid;
  else
    tid = ++last_tid;

  // add to gather set
  wr->waitfor_ack[tid] = ex;
  wr->waitfor_commit[tid] = ex;
  op_modify[tid] = wr;
  pg.active_tids.insert(tid);
  
  ++num_unacked;
  ++num_uncommitted;

  // send?
  dout(10) << "modifyx_submit " << MOSDOp::get_opname(wr->op) << " tid " << tid
           << "  oid " << ex.oid
           << " " << ex.start << "~" << ex.length 
           << " pg " << ex.pgid 
           << " osd" << pg.primary()
           << endl;
  if (pg.primary() >= 0) {
	MOSDOp *m = new MOSDOp(messenger->get_myinst(), client_inc, tid,
						   ex.oid, ex.pgid, osdmap->get_epoch(),
						   wr->op);
	m->set_length(ex.length);
	m->set_offset(ex.start);
	m->set_rev(ex.rev);
	
	// only cap for a write, fix later
	// FIXME mds does writes through this interface without a cap
	// we let it for now
	if (wr->op == OSD_OP_WRITE && m->get_client().is_client()) {
	  m->set_user(wr->modify_cap->get_uid());
	  m->set_capability(wr->modify_cap);   
	}

	if (wr->tid_version.count(tid)) 
	  m->set_version(wr->tid_version[tid]);  // we're replaying this op!
    
	// what type of op?
	switch (wr->op) {
	case OSD_OP_WRITE:
	  {
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
	
    messenger->send_message(m, osdmap->get_inst(pg.primary()));
  }
  
  dout(5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << endl;
  
  return tid;
}



void Objecter::handle_osd_modify_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();

  if (op_modify.count(tid) == 0) {
    dout(7) << "handle_osd_modify_reply " << tid 
            << (m->get_commit() ? " commit":" ack")
            << " ... stray" << endl;
    delete m;
    return;
  }

  dout(7) << "handle_osd_modify_reply " << tid 
          << (m->get_commit() ? " commit":" ack")
          << " v " << m->get_version()
          << endl;
  OSDModify *wr = op_modify[ tid ];

  Context *onack = 0;
  Context *oncommit = 0;

  PG &pg = get_pg( m->get_pg() );

  // ignore?
  if (pg.acker() != m->get_source().num()) {
    dout(7) << " ignoring ack|commit from non-acker" << endl;
    delete m;
    return;
  }

  assert(m->get_result() >= 0);

  // ack or commit?
  if (m->get_commit()) {
    //dout(15) << " handle_osd_write_reply commit on " << tid << endl;
    assert(wr->tid_version.count(tid) == 0 ||
           m->get_version() == wr->tid_version[tid]);

    // remove from tid/osd maps
    assert(pg.active_tids.count(tid));
    pg.active_tids.erase(tid);
    if (pg.active_tids.empty()) close_pg( m->get_pg() );

    // commit.
    op_modify.erase( tid );
    wr->waitfor_ack.erase(tid);
    wr->waitfor_commit.erase(tid);

    num_uncommitted--;

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
    
    num_unacked--;

    if (wr->tid_version.count(tid) &&
        wr->tid_version[tid].version != m->get_version().version) {
      dout(-10) << "handle_osd_modify_reply WARNING: replay of tid " << tid 
                << " did not achieve previous ordering" << endl;
    }
    wr->tid_version[tid] = m->get_version();
    
    if (wr->waitfor_ack.empty()) {
      onack = wr->onack;
      wr->onack = 0;  // only do callback once
      
      // buffer uncommitted?
      if (!g_conf.objecter_buffer_uncommitted &&
          wr->op == OSD_OP_WRITE) {
        // discard buffer!
        ((OSDWrite*)wr)->bl.clear();
      }
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



void Objecter::ms_handle_failure(Message *m, entity_name_t dest, const entity_inst_t& inst)
{
  if (dest.is_mon()) {
    // try a new mon
    int mon = monmap->pick_mon(true);
    dout(0) << "ms_handle_failure " << dest << " inst " << inst 
            << ", resending to mon" << mon 
            << endl;
    messenger->send_message(m, monmap->get_inst(mon));
  } 
  else if (dest.is_osd()) {
    int mon = monmap->pick_mon();
    dout(0) << "ms_handle_failure " << dest << " inst " << inst 
            << ", dropping and reporting to mon" << mon 
            << endl;
    messenger->send_message(new MOSDFailure(inst, osdmap->get_epoch()), 
                            monmap->get_inst(mon));
    delete m;
  } else {
    dout(0) << "ms_handle_failure " << dest << " inst " << inst 
            << ", dropping" << endl;
    delete m;
  }
}
