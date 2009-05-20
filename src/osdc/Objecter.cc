// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "Objecter.h"
#include "osd/OSDMap.h"
#include "mon/MonMap.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"

#include "messages/MOSDFailure.h"

#include <errno.h>

#include "config.h"

#define DOUT_SUBSYS objecter
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << messenger->get_myname() << ".objecter "


// messages ------------------------------

void Objecter::init()
{
  assert(client_lock.is_locked());  // otherwise event cancellation is unsafe
  timer.add_event_after(g_conf.objecter_tick_interval, new C_Tick(this));
  maybe_request_map();
}

void Objecter::shutdown() 
{
  assert(client_lock.is_locked());  // otherwise event cancellation is unsafe
  timer.cancel_all();
}


void Objecter::dispatch(Message *m)
{
  switch (m->get_type()) {
  case CEPH_MSG_OSD_OPREPLY:
    handle_osd_op_reply((MOSDOpReply*)m);
    break;
    
  case CEPH_MSG_OSD_MAP:
    handle_osd_map((MOSDMap*)m);
    break;

  default:
    dout(1) << "don't know message type " << m->get_type() << dendl;
    assert(0);
  }
}

void Objecter::handle_osd_map(MOSDMap *m)
{
  assert(osdmap); 

  if (ceph_fsid_compare(&m->fsid, &monmap->fsid)) {
    dout(0) << "handle_osd_map fsid " << m->fsid << " != " << monmap->fsid << dendl;
    delete m;
    return;
  }

  if (m->get_last() <= osdmap->get_epoch()) {
    dout(3) << "handle_osd_map ignoring epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "] <= " << osdmap->get_epoch() << dendl;
  } 
  else {
    dout(3) << "handle_osd_map got epochs [" 
            << m->get_first() << "," << m->get_last() 
            << "] > " << osdmap->get_epoch()
            << dendl;

    set<pg_t> changed_pgs;

    if (osdmap->get_epoch()) {
      // we want incrementals
      for (epoch_t e = osdmap->get_epoch() + 1;
	   e <= m->get_last();
	   e++) {

	bool was_pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
	bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR);
    
	if (m->incremental_maps.count(e)) {
	  dout(3) << "handle_osd_map decoding incremental epoch " << e << dendl;
	  OSDMap::Incremental inc(m->incremental_maps[e]);
	  osdmap->apply_incremental(inc);
	  
	  // notify messenger
	  for (map<int32_t,uint8_t>::iterator i = inc.new_down.begin();
	       i != inc.new_down.end();
	       i++) 
	    messenger->mark_down(osdmap->get_addr(i->first));
	  
	}
	else if (m->maps.count(e)) {
	  dout(3) << "handle_osd_map decoding full epoch " << e << dendl;
	  osdmap->decode(m->maps[e]);
	}
	else {
	  dout(3) << "handle_osd_map requesting missing epoch " << osdmap->get_epoch()+1 << dendl;
	  int mon = monmap->pick_mon();
	  messenger->send_message(new MOSDGetMap(monmap->fsid, osdmap->get_epoch()+1), 
				  monmap->get_inst(mon));
	  break;
	}
	
	// scan pgs for changes
	scan_pgs(changed_pgs);

	// kick paused
	if (was_pauserd && !osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
	  for (hash_map<tid_t,ReadOp*>::iterator p = op_read.begin();
	       p != op_read.end();
	       p++) {
	    if (p->second->paused) {
	      p->second->paused = false;
	      read_submit(p->second);
	    }
	  }
	}
	if (was_pausewr && !osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
	  for (hash_map<tid_t,ModifyOp*>::iterator p = op_modify.begin();
	       p != op_modify.end();
	       p++) {
	    if (p->second->paused) {
	      p->second->paused = false;
	      modify_submit(p->second);
	    }
	  }
	}
        
	assert(e == osdmap->get_epoch());
      }
      
    } else {
      // first map.  we want the full thing.
      if (m->maps.count(m->get_last())) {
	dout(3) << "handle_osd_map decoding full epoch " << m->get_last() << dendl;
	osdmap->decode(m->maps[m->get_last()]);

	scan_pgs(changed_pgs);
      } else {
	dout(3) << "handle_osd_map hmm, i want a full map, requesting" << dendl;
	int mon = monmap->pick_mon();
	messenger->send_message(new MOSDGetMap(monmap->fsid, 0),
				monmap->get_inst(mon));
      }
    }

    // kick requests who might be timing out on the wrong osds
    if (!changed_pgs.empty())
      kick_requests(changed_pgs);
  }
  
  delete m;
}


void Objecter::maybe_request_map()
{
  utime_t now;
  assert (osdmap);
  if (!osdmap->get_epoch() || last_epoch_requested <= osdmap->get_epoch()) goto yes;
  now = g_clock.now();
  if (now - last_epoch_requested_stamp > g_conf.objecter_map_request_interval) goto yes;
  return;
  
 yes:
  dout(10) << "maybe_request_map requesting next osd map" << dendl;
  last_epoch_requested_stamp = now;
  last_epoch_requested = osdmap->get_epoch()+1;
  messenger->send_message(new MOSDGetMap(monmap->fsid, last_epoch_requested),
			  monmap->get_inst(monmap->pick_mon()));
}


Objecter::PG &Objecter::get_pg(pg_t pgid)
{
  if (!pg_map.count(pgid)) {
    osdmap->pg_to_acting_osds(pgid, pg_map[pgid].acting);
    dout(10) << "get_pg " << pgid << " is new, " << pg_map[pgid].acting << dendl;
  } else {
    dout(10) << "get_pg " << pgid << " is old, " << pg_map[pgid].acting << dendl;
  }
  return pg_map[pgid];
}


void Objecter::scan_pgs_for(set<pg_t>& pgs, int osd)
{
  dout(10) << "scan_pgs_for osd" << osd << dendl;

  for (hash_map<pg_t,PG>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    pg_t pgid = i->first;
    PG& pg = i->second;
    if (pg.acting.size() && pg.acting[0] == osd)
      pgs.insert(pgid);
  }
}

void Objecter::scan_pgs(set<pg_t>& changed_pgs)
{
  dout(10) << "scan_pgs" << dendl;

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

    dout(10) << "scan_pgs " << pgid << " " << pg.acting << " -> " << other << dendl;
    
    other.swap(pg.acting);

    if (other.size() && pg.acting.size() &&
	other[0] == pg.acting[0])
      continue;  // same primary.

    // changed significantly.
    dout(10) << "scan_pgs pg " << pgid 
             << " (" << pg.active_tids << ")"
             << " " << other << " -> " << pg.acting
             << dendl;
    changed_pgs.insert(pgid);
  }
}

void Objecter::kick_requests(set<pg_t>& changed_pgs) 
{
  dout(10) << "kick_requests in pgs " << changed_pgs << dendl;

  for (set<pg_t>::iterator i = changed_pgs.begin();
       i != changed_pgs.end();
       i++) {
    pg_t pgid = *i;
    PG& pg = pg_map[pgid];

    // resubmit ops!
    set<tid_t> tids;
    tids.swap( pg.active_tids );
    close_pg( pgid );  // will pbly reopen, unless it's just commits we're missing
    
    dout(10) << "kick_requests pg " << pgid << " tids " << tids << dendl;
    for (set<tid_t>::iterator p = tids.begin();
         p != tids.end();
         p++) {
      tid_t tid = *p;
      
      if (op_modify.count(tid)) {
        ModifyOp *wr = op_modify[tid];
        op_modify.erase(tid);

	if (wr->onack)
	  num_unacked--;
	if (wr->oncommit)
	  num_uncommitted--;
	
        // WRITE
	if (wr->onack) {
          dout(3) << "kick_requests missing ack, resub write " << tid << dendl;
          modify_submit(wr);
        } else {
	  assert(wr->oncommit);
	  dout(3) << "kick_requests missing commit, resub write " << tid << dendl;
	  modify_submit(wr);
        } 
      }

      else if (op_read.count(tid)) {
        // READ
        ReadOp *rd = op_read[tid];
        op_read.erase(tid);
        dout(3) << "kick_requests resub read " << tid << dendl;

        // resubmit
        read_submit(rd);
      }
      else 
        assert(0);
    }         
  }
}


void Objecter::tick()
{
  dout(10) << "tick" << dendl;

  set<int> ping;

  // look for laggy pgs
  utime_t cutoff = g_clock.now();
  cutoff -= g_conf.objecter_timeout;  // timeout
  for (hash_map<pg_t,PG>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    if (!i->second.active_tids.empty() &&
	i->second.last < cutoff) {
      dout(10) << "tick pg " << i->first << " is laggy: " << i->second.active_tids << dendl;
      maybe_request_map();
      //break;

      // send a ping to this osd, to ensure we detect any session resets
      // (osd reply message policy is lossy)
      if (i->second.acting.size())
	ping.insert(i->second.acting[0]);
    }
  }

  for (set<int>::iterator p = ping.begin(); p != ping.end(); p++)
    messenger->send_message(new MPing, osdmap->get_inst(*p));

  // reschedule
  timer.add_event_after(g_conf.objecter_tick_interval, new C_Tick(this));
}



void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  if (m->is_modify())
    handle_osd_modify_reply(m);
  else
    handle_osd_read_reply(m);
}



// read -----------------------------------

tid_t Objecter::read_submit(ReadOp *rd)
{
  // find OSD
  PG &pg = get_pg( pg_t(rd->layout.ol_pgid) );

  // pick tid
  rd->tid = ++last_tid;
  op_read[last_tid] = rd;    

  assert(client_inc >= 0);

  pg.active_tids.insert(last_tid);
  pg.last = g_clock.now();

  // send?
  dout(10) << "read_submit " << rd << " tid " << last_tid
           << " oid " << rd->oid
	   << " " << rd->ops
           << " " << rd->layout
           << " osd" << pg.acker() 
           << dendl;

  if (osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
    dout(10) << " paused read " << rd << " tid " << last_tid << dendl;
    rd->paused = true;
    maybe_request_map();
  } else if (pg.acker() >= 0) {
    int flags = rd->flags;
    if (rd->onfinish)
      flags |= CEPH_OSD_FLAG_ACK;
    MOSDOp *m = new MOSDOp(client_inc, last_tid,
			   rd->oid, rd->layout, osdmap->get_epoch(), 
			   flags);
    m->set_snapid(rd->snap);
    m->ops = rd->ops;
    m->set_data(rd->bl);
    m->set_retry_attempt(rd->attempts++);
    
    int who = pg.acker();
    if (rd->flags & CEPH_OSD_FLAG_BALANCE_READS) {
      int replica = messenger->get_myname().num() % pg.acting.size();
      who = pg.acting[replica];
      dout(-10) << "read_submit reading from random replica " << replica
		<< " = osd" << who <<  dendl;
    }
    messenger->send_message(m, osdmap->get_inst(who));
  } else 
    maybe_request_map();
    
  return last_tid;
}


void Objecter::handle_osd_read_reply(MOSDOpReply *m) 
{
  // get pio
  tid_t tid = m->get_tid();

  if (op_read.count(tid) == 0) {
    dout(7) << "handle_osd_read_reply " << tid << " ... stray" << dendl;
    delete m;
    return;
  }

  dout(7) << "handle_osd_read_reply " << tid << dendl;
  ReadOp *rd = op_read[ tid ];
  op_read.erase( tid );
  
  // remove from osd/tid maps
  PG& pg = get_pg( m->get_pg() );
  assert(pg.active_tids.count(tid));
  pg.active_tids.erase(tid);
  if (pg.active_tids.empty()) close_pg( m->get_pg() );
  
  // success?
  if (m->get_result() == -EAGAIN) {
    dout(7) << " got -EAGAIN resubmitting" << dendl;
    read_submit(rd);
    delete m;
    return;
  }

  // what buffer offset are we?
  dout(7) << " got reply on " << rd->ops << dendl;

  int bytes_read = m->get_data().length();
  if (rd->pbl)
    rd->pbl->claim(m->get_data());

  // finish, clean up
  Context *onfinish = rd->onfinish;
  dout(7) << " " << bytes_read << " bytes " << dendl;
  
  // done
  delete rd;
  if (onfinish) {
    onfinish->finish(m->get_result());
    delete onfinish;
  }
  delete m;
}



// write ------------------------------------

tid_t Objecter::modify_submit(ModifyOp *wr)
{
  // find
  PG &pg = get_pg( pg_t(wr->layout.ol_pgid) );
    
  // pick tid
  if (!wr->tid)
    wr->tid = ++last_tid;
  assert(client_inc >= 0);

  // add to gather set(s)
  int flags = wr->flags;
  if (wr->onack) {
    flags |= CEPH_OSD_FLAG_ACK;
    ++num_unacked;
  } else {
    dout(20) << " note: not requesting ack" << dendl;
  }
  if (wr->oncommit) {
    flags |= CEPH_OSD_FLAG_ONDISK;
    ++num_uncommitted;
  } else {
    dout(20) << " note: not requesting commit" << dendl;
  }
  op_modify[wr->tid] = wr;
  pg.active_tids.insert(wr->tid);
  pg.last = g_clock.now();

  // send?
  dout(10) << "modify_submit oid " << wr->oid
	   << " " << wr->ops << " tid " << wr->tid
           << " " << wr->layout 
           << " osd" << pg.primary()
           << dendl;

  if (osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
    dout(10) << " paused modify " << wr << " tid " << last_tid << dendl;
    wr->paused = true;
    maybe_request_map();
  } else if (pg.primary() >= 0) {
    MOSDOp *m = new MOSDOp(client_inc, wr->tid,
			   wr->oid, wr->layout, osdmap->get_epoch(),
			   flags | CEPH_OSD_FLAG_MODIFY);
    m->ops = wr->ops;
    m->set_mtime(wr->mtime);
    m->set_snap_seq(wr->snapc.seq);
    m->get_snaps() = wr->snapc.snaps;
    m->set_retry_attempt(wr->attempts++);
    
    if (wr->version != eversion_t())
      m->set_version(wr->version);  // we're replaying this op!

    m->set_data(wr->bl);

    messenger->send_message(m, osdmap->get_inst(pg.primary()));
  } else 
    maybe_request_map();
  
  dout(5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;
  
  return wr->tid;
}



void Objecter::handle_osd_modify_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();

  if (op_modify.count(tid) == 0) {
    dout(7) << "handle_osd_modify_reply " << tid
	    << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	    << " ... stray" << dendl;
    delete m;
    return;
  }

  dout(7) << "handle_osd_modify_reply " << tid
	  << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	  << " v " << m->get_version() << " in " << m->get_pg()
	  << dendl;
  ModifyOp *wr = op_modify[ tid ];

  Context *onack = 0;
  Context *oncommit = 0;

  PG &pg = get_pg( m->get_pg() );

  // ignore?
  if (pg.acker() != m->get_source().num()) {
    dout(7) << " ignoring ack|commit from non-acker" << dendl;
    delete m;
    return;
  }
  
  int rc = 0;

  if (m->get_result() == -EAGAIN) {
    dout(7) << " got -EAGAIN, resubmitting" << dendl;
    if (wr->onack) num_unacked--;
    if (wr->oncommit) num_uncommitted--;
    modify_submit(wr);
    delete m;
    return;
  }

  assert(m->get_result() >= 0); // FIXME

  // ack|commit -> ack
  if (wr->onack) {
    dout(15) << "handle_osd_modify_reply ack" << dendl;
    wr->version = m->get_version();
    onack = wr->onack;
    wr->onack = 0;  // only do callback once
    num_unacked--;
  }
  if (wr->oncommit && m->is_ondisk()) {
    dout(15) << "handle_osd_modify_reply safe" << dendl;
    oncommit = wr->oncommit;
    wr->oncommit = 0;
    num_uncommitted--;
  }

  // done with this tid?
  if (!wr->onack && !wr->oncommit) {
    assert(pg.active_tids.count(tid));
    pg.active_tids.erase(tid);
    dout(15) << "handle_osd_modify_reply completed tid " << tid << ", pg " << m->get_pg()
	     << " still has " << pg.active_tids << dendl;
    if (pg.active_tids.empty()) 
      close_pg( m->get_pg() );
    op_modify.erase( tid );
    delete wr;
  }
  
  dout(5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;

  // do callbacks
  if (onack) {
    onack->finish(rc);
    delete onack;
  }
  if (oncommit) {
    oncommit->finish(rc);
    delete oncommit;
  }

  delete m;
}




// scatter/gather

void Objecter::_sg_read_finish(vector<ObjectExtent>& extents, vector<bufferlist>& resultbl, 
			       bufferlist *bl, Context *onfinish)
{
  // all done
  size_t bytes_read = 0;
  
  dout(15) << "_sg_read_finish" << dendl;

  if (extents.size() > 1) {
    /** FIXME This doesn't handle holes efficiently.
     * It allocates zero buffers to fill whole buffer, and
     * then discards trailing ones at the end.
     *
     * Actually, this whole thing is pretty messy with temporary bufferlist*'s all over
     * the heap. 
     */
    
    // map extents back into buffer
    map<__u64, bufferlist*> by_off;  // buffer offset -> bufferlist
    
    // for each object extent...
    vector<bufferlist>::iterator bit = resultbl.begin();
    for (vector<ObjectExtent>::iterator eit = extents.begin();
	 eit != extents.end();
	 eit++, bit++) {
      bufferlist& ox_buf = *bit;
      unsigned ox_len = ox_buf.length();
      unsigned ox_off = 0;
      assert(ox_len <= eit->length);           
      
      // for each buffer extent we're mapping into...
      for (map<__u32,__u32>::iterator bit = eit->buffer_extents.begin();
	   bit != eit->buffer_extents.end();
	   bit++) {
	dout(21) << " object " << eit->oid
		 << " extent " << eit->offset << "~" << eit->length
		 << " : ox offset " << ox_off
		 << " -> buffer extent " << bit->first << "~" << bit->second << dendl;
	by_off[bit->first] = new bufferlist;
	
	if (ox_off + bit->second <= ox_len) {
	  // we got the whole bx
	  by_off[bit->first]->substr_of(ox_buf, ox_off, bit->second);
	  if (bytes_read < bit->first + bit->second) 
	    bytes_read = bit->first + bit->second;
	} else if (ox_off + bit->second > ox_len && ox_off < ox_len) {
	  // we got part of this bx
	  by_off[bit->first]->substr_of(ox_buf, ox_off, (ox_len-ox_off));
	  if (bytes_read < bit->first + ox_len-ox_off) 
	    bytes_read = bit->first + ox_len-ox_off;
	  
	  // zero end of bx
	  dout(21) << "  adding some zeros to the end " << ox_off + bit->second-ox_len << dendl;
	  bufferptr z(ox_off + bit->second - ox_len);
	  z.zero();
	  by_off[bit->first]->append( z );
	} else {
	  // we got none of this bx.  zero whole thing.
	  assert(ox_off >= ox_len);
	  dout(21) << "  adding all zeros for this bit " << bit->second << dendl;
	  bufferptr z(bit->second);
	  z.zero();
	  by_off[bit->first]->append( z );
	}
	ox_off += bit->second;
      }
      assert(ox_off == eit->length);
    }
    
    // sort and string bits together
    for (map<__u64, bufferlist*>::iterator it = by_off.begin();
	 it != by_off.end();
	 it++) {
      assert(it->second->length());
      if (it->first < (__u64)bytes_read) {
	dout(21) << "  concat buffer frag off " << it->first << " len " << it->second->length() << dendl;
	bl->claim_append(*(it->second));
      } else {
	dout(21) << "  NO concat zero buffer frag off " << it->first << " len " << it->second->length() << dendl;          
      }
      delete it->second;
    }
    
    // trim trailing zeros?
    if (bl->length() > bytes_read) {
      dout(10) << " trimming off trailing zeros . bytes_read=" << bytes_read 
	       << " len=" << bl->length() << dendl;
      bl->splice(bytes_read, bl->length() - bytes_read);
      assert(bytes_read == bl->length());
    }
    
  } else {
    dout(15) << "  only one frag" << dendl;
  
    // only one fragment, easy
    bl->claim(resultbl[0]);
    bytes_read = bl->length();
  }
  
  // finish, clean up
  dout(7) << " " << bytes_read << " bytes " 
	  << bl->length()
	  << dendl;
    
  // done
  if (onfinish) {
    onfinish->finish(bytes_read);// > 0 ? bytes_read:m->get_result());
    delete onfinish;
  }
}


void Objecter::ms_handle_remote_reset(const entity_addr_t& addr, entity_name_t dest)
{
  entity_inst_t inst;
  inst.name = dest;
  inst.addr = addr;

  if (dest.is_osd()) {
    if (!osdmap->have_inst(dest.num()) ||
	(osdmap->get_inst(dest.num()) != inst)) {
      dout(0) << "ms_handle_remote_reset " << dest << " inst " << inst
	      << ", ignoring, already have newer osdmap" << dendl;
    } else {
      // kick requests
      set<pg_t> changed_pgs;
      dout(0) << "ms_handle_remote_reset " << dest << dendl;
      scan_pgs_for(changed_pgs, dest.num());
      if (!changed_pgs.empty()) {
	dout(0) << "ms_handle_remote_reset " << dest << " kicking " << changed_pgs << dendl;
	kick_requests(changed_pgs);
      }
    }
  }
}


void Objecter::dump_active()
{
  dout(10) << "dump_active" << dendl;
  
  for (hash_map<tid_t,ReadOp*>::iterator p = op_read.begin(); p != op_read.end(); p++)
    dout(10) << " read " << p->first << dendl;
  for (hash_map<tid_t,ModifyOp*>::iterator p = op_modify.begin(); p != op_modify.end(); p++)
    dout(10) << " modify " << p->first << dendl;

}
