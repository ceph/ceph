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

#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

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

  case MSG_GETPOOLSTATSREPLY:
    handle_get_pool_stats_reply((MGetPoolStatsReply*)m);
    break;

  case CEPH_MSG_STATFS_REPLY:
    handle_fs_stats_reply((MStatfsReply*)m);
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
	if ((was_pauserd && !osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) ||
	    (was_pausewr && !osdmap->test_flag(CEPH_OSDMAP_PAUSEWR))) {
	  for (hash_map<tid_t,Op*>::iterator p = op_osd.begin();
	       p != op_osd.end();
	       p++) {
	    if (p->second->paused) {
	      p->second->paused = false;
	      op_submit(p->second);
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
      
      hash_map<tid_t, Op*>::iterator p = op_osd.find(tid);
      if (p != op_osd.end()) {
	Op *op = p->second;
	op_osd.erase(p);

	if (op->onack)
	  num_unacked--;
	if (op->oncommit)
	  num_uncommitted--;
	
        // WRITE
	if (op->onack) {
          dout(3) << "kick_requests missing ack, resub " << tid << dendl;
          op_submit(op);
        } else {
	  assert(op->oncommit);
	  dout(3) << "kick_requests missing commit, resub " << tid << dendl;
	  op_submit(op);
        } 
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



/*
void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  if (m->may_write())
    handle_osd_modify_reply(m);
  else
    handle_osd_read_reply(m);
}
*/



// read | write ---------------------------

tid_t Objecter::op_submit(Op *op)
{
  // find
  PG &pg = get_pg( pg_t(op->layout.ol_pgid) );
    
  // pick tid
  if (!op->tid)
    op->tid = ++last_tid;
  assert(client_inc >= 0);

  // add to gather set(s)
  int flags = op->flags;
  if (op->onack) {
    flags |= CEPH_OSD_FLAG_ACK;
    ++num_unacked;
  } else {
    dout(20) << " note: not requesting ack" << dendl;
  }
  if (op->oncommit) {
    flags |= CEPH_OSD_FLAG_ONDISK;
    ++num_uncommitted;
  } else {
    dout(20) << " note: not requesting commit" << dendl;
  }
  op_osd[op->tid] = op;
  pg.active_tids.insert(op->tid);
  pg.last = g_clock.now();

  // send?
  dout(10) << "op_submit oid " << op->oid
	   << " " << op->ops << " tid " << op->tid
           << " " << op->layout 
           << " osd" << pg.primary()
           << dendl;

  if ((op->flags & CEPH_OSD_FLAG_WRITE) &&
      osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
    dout(10) << " paused modify " << op << " tid " << last_tid << dendl;
    op->paused = true;
    maybe_request_map();
  } else if ((op->flags & CEPH_OSD_FLAG_READ) &&
	     osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
    dout(10) << " paused read " << op << " tid " << last_tid << dendl;
    op->paused = true;
    maybe_request_map();
  } else if (pg.primary() >= 0) {
    int flags = op->flags;
    if (op->oncommit)
      flags |= CEPH_OSD_FLAG_ONDISK;
    if (op->onack)
      flags |= CEPH_OSD_FLAG_ACK;

    MOSDOp *m = new MOSDOp(signed_ticket, client_inc, op->tid,
			   op->oid, op->layout, osdmap->get_epoch(),
			   flags);

    m->set_snapid(op->snapid);
    m->set_snap_seq(op->snapc.seq);
    m->get_snaps() = op->snapc.snaps;

    m->ops = op->ops;
    m->set_mtime(op->mtime);
    m->set_retry_attempt(op->attempts++);
    
    if (op->version != eversion_t())
      m->set_version(op->version);  // we're replaying this op!

    messenger->send_message(m, osdmap->get_inst(pg.primary()));
  } else 
    maybe_request_map();
  
  dout(5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;
  
  return op->tid;
}

void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();

  if (op_osd.count(tid) == 0) {
    dout(7) << "handle_osd_op_reply " << tid
	    << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	    << " ... stray" << dendl;
    delete m;
    return;
  }

  dout(7) << "handle_osd_op_reply " << tid
	  << (m->is_ondisk() ? " ondisk":(m->is_onnvram() ? " onnvram":" ack"))
	  << " v " << m->get_version() << " in " << m->get_pg()
	  << dendl;
  Op *op = op_osd[ tid ];

  Context *onack = 0;
  Context *oncommit = 0;

  PG &pg = get_pg( m->get_pg() );

  // ignore?
  if (pg.acker() != m->get_source().num()) {
    dout(7) << " ignoring ack|commit from non-acker" << dendl;
    delete m;
    return;
  }
  
  int rc = m->get_result();

  if (rc == -EAGAIN) {
    dout(7) << " got -EAGAIN, resubmitting" << dendl;
    if (op->onack)
      num_unacked--;
    if (op->oncommit)
      num_uncommitted--;
    op_submit(op);
    delete m;
    return;
  }

  // got data?
  if (op->outbl) {
    op->outbl->claim(m->get_data());
    op->outbl = 0;
  }

  // ack|commit -> ack
  if (op->onack) {
    dout(15) << "handle_osd_op_reply ack" << dendl;
    op->version = m->get_version();
    onack = op->onack;
    op->onack = 0;  // only do callback once
    num_unacked--;
  }
  if (op->oncommit && m->is_ondisk()) {
    dout(15) << "handle_osd_op_reply safe" << dendl;
    oncommit = op->oncommit;
    op->oncommit = 0;
    num_uncommitted--;
  }

  // done with this tid?
  if (!op->onack && !op->oncommit) {
    assert(pg.active_tids.count(tid));
    pg.active_tids.erase(tid);
    dout(15) << "handle_osd_op_reply completed tid " << tid << ", pg " << m->get_pg()
	     << " still has " << pg.active_tids << dendl;
    if (pg.active_tids.empty()) 
      close_pg( m->get_pg() );
    op_osd.erase( tid );
    delete op;
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



// pool stats

void Objecter::get_pool_stats(vector<string>& pools, map<string,pool_stat_t> *result,
			      Context *onfinish)
{
  dout(10) << "get_pool_stats " << pools << dendl;

  PoolStatOp *op = new PoolStatOp;
  op->tid = ++last_tid;
  op->pools = pools;
  op->pool_stats = result;
  op->onfinish = onfinish;
  op_poolstat[op->tid] = op;

  poolstat_submit(op);
}

void Objecter::poolstat_submit(PoolStatOp *op)
{
  dout(10) << "poolstat_submit " << op->tid << dendl;
  MGetPoolStats *m = new MGetPoolStats(monmap->fsid, op->tid, op->pools);
  int mon = monmap->pick_mon();
  messenger->send_message(m, monmap->get_inst(mon));
}

void Objecter::handle_get_pool_stats_reply(MGetPoolStatsReply *m)
{
  dout(10) << "handle_get_pool_stats_reply " << *m << dendl;
  tid_t tid = m->tid;

  if (op_poolstat.count(tid)) {
    PoolStatOp *op = op_poolstat[tid];
    dout(10) << "have request " << tid << " at " << op << dendl;
    *op->pool_stats = m->pool_stats;
    op->onfinish->finish(0);
    delete op->onfinish;
    op_poolstat.erase(tid);
    delete op;
  } else {
    dout(10) << "unknown request " << tid << dendl;
  } 
  dout(10) << "done" << dendl;
  delete m;
}


void Objecter::get_fs_stats(ceph_statfs& result, Context *onfinish) {
  dout(10) << "get_fs_stats" << dendl;

  StatfsOp *op = new StatfsOp;
  op->tid = ++last_tid;
  op->stats = &result;
  op->onfinish = onfinish;
  op_statfs[op->tid] = op;

  fs_stats_submit(op);
}

void Objecter::fs_stats_submit(StatfsOp *op) {
  dout(10) << "fs_stats_submit" << op->tid << dendl;
  MStatfs *m = new MStatfs(monmap->fsid, op->tid);
  int mon = monmap->pick_mon();
  messenger->send_message(m, monmap->get_inst(mon));
}

void Objecter::handle_fs_stats_reply(MStatfsReply *m) {
  dout(10) << "handle_fs_stats_reply " << *m << dendl;
  tid_t tid = m->h.tid;

  if (op_statfs.count(tid)) {
    StatfsOp *op = op_statfs[tid];
    dout(10) << "have request " << tid << " at " << op << dendl;
    *(op->stats) = m->h.st;
    /*op->stats->f_total = m->h.st.f_total;
    op->stats->f_free = m->h.st.f_free;
    op->stats->f_avail = m->h.st.f_avail;
    op->stats->f_objects = m->h.st.f_objects;*/
    op->onfinish->finish(0);
    delete op->onfinish;
    op_statfs.erase(tid);
    delete op;
  } else {
    dout(10) << "unknown request " << tid << dendl;
  }
  dout(10) << "done" << dendl;
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
  for (hash_map<tid_t,Op*>::iterator p = op_osd.begin(); p != op_osd.end(); p++)
    dout(10) << " " << p->first << "\t" << p->second->oid << "\t" << p->second->ops << dendl;
}
