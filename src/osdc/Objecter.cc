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

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"

#include "messages/MOSDFailure.h"

#include <errno.h>

#include "config.h"

#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_objecter) *_dout << dbeginl << g_clock.now() << " " << messenger->get_myname() << ".objecter "
#define derr(x)  if (x <= g_conf.debug || x <= g_conf.debug_objecter) *_derr << dbeginl << g_clock.now() << " " << messenger->get_myname() << ".objecter "


// messages ------------------------------

void Objecter::init()
{
  assert(client_lock.is_locked());  // otherwise event cancellation is unsafe
  timer.add_event_after(g_conf.objecter_tick_interval, new C_Tick(this));
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

  if (!ceph_fsid_equal(&m->fsid, &monmap->fsid)) {
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

    for (epoch_t e = osdmap->get_epoch() + 1;
         e <= m->get_last();
         e++) {
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
        
      assert(e == osdmap->get_epoch());
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
  if (!osdmap) goto yes;
  if (last_epoch_requested <= osdmap->get_epoch()) goto yes;
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
        OSDModify *wr = op_modify[tid];
        op_modify.erase(tid);

	if (wr->onack)
	  num_unacked--;
	if (wr->oncommit)
	  num_uncommitted--;
	
        // WRITE
	if (wr->waitfor_ack.count(tid)) {
          dout(3) << "kick_requests missing ack, resub write " << tid << dendl;
          modifyx_submit(wr, wr->waitfor_ack[tid], tid);
        } else {
	  assert(wr->waitfor_commit.count(tid));
	  
	  if (wr->tid_version.count(tid)) {
	    if (wr->op == CEPH_OSD_OP_WRITE &&
		!g_conf.objecter_buffer_uncommitted) {
	      dout(0) << "kick_requests missing commit, cannot replay: objecter_buffer_uncommitted == FALSE" << dendl;
	      assert(0);  // crap. fixme.
	    } else {
	      dout(3) << "kick_requests missing commit, replay write " << tid
		      << " v " << wr->tid_version[tid] << dendl;
	    }
	  } else {
	    dout(3) << "kick_requests missing commit, resub write " << tid << dendl;
	  }
	  modifyx_submit(wr, wr->waitfor_commit[tid], tid);
        } 
      }

      else if (op_read.count(tid)) {
        // READ
        OSDRead *rd = op_read[tid];
        op_read.erase(tid);
        dout(3) << "kick_requests resub read " << tid << dendl;

        // resubmit
        readx_submit(rd, rd->ops[tid], true);
        rd->ops.erase(tid);
      }

      else if (op_stat.count(tid)) {
	OSDStat *st = op_stat[tid];
	op_stat.erase(tid);
	
	dout(3) << "kick_requests resub stat " << tid << dendl;
		
        // resubmit
        stat_submit(st);
      }
	  
      else 
        assert(0);
    }         
  }
}


void Objecter::tick()
{
  dout(10) << "tick" << dendl;

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
    }
  }

  // reschedule
  timer.add_event_after(g_conf.objecter_tick_interval, new C_Tick(this));
}



void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  // read or modify?
  switch (m->get_op()) {
  case CEPH_OSD_OP_READ:
    handle_osd_read_reply(m);
    break;

  case CEPH_OSD_OP_STAT:
    handle_osd_stat_reply(m);
    break;
    
  case CEPH_OSD_OP_WRNOOP:
  case CEPH_OSD_OP_WRITE:
  case CEPH_OSD_OP_ZERO:
  case CEPH_OSD_OP_DELETE:
  case CEPH_OSD_OP_WRUNLOCK:
  case CEPH_OSD_OP_WRLOCK:
  case CEPH_OSD_OP_RDLOCK:
  case CEPH_OSD_OP_RDUNLOCK:
  case CEPH_OSD_OP_UPLOCK:
  case CEPH_OSD_OP_DNLOCK:
    handle_osd_modify_reply(m);
    break;

  default:
    assert(0);
  }
}



// stat -----------------------------------

tid_t Objecter::stat(object_t oid, __u64 *size, ceph_object_layout ol, const vector<snapid_t>& snaps, int flags, Context *onfinish)
{
  OSDStat *st = prepare_stat(snaps, size, flags);
  st->extents.push_back(ObjectExtent(oid, 0, 0));
  st->extents.front().layout = ol;
  st->onfinish = onfinish;

  return stat_submit(st);
}

tid_t Objecter::stat_submit(OSDStat *st) 
{
  // find OSD
  ObjectExtent &ex = st->extents.front();
  PG &pg = get_pg( pg_t(ex.layout.ol_pgid) );

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
           << " " << ex.layout
           << " osd" << pg.acker() 
           << dendl;

  if (pg.acker() >= 0) {
    int flags = st->flags;
    if (st->onfinish) flags |= CEPH_OSD_OP_ACK;

    MOSDOp *m = new MOSDOp(client_inc, last_tid,
			   ex.oid, ex.layout, osdmap->get_epoch(), 
			   CEPH_OSD_OP_STAT, flags);
    m->get_snaps() = st->snaps;
    if (inc_lock > 0) {
      st->inc_lock = inc_lock;
      m->set_inc_lock(inc_lock);
    }
    
    messenger->send_message(m, osdmap->get_inst(pg.acker()));
  }
  
  return last_tid;
}

void Objecter::handle_osd_stat_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();

  if (op_stat.count(tid) == 0) {
    dout(7) << "handle_osd_stat_reply " << tid << " ... stray" << dendl;
    delete m;
    return;
  }

  dout(7) << "handle_osd_stat_reply " << tid 
		  << " r=" << m->get_result()
		  << " size=" << m->get_length()
		  << dendl;
  OSDStat *st = op_stat[ tid ];
  op_stat.erase( tid );

  // remove from osd/tid maps
  PG& pg = get_pg( m->get_pg() );
  assert(pg.active_tids.count(tid));
  pg.active_tids.erase(tid);
  if (pg.active_tids.empty()) close_pg( m->get_pg() );
  
  // success?
  if (m->get_result() == -EINCLOCKED &&
      st->flags & CEPH_OSD_OP_INCLOCK_FAIL == 0) {
    dout(7) << " got -EINCLOCKED, resubmitting" << dendl;
    stat_submit(st);
    delete m;
    return;
  }
  if (m->get_result() == -EAGAIN) {
    dout(7) << " got -EAGAIN, resubmitting" << dendl;
    stat_submit(st);
    delete m;
    return;
  }

  // ok!
  if (m->get_result() < 0) {
    *st->size = 0;
  } else {
    *st->size = m->get_length();
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


tid_t Objecter::read(object_t oid, __u64 off, size_t len, ceph_object_layout ol, const vector<snapid_t> &snaps, bufferlist *bl, int flags, 
                     Context *onfinish)
{
  OSDRead *rd = prepare_read(snaps, bl, flags);
  rd->extents.push_back(ObjectExtent(oid, off, len));
  rd->extents.front().layout = ol;
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

tid_t Objecter::readx_submit(OSDRead *rd, ObjectExtent &ex, bool retry) 
{
  // find OSD
  PG &pg = get_pg( pg_t(ex.layout.ol_pgid) );

  // pick tid
  last_tid++;
  assert(client_inc >= 0);

  // add to gather set
  rd->ops[last_tid] = ex;
  op_read[last_tid] = rd;    

  pg.active_tids.insert(last_tid);
  pg.last = g_clock.now();

  // send?
  dout(10) << "readx_submit " << rd << " tid " << last_tid
           << " oid " << ex.oid << " " << ex.start << "~" << ex.length
           << " (" << ex.buffer_extents.size() << " buffer fragments)" 
           << " " << ex.layout
           << " osd" << pg.acker() 
           << dendl;

  if (pg.acker() >= 0) {
    int flags = rd->flags;
    if (rd->onfinish) flags |= CEPH_OSD_OP_ACK;
    MOSDOp *m = new MOSDOp(client_inc, last_tid,
			   ex.oid, ex.layout, osdmap->get_epoch(), 
			   CEPH_OSD_OP_READ, flags);
    m->get_snaps() = rd->snaps;
    if (inc_lock > 0) {
      rd->inc_lock = inc_lock;
      m->set_inc_lock(inc_lock);
    }
    m->set_length(ex.length);
    m->set_offset(ex.start);
    m->set_retry_attempt(retry);
    
    int who = pg.acker();
    if (rd->flags & CEPH_OSD_OP_BALANCE_READS) {
      int replica = messenger->get_myname().num() % pg.acting.size();
      who = pg.acting[replica];
      dout(-10) << "readx_submit reading from random replica " << replica
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
  OSDRead *rd = op_read[ tid ];
  op_read.erase( tid );

  // remove from osd/tid maps
  PG& pg = get_pg( m->get_pg() );
  assert(pg.active_tids.count(tid));
  pg.active_tids.erase(tid);
  if (pg.active_tids.empty()) close_pg( m->get_pg() );
  
  // our op finished
  rd->ops.erase(tid);

  // fail?
  if (m->get_result() == -EINCLOCKED &&
      rd->flags & CEPH_OSD_OP_INCLOCK_FAIL) {
    dout(7) << " got -EINCLOCKED, failing" << dendl;
    if (rd->onfinish) {
      rd->onfinish->finish(-EINCLOCKED);
      delete rd->onfinish;
    }
    delete rd;
    delete m;
    return;
  }

  // success?
  if (m->get_result() == -EAGAIN ||
      m->get_result() == -EINCLOCKED) {
    dout(7) << " got -EAGAIN or -EINCLOCKED, resubmitting" << dendl;
    readx_submit(rd, rd->ops[tid], true);
    delete m;
    return;
  }

  // what buffer offset are we?
  dout(7) << " got frag from " << m->get_oid() << " "
          << m->get_offset() << "~" << m->get_length()
          << ", still have " << rd->ops.size() << " more ops" << dendl;
  
  if (rd->ops.empty()) {
    // all done
    size_t bytes_read = 0;
    
    if (rd->read_data.size()) {
      dout(15) << " assembling frags" << dendl;

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
      map<__u64, bufferlist*> by_off;  // buffer offset -> bufferlist

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
          dout(21) << " object " << eit->oid << " extent " << eit->start << "~" << eit->length << " : ox offset " << ox_off << " -> buffer extent " << bit->first << "~" << bit->second << dendl;
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
          rd->bl->claim_append(*(it->second));
        } else {
          dout(21) << "  NO concat zero buffer frag off " << it->first << " len " << it->second->length() << dendl;          
        }
        delete it->second;
      }

      // trim trailing zeros?
      if (rd->bl->length() > bytes_read) {
        dout(10) << " trimming off trailing zeros . bytes_read=" << bytes_read 
                 << " len=" << rd->bl->length() << dendl;
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
      dout(15) << "  only one frag" << dendl;

      // only one fragment, easy
      rd->bl->claim( m->get_data() );
      bytes_read = rd->bl->length();
    }

    // finish, clean up
    Context *onfinish = rd->onfinish;

    dout(7) << " " << bytes_read << " bytes " 
            << rd->bl->length()
            << dendl;
    
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

tid_t Objecter::write(object_t oid, __u64 off, size_t len, ceph_object_layout ol, const vector<snapid_t> &snaps, bufferlist &bl, int flags,
                      Context *onack, Context *oncommit)
{
  OSDWrite *wr = prepare_write(snaps, bl, flags);
  wr->extents.push_back(ObjectExtent(oid, off, len));
  wr->extents.front().layout = ol;
  wr->extents.front().buffer_extents[0] = len;
  modifyx(wr, onack, oncommit);
  return last_tid;
}


// zero

tid_t Objecter::zero(object_t oid, __u64 off, size_t len, ceph_object_layout ol, const vector<snapid_t> &snaps, int flags, 
                     Context *onack, Context *oncommit)
{
  OSDModify *z = prepare_modify(snaps, CEPH_OSD_OP_ZERO, flags);
  z->extents.push_back(ObjectExtent(oid, off, len));
  z->extents.front().layout = ol;
  modifyx(z, onack, oncommit);
  return last_tid;
}


// lock ops

tid_t Objecter::lock(int op, object_t oid, int flags, ceph_object_layout ol, const vector<snapid_t> &snaps,
                     Context *onack, Context *oncommit)
{
  OSDModify *l = prepare_modify(snaps, op, flags);
  l->extents.push_back(ObjectExtent(oid, 0, 0));
  l->extents.front().layout = ol;
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
  PG &pg = get_pg( pg_t(ex.layout.ol_pgid) );
    
  // pick tid
  tid_t tid;
  if (usetid > 0) 
    tid = usetid;
  else
    tid = ++last_tid;
  assert(client_inc >= 0);

  // add to gather set(s)
  int flags = wr->flags;
  if (wr->onack) {
    flags |= CEPH_OSD_OP_ACK;
    wr->waitfor_ack[tid] = ex;
    ++num_unacked;
  } else {
    dout(20) << " note: not requesting ack" << dendl;
  }
  if (wr->oncommit) {
    flags |= CEPH_OSD_OP_SAFE;
    wr->waitfor_commit[tid] = ex;
    ++num_uncommitted;
  } else {
    dout(20) << " note: not requesting commit" << dendl;
  }
  op_modify[tid] = wr;
  pg.active_tids.insert(tid);
  pg.last = g_clock.now();

  // send?
  dout(10) << "modifyx_submit " << MOSDOp::get_opname(wr->op) << " tid " << tid
           << "  oid " << ex.oid
           << " " << ex.start << "~" << ex.length 
           << " " << ex.layout 
           << " osd" << pg.primary()
           << dendl;
  if (pg.primary() >= 0) {
    MOSDOp *m = new MOSDOp(client_inc, tid,
			   ex.oid, ex.layout, osdmap->get_epoch(),
			   wr->op, flags);
    m->get_snaps() = wr->snaps;
    if (inc_lock > 0) {
      wr->inc_lock = inc_lock;
      m->set_inc_lock(inc_lock);
    }
    m->set_length(ex.length);
    m->set_offset(ex.start);
    if (usetid > 0)
      m->set_retry_attempt(true);
    
    if (wr->tid_version.count(tid)) 
      m->set_version(wr->tid_version[tid]);  // we're replaying this op!
    
    // what type of op?
    switch (wr->op) {
    case CEPH_OSD_OP_WRITE:
      {
	// map buffer segments into this extent
	// (may be fragmented bc of striping)
	bufferlist cur;
	for (map<size_t,size_t>::iterator bit = ex.buffer_extents.begin();
	     bit != ex.buffer_extents.end();
	     bit++) 
	  ((OSDWrite*)wr)->bl.copy(bit->first, bit->second, cur);
	assert(cur.length() == ex.length);
	m->set_data(cur);//.claim(cur);
      }
      break;
    }
    
    messenger->send_message(m, osdmap->get_inst(pg.primary()));
  } else 
    maybe_request_map();
  
  dout(5) << num_unacked << " unacked, " << num_uncommitted << " uncommitted" << dendl;
  
  return tid;
}



void Objecter::handle_osd_modify_reply(MOSDOpReply *m)
{
  // get pio
  tid_t tid = m->get_tid();

  if (op_modify.count(tid) == 0) {
    dout(7) << "handle_osd_modify_reply " << tid 
            << (m->is_safe() ? " commit":" ack")
            << " ... stray" << dendl;
    delete m;
    return;
  }

  dout(7) << "handle_osd_modify_reply " << tid 
          << (m->is_safe() ? " commit":" ack")
          << " v " << m->get_version() << " in " << m->get_pg()
          << dendl;
  OSDModify *wr = op_modify[ tid ];

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
  if (m->get_result() == -EINCLOCKED && wr->flags & CEPH_OSD_OP_INCLOCK_FAIL) {
    dout(7) << " got -EINCLOCKED, failing" << dendl;
    rc = -EINCLOCKED;
    if (wr->onack) {
      onack = wr->onack;
      wr->onack = 0;
      num_unacked--;
    }
    if (wr->oncommit) {
      oncommit = wr->oncommit;
      wr->oncommit = 0;
      num_uncommitted--;
    }
    goto done;
  }

  if (m->get_result() == -EAGAIN ||
      m->get_result() == -EINCLOCKED) {
    dout(7) << " got -EAGAIN or -EINCLOCKED, resubmitting" << dendl;
    if (wr->onack) num_unacked--;
    if (wr->oncommit) num_uncommitted--;
    if (wr->waitfor_ack.count(tid)) 
      modifyx_submit(wr, wr->waitfor_ack[tid]);
    else if (wr->waitfor_commit.count(tid)) 
      modifyx_submit(wr, wr->waitfor_commit[tid]);
    else assert(0);
    delete m;
    return;
  }

  assert(m->get_result() >= 0); // FIXME

  // ack|commit -> ack
  if (wr->waitfor_ack.count(tid)) {
    wr->waitfor_ack.erase(tid);
    num_unacked--;
    dout(15) << "handle_osd_modify_reply ack" << dendl;
    
    /*
      osd uses v to reorder during replay, but doesn't preserve it
    if (wr->tid_version.count(tid) &&
	wr->tid_version[tid].version != m->get_version().version) {
      dout(-10) << "handle_osd_modify_reply WARNING: replay of tid " << tid 
		<< " did not achieve previous ordering" << dendl;
    }
    */
    wr->tid_version[tid] = m->get_version();
    
    if (wr->waitfor_ack.empty()) {
      onack = wr->onack;
      wr->onack = 0;  // only do callback once
      
      // buffer uncommitted?
      if (!g_conf.objecter_buffer_uncommitted &&
	  wr->op == CEPH_OSD_OP_WRITE) {
	// discard buffer!
	((OSDWrite*)wr)->bl.clear();
      }
    } else {
      dout(15) << "handle_osd_modify_reply still need " 
	       << wr->waitfor_ack.size() << " acks" << dendl;
    }
  }
  if (m->is_safe()) {
    // safe
    /*
      osd uses v to reorder during replay, but doesn't preserve it
    assert(wr->tid_version.count(tid) == 0 ||
           m->get_version() == wr->tid_version[tid]);
    */

    wr->waitfor_commit.erase(tid);
    num_uncommitted--;
    dout(15) << "handle_osd_modify_reply safe" << dendl;
    
    if (wr->waitfor_commit.empty()) {
      oncommit = wr->oncommit;
      wr->oncommit = 0;
    } else {
      dout(15) << "handle_osd_modify_reply still need "
	       << wr->waitfor_commit.size() << " safes" << dendl;
    }
  }

  // done?
 done:

  // done with this tid?
  if (wr->waitfor_commit.count(tid) == 0 &&
      wr->waitfor_ack.count(tid) == 0) {
    assert(pg.active_tids.count(tid));
    pg.active_tids.erase(tid);
    dout(15) << "handle_osd_modify_reply pg " << m->get_pg()
	     << " still has " << pg.active_tids << dendl;
    if (pg.active_tids.empty()) 
      close_pg( m->get_pg() );
    op_modify.erase( tid );
  }
  
  // done with this overall op?
  if (wr->onack == 0 && wr->oncommit == 0) {
    dout(15) << "handle_osd_modify_reply completed" << dendl;
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



void Objecter::ms_handle_failure(Message *m, entity_name_t dest, const entity_inst_t& inst)
{
  if (dest.is_mon()) {
    // try a new mon
    int mon = monmap->pick_mon(true);
    dout(0) << "ms_handle_failure " << dest << " inst " << inst 
            << ", resending to mon" << mon 
            << dendl;
    messenger->send_message(m, monmap->get_inst(mon));
  } 
  else if (dest.is_osd()) {
    if (!osdmap->have_inst(dest.num()) ||
	(osdmap->get_inst(dest.num()) != inst)) {
      dout(0) << "ms_handle_failure " << dest << " inst " << inst 
	      << ", dropping, already have newer osdmap" << dendl;
    } else {
      int mon = monmap->pick_mon();
      dout(0) << "ms_handle_failure " << dest << " inst " << inst 
	      << ", dropping, reporting to mon" << mon 
	      << dendl;
      messenger->send_message(new MOSDFailure(monmap->fsid, inst, osdmap->get_epoch()), 
			      monmap->get_inst(mon));
    }
    delete m;
  } else {
    dout(0) << "ms_handle_failure " << dest << " inst " << inst 
            << ", dropping" << dendl;
    delete m;
  }
}


void Objecter::dump_active()
{
  dout(10) << "dump_active" << dendl;
  
  for (hash_map<tid_t,OSDStat*>::iterator p = op_stat.begin(); p != op_stat.end(); p++)
    dout(10) << " stat " << p->first << dendl;
  for (hash_map<tid_t,OSDRead*>::iterator p = op_read.begin(); p != op_read.end(); p++)
    dout(10) << " read " << p->first << dendl;
  for (hash_map<tid_t,OSDModify*>::iterator p = op_modify.begin(); p != op_modify.end(); p++)
    dout(10) << " modify " << p->first << dendl;

}
