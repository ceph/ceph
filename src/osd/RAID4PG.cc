// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "RAID4PG.h"
#include "OSD.h"

#include "common/Logger.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGRemove.h"

#include "config.h"

#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) *_dout << dbeginl << g_clock.now() << " osd" << osd->get_nodeid() << " " << (osd->osdmap ? osd->osdmap->get_epoch():0) << " " << *this << " "

#include <errno.h>
#include <sys/stat.h>



bool RAID4PG::preprocess_op(MOSDOp *op, utime_t now)
{
  return false;
}

void RAID4PG::do_op(MOSDOp *op)
{

  // a write will do something like
  object_t oid = op->get_oid();   // logical object
  pg_t pg = op->get_pg();
  ceph_object_layout layout = op->get_layout();
  bufferlist data = op->get_data();
  off_t off = op->get_offset();
  off_t left = op->get_length();

  // map data onto pobjects
  int n = pg.size() - 1;  // n+1 raid4
  int rank = (off % layout.ol_stripe_unit) % n;
  off_t off_in_bl = 0;
  while (left > 0) {
    pobject_t po(0, rank, oid);
    off_t off_in_po = off % layout.ol_stripe_unit;
    off_t stripe_unit_end = off - off_in_po + layout.ol_stripe_unit;
    off_t len_in_po = MAX(left, stripe_unit_end-off);
    bufferlist data_in_po;
    data_in_po.substr_of(data, off_in_bl, len_in_po);
    
    // next!
    off_in_bl += len_in_po;
    rank++;
    if (rank == n) rank = 0;
  }

  
  

}

void RAID4PG::do_sub_op(MOSDSubOp *op)
{

}

void RAID4PG::do_sub_op_reply(MOSDSubOpReply *reply)
{

}



// -----------------
// pg changes

bool RAID4PG::same_for_read_since(epoch_t e)
{
  return e >= info.history.same_since;   // whole pg set same
}

bool RAID4PG::same_for_modify_since(epoch_t e)
{
  return e >= info.history.same_since;   // whole pg set same
}

bool RAID4PG::same_for_rep_modify_since(epoch_t e)
{
  return e >= info.history.same_since;   // whole pg set same
}


// -----------------
// RECOVERY

bool RAID4PG::is_missing_object(object_t oid)
{
  return false;
}

void RAID4PG::wait_for_missing_object(object_t oid, Message *op)
{
  //assert(0);
}

void RAID4PG::on_osd_failure(int o)
{
  dout(10) << "on_osd_failure osd" << o << dendl;
  //assert(0);
}

void RAID4PG::on_acker_change()
{
  dout(10) << "on_acker_change" << dendl;
  //assert(0);
}


void RAID4PG::on_role_change()
{
  dout(10) << "on_role_change" << dendl;
  //assert(0);
}

void RAID4PG::on_change()
{
  dout(10) << "on_change" << dendl;
  //assert(0);
}


// misc recovery crap

void RAID4PG::clean_up_local(ObjectStore::Transaction&)
{
}

void RAID4PG::cancel_recovery() 
{
  //assert(0);
}

bool RAID4PG::do_recovery() 
{
  //assert(0);
  return false;
}

void RAID4PG::purge_strays() 
{
  //assert(0);
}



