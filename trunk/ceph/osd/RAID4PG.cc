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




void RAID4PG::do_op(MOSDOp *op)
{


}



void RAID4PG::do_op_reply(MOSDOpReply *reply)
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

void RAID4PG::wait_for_missing_object(object_t oid, MOSDOp *op)
{
  //assert(0);
}

void RAID4PG::note_failed_osd(int o)
{
  dout(10) << "note_failed_osd osd" << o << dendl;
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

void RAID4PG::clean_replicas() 
{
  //assert(0);
}



