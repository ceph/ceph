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

#include "AnchorClient.h"
#include "MDSMap.h"
#include "LogSegment.h"
#include "MDS.h"
#include "msg/Messenger.h"

#include "messages/MMDSTableRequest.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".anchorclient "



// LOOKUPS
/* This function DOES NOT put the passed message before returning */
void AnchorClient::handle_query_result(class MMDSTableRequest *m)
{
  dout(10) << "handle_anchor_reply " << *m << dendl;

  inodeno_t ino;
  vector<Anchor> trace;

  bufferlist::iterator p = m->bl.begin();
  ::decode(ino, p);
  ::decode(trace, p);

  if (!pending_lookup.count(ino))
    return;

  list<_pending_lookup> ls;
  ls.swap(pending_lookup[ino]);
  pending_lookup.erase(ino);

  for (list<_pending_lookup>::iterator q = ls.begin(); q != ls.end(); ++q) {
    *q->trace = trace;
    if (q->onfinish) {
      q->onfinish->finish(0);
      delete q->onfinish;
    }
  }
}

void AnchorClient::resend_queries()
{
  // resend any pending lookups.
  for (map<inodeno_t, list<_pending_lookup> >::iterator p = pending_lookup.begin();
       p != pending_lookup.end();
       ++p) {
    dout(10) << "resending lookup on " << p->first << dendl;
    _lookup(p->first);
  }
}

void AnchorClient::lookup(inodeno_t ino, vector<Anchor>& trace, Context *onfinish)
{
  _pending_lookup l;
  l.onfinish = onfinish;
  l.trace = &trace;

  bool isnew = (pending_lookup.count(ino) == 0);
  pending_lookup[ino].push_back(l);
  if (isnew)
    _lookup(ino);
}

void AnchorClient::_lookup(inodeno_t ino)
{
  int ts = mds->mdsmap->get_tableserver();
  if (mds->mdsmap->get_state(ts) < MDSMap::STATE_REJOIN)
    return;
  MMDSTableRequest *req = new MMDSTableRequest(table, TABLESERVER_OP_QUERY, 0, 0);
  ::encode(ino, req->bl);
  mds->send_message_mds(req, ts);
}


// FRIENDLY PREPARE

void AnchorClient::prepare_create(inodeno_t ino, vector<Anchor>& trace, 
				  version_t *patid, Context *onfinish)
{
  dout(10) << "prepare_create " << ino << " " << trace << dendl;
  bufferlist bl;
  __u32 op = TABLE_OP_CREATE;
  ::encode(op, bl);
  ::encode(ino, bl);
  ::encode(trace, bl);
  _prepare(bl, patid, 0, onfinish);
}

void AnchorClient::prepare_destroy(inodeno_t ino, 
				   version_t *patid, Context *onfinish)
{
  dout(10) << "prepare_destroy " << ino << dendl;
  bufferlist bl;
  __u32 op = TABLE_OP_DESTROY;
  ::encode(op, bl);
  ::encode(ino, bl);
  _prepare(bl, patid, 0, onfinish);
}


void AnchorClient::prepare_update(inodeno_t ino, vector<Anchor>& trace, 
				  version_t *patid, Context *onfinish)
{
  dout(10) << "prepare_update " << ino << " " << trace << dendl;
  bufferlist bl;
  __u32 op = TABLE_OP_UPDATE;
  ::encode(op, bl);
  ::encode(ino, bl);
  ::encode(trace, bl);
  _prepare(bl, patid, 0, onfinish);
}
