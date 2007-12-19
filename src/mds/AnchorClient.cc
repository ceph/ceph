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

#include <iostream>
using std::cout;
using std::cerr;

#include "Anchor.h"
#include "AnchorClient.h"
#include "MDSMap.h"

#include "include/Context.h"
#include "msg/Messenger.h"

#include "MDS.h"
#include "MDLog.h"
#include "LogSegment.h"

#include "events/EAnchorClient.h"
#include "messages/MAnchor.h"

#include "config.h"

#define dout(x)  if (x <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " " << mds->messenger->get_myname() << ".anchorclient "
#define derr(x)  if (x <= g_conf.debug_mds) *_derr << dbeginl << g_clock.now() << " " << mds->messenger->get_myname() << ".anchorclient "


void AnchorClient::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_MDS_ANCHOR:
    handle_anchor_reply((MAnchor*)m);
    break;

  default:
    assert(0);
  }
}

void AnchorClient::handle_anchor_reply(class MAnchor *m)
{
  inodeno_t ino = m->get_ino();
  version_t atid = m->get_atid();

  dout(10) << "handle_anchor_reply " << *m << dendl;

  switch (m->get_op()) {

    // lookup
  case ANCHOR_OP_LOOKUP_REPLY:
    assert(pending_lookup.count(ino));
    {
      *pending_lookup[ino].trace = m->get_trace();
      Context *onfinish = pending_lookup[ino].onfinish;
      pending_lookup.erase(ino);
      
      if (onfinish) {
        onfinish->finish(0);
        delete onfinish;
      }
    }
    break;

    // prepare -> agree
  case ANCHOR_OP_CREATE_AGREE:
    if (pending_create_prepare.count(ino)) {
      dout(10) << "got create_agree on " << ino << " atid " << atid << dendl;
      Context *onfinish = pending_create_prepare[ino].onfinish;
      *pending_create_prepare[ino].patid = atid;
      pending_create_prepare.erase(ino);

      if (onfinish) {
        onfinish->finish(0);
        delete onfinish;
      }
    } 
    else if (pending_commit.count(atid)) {
      dout(10) << "stray create_agree on " << ino
	       << " atid " << atid
	       << ", already committing, resending COMMIT"
	       << dendl;      
      MAnchor *req = new MAnchor(ANCHOR_OP_COMMIT, 0, atid);
      mds->messenger->send_message(req, 
				   mds->mdsmap->get_inst(mds->mdsmap->get_anchortable()));
    }
    else {
      dout(10) << "stray create_agree on " << ino
	       << " atid " << atid
	       << ", sending ROLLBACK"
	       << dendl;      
      MAnchor *req = new MAnchor(ANCHOR_OP_ROLLBACK, 0, atid);
      mds->messenger->send_message(req, 
				   mds->mdsmap->get_inst(mds->mdsmap->get_anchortable()));
    }
    break;

  case ANCHOR_OP_DESTROY_AGREE:
    if (pending_destroy_prepare.count(ino)) {
      dout(10) << "got destroy_agree on " << ino << " atid " << atid << dendl;
      Context *onfinish = pending_destroy_prepare[ino].onfinish;
      *pending_destroy_prepare[ino].patid = atid;
      pending_destroy_prepare.erase(ino);

      if (onfinish) {
        onfinish->finish(0);
        delete onfinish;
      }
    } 
    else if (pending_commit.count(atid)) {
      dout(10) << "stray destroy_agree on " << ino
	       << " atid " << atid
	       << ", already committing, resending COMMIT"
	       << dendl;      
      MAnchor *req = new MAnchor(ANCHOR_OP_COMMIT, 0, atid);
      mds->messenger->send_message(req, 
				   mds->mdsmap->get_inst(mds->mdsmap->get_anchortable()));
    }
    else {
      dout(10) << "stray destroy_agree on " << ino
	       << " atid " << atid
	       << ", sending ROLLBACK"
	       << dendl;      
      MAnchor *req = new MAnchor(ANCHOR_OP_ROLLBACK, 0, atid);
      mds->messenger->send_message(req, 
				   mds->mdsmap->get_inst(mds->mdsmap->get_anchortable()));
    }
    break;

  case ANCHOR_OP_UPDATE_AGREE:
    if (pending_update_prepare.count(ino)) {
      dout(10) << "got update_agree on " << ino << " atid " << atid << dendl;
      Context *onfinish = pending_update_prepare[ino].onfinish;
      *pending_update_prepare[ino].patid = atid;
      pending_update_prepare.erase(ino);

      if (onfinish) {
        onfinish->finish(0);
        delete onfinish;
      }
    }
    else if (pending_commit.count(atid)) {
      dout(10) << "stray update_agree on " << ino
	       << " atid " << atid
	       << ", already committing, resending COMMIT"
	       << dendl;      
      MAnchor *req = new MAnchor(ANCHOR_OP_COMMIT, 0, atid);
      mds->messenger->send_message(req, 
				   mds->mdsmap->get_inst(mds->mdsmap->get_anchortable()));
    }
    else {
      dout(10) << "stray update_agree on " << ino
	       << " atid " << atid
	       << ", sending ROLLBACK"
	       << dendl;      
      MAnchor *req = new MAnchor(ANCHOR_OP_ROLLBACK, 0, atid);
      mds->messenger->send_message(req, 
				   mds->mdsmap->get_inst(mds->mdsmap->get_anchortable()));
    }
    break;

    // commit -> ack
  case ANCHOR_OP_ACK:
    {
      dout(10) << "got ack on atid " << atid << ", logging" << dendl;

      // remove from committing list
      assert(pending_commit.count(atid));
      assert(pending_commit[atid]->pending_commit_atids.count(atid));
      
      // log ACK.
      mds->mdlog->submit_entry(new EAnchorClient(ANCHOR_OP_ACK, atid),
			       new C_LoggedAck(this, atid));
    }
    break;

  default:
    assert(0);
  }

  delete m;
}


void AnchorClient::_logged_ack(version_t atid)
{
  dout(10) << "_logged_ack" << dendl;

  assert(pending_commit.count(atid));
  assert(pending_commit[atid]->pending_commit_atids.count(atid));
  
  pending_commit[atid]->pending_commit_atids.erase(atid);
  pending_commit.erase(atid);
  
  // kick any waiters (LogSegment trim)
  if (ack_waiters.count(atid)) {
    dout(15) << "kicking ack waiters on atid " << atid << dendl;
    mds->queue_waiters(ack_waiters[atid]);
    ack_waiters.erase(atid);
  }
}


/*
 * public async interface
 */


/*
 * FIXME: we need to be able to resubmit messages if the anchortable mds fails.
 */


void AnchorClient::lookup(inodeno_t ino, vector<Anchor>& trace, Context *onfinish)
{
  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_LOOKUP, ino);

  assert(pending_lookup.count(ino) == 0);
  pending_lookup[ino].onfinish = onfinish;
  pending_lookup[ino].trace = &trace;

  mds->send_message_mds(req, 
			mds->mdsmap->get_anchortable());
}


// PREPARE

void AnchorClient::prepare_create(inodeno_t ino, vector<Anchor>& trace, 
				  version_t *patid, Context *onfinish)
{
  dout(10) << "prepare_create " << ino << " " << trace << dendl;

  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_CREATE_PREPARE, ino);
  req->set_trace(trace);

  pending_create_prepare[ino].trace = trace;
  pending_create_prepare[ino].patid = patid;
  pending_create_prepare[ino].onfinish = onfinish;

  mds->send_message_mds(req, 
			mds->mdsmap->get_anchortable());
}

void AnchorClient::prepare_destroy(inodeno_t ino, 
				  version_t *patid, Context *onfinish)
{
  dout(10) << "prepare_destroy " << ino << dendl;

  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_DESTROY_PREPARE, ino);
  pending_destroy_prepare[ino].onfinish = onfinish;
  pending_destroy_prepare[ino].patid = patid;
  mds->messenger->send_message(req, 
			       mds->mdsmap->get_inst(mds->mdsmap->get_anchortable()));
}


void AnchorClient::prepare_update(inodeno_t ino, vector<Anchor>& trace, 
				  version_t *patid, Context *onfinish)
{
  dout(10) << "prepare_update " << ino << " " << trace << dendl;

  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_UPDATE_PREPARE, ino);
  req->set_trace(trace);
  
  pending_update_prepare[ino].trace = trace;
  pending_update_prepare[ino].patid = patid;
  pending_update_prepare[ino].onfinish = onfinish;
  
  mds->messenger->send_message(req, 
			       mds->mdsmap->get_inst(mds->mdsmap->get_anchortable()));
}


// COMMIT

void AnchorClient::commit(version_t atid, LogSegment *ls)
{
  dout(10) << "commit " << atid << dendl;

  assert(pending_commit.count(atid) == 0);
  pending_commit[atid] = ls;
  ls->pending_commit_atids.insert(atid);

  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_COMMIT, 0, atid);
  mds->messenger->send_message(req, 
			       mds->mdsmap->get_inst(mds->mdsmap->get_anchortable()));
}



// RECOVERY

void AnchorClient::finish_recovery()
{
  dout(7) << "finish_recovery" << dendl;

  resend_commits();
}

void AnchorClient::resend_commits()
{
  for (map<version_t,LogSegment*>::iterator p = pending_commit.begin();
       p != pending_commit.end();
       ++p) {
    dout(10) << "resending commit on " << p->first << dendl;
    MAnchor *req = new MAnchor(ANCHOR_OP_COMMIT, 0, p->first);
    mds->send_message_mds(req, 
			  mds->mdsmap->get_anchortable());
  }
}

void AnchorClient::resend_prepares(hash_map<inodeno_t, _pending_prepare>& prepares, int op)
{
  for (hash_map<inodeno_t, _pending_prepare>::iterator p = prepares.begin();
       p != prepares.end();
       p++) {
    dout(10) << "resending " << get_anchor_opname(op) << " on " << p->first << dendl;
    MAnchor *req = new MAnchor(op, p->first);
    req->set_trace(p->second.trace);
    mds->send_message_mds(req, 
			  mds->mdsmap->get_anchortable());
  } 
}


void AnchorClient::handle_mds_recovery(int who)
{
  dout(7) << "handle_mds_recovery mds" << who << dendl;

  if (who != mds->mdsmap->get_anchortable()) 
    return; // do nothing.

  // resend any pending lookups.
  for (hash_map<inodeno_t, _pending_lookup>::iterator p = pending_lookup.begin();
       p != pending_lookup.end();
       p++) {
    dout(10) << "resending lookup on " << p->first << dendl;
    mds->send_message_mds(new MAnchor(ANCHOR_OP_LOOKUP, p->first),
			  mds->mdsmap->get_anchortable());
  }
  
  // resend any pending prepares.
  resend_prepares(pending_create_prepare, ANCHOR_OP_CREATE_PREPARE);
  resend_prepares(pending_update_prepare, ANCHOR_OP_UPDATE_PREPARE);
  resend_prepares(pending_destroy_prepare, ANCHOR_OP_DESTROY_PREPARE);

  // resend any pending commits.
  resend_commits();
}
