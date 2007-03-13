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

#include <iostream>
using std::cout;
using std::cerr;
using std::endl;

#include "Anchor.h"
#include "AnchorClient.h"
#include "MDSMap.h"

#include "include/Context.h"
#include "msg/Messenger.h"

#include "MDS.h"

#include "messages/MAnchor.h"

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << g_clock.now() << " " << messenger->get_myaddr() << ".anchorclient "
#define derr(x)  if (x <= g_conf.debug) cout << g_clock.now() << " " << messenger->get_myaddr() << ".anchorclient "


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
  switch (m->get_op()) {

  case ANCHOR_OP_LOOKUP_REPLY:
    {
      assert(pending_lookup_trace.count(m->get_ino()) == 1);

      *(pending_lookup_trace[ m->get_ino() ]) = m->get_trace();
      Context *onfinish = pending_lookup[ m->get_ino() ];

      pending_lookup_trace.erase(m->get_ino());
      pending_lookup.erase(m->get_ino());
      
      if (onfinish) {
        onfinish->finish(0);
        delete onfinish;
      }
    }
    break;

  case ANCHOR_OP_UPDATE_ACK:
  case ANCHOR_OP_CREATE_ACK:
  case ANCHOR_OP_DESTROY_ACK:
    {
      assert(pending_op.count(m->get_ino()) == 1);

      Context *onfinish = pending_op[m->get_ino()];
      pending_op.erase(m->get_ino());

      if (onfinish) {
        onfinish->finish(0);
        delete onfinish;
      }
    }
    break;

  default:
    assert(0);
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

  pending_lookup_trace[ino] = &trace;
  pending_lookup[ino] = onfinish;

  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORTABLE, MDS_PORT_ANCHORCLIENT);
}

void AnchorClient::prepare_create(inodeno_t ino, vector<Anchor>& trace, Context *onfinish)
{
  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_CREATE_PREPARE, ino);
  req->set_trace(trace);

  pending_op[ino] = onfinish;

  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORTABLE, MDS_PORT_ANCHORCLIENT);
}

void AnchorClient::commit_create(inodeno_t ino)
{
  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_CREATE_COMMIT, ino);
  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORTABLE, MDS_PORT_ANCHORCLIENT);
}


void AnchorClient::prepare_destroy(inodeno_t ino, Context *onfinish)
{
  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_DESTROY_PREPARE, ino);
  pending_op[ino] = onfinish;
  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORTABLE, MDS_PORT_ANCHORCLIENT);
}
void AnchorClient::commit_destroy(inodeno_t ino)
{
  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_DESTROY_COMMIT, ino);
  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORTABLE, MDS_PORT_ANCHORCLIENT);
}



void AnchorClient::prepare_update(inodeno_t ino, vector<Anchor>& trace, Context *onfinish)
{
  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_UPDATE_PREPARE, ino);
  req->set_trace(trace);
  
  pending_op[ino] = onfinish;
  
  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORTABLE, MDS_PORT_ANCHORCLIENT);
}

void AnchorClient::commit_update(inodeno_t ino)
{
  // send message
  MAnchor *req = new MAnchor(ANCHOR_OP_UPDATE_COMMIT, ino);
  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORTABLE, MDS_PORT_ANCHORCLIENT);
}


