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

#include "messages/MAnchorRequest.h"
#include "messages/MAnchorReply.h"

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << g_clock.now() << " " << messenger->get_myaddr() << ".anchorclient "
#define derr(x)  if (x <= g_conf.debug) cout << g_clock.now() << " " << messenger->get_myaddr() << ".anchorclient "


void AnchorClient::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_MDS_ANCHORREPLY:
    handle_anchor_reply((MAnchorReply*)m);
    break;

  default:
    assert(0);
  }
}

void AnchorClient::handle_anchor_reply(class MAnchorReply *m)
{
  switch (m->get_op()) {

  case ANCHOR_OP_LOOKUP:
    {
      assert(pending_lookup_trace.count(m->get_ino()) == 1);

      *(pending_lookup_trace[ m->get_ino() ]) = m->get_trace();
      Context *onfinish = pending_lookup_context[ m->get_ino() ];

      pending_lookup_trace.erase(m->get_ino());
      pending_lookup_context.erase(m->get_ino());

      if (onfinish) {
        onfinish->finish(0);
        delete onfinish;
      }
    }
    break;

  case ANCHOR_OP_UPDATE:
  case ANCHOR_OP_CREATE:
  case ANCHOR_OP_DESTROY:
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

void AnchorClient::lookup(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish)
{
  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_LOOKUP, ino);

  pending_lookup_trace[ino] = &trace;
  pending_lookup_context[ino] = onfinish;

  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORMGR, MDS_PORT_ANCHORCLIENT);
}

void AnchorClient::create(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish)
{
  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_CREATE, ino);
  req->set_trace(trace);

  pending_op[ino] = onfinish;

  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORMGR, MDS_PORT_ANCHORCLIENT);
}

void AnchorClient::update(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish)
{
  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_UPDATE, ino);
  req->set_trace(trace);
  
  pending_op[ino] = onfinish;
  
  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORMGR, MDS_PORT_ANCHORCLIENT);
}

void AnchorClient::destroy(inodeno_t ino, Context *onfinish)
{
  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_DESTROY, ino);

  pending_op[ino] = onfinish;

  messenger->send_message(req, 
			  mdsmap->get_inst(mdsmap->get_anchortable()),
			  MDS_PORT_ANCHORMGR, MDS_PORT_ANCHORCLIENT);
}


