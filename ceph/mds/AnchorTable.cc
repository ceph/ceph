
#include "AnchorTable.h"
#include "MDS.h"

#include "msg/Messenger.h"
#include "messages/MAnchorRequest.h"
#include "messages/MAnchorReply.h"

#include "include/config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "anchortable: "



/*
 * basic updates
 */

bool AnchorTable::add(inodeno_t ino, inodeno_t dirino, string& ref_dn) 
{
  dout(7) << "add " << ino << " dirino " << dirino << " ref_dn " << ref_dn << endl;

  // parent should be there
  assert(anchor_map.count(dirino));
  
  if (anchor_map.count(ino) == 0) {
	// new item
	anchor_map[ ino ] = new Anchor(ino, dirino, ref_dn);
	dout(10) << "  add: added " << ino << endl;
	return true;
  } else {
	dout(10) << "  add: had " << ino << endl;
	return false;
  }
}

void AnchorTable::inc(inodeno_t ino)
{
  dout(7) << "inc " << ino << endl;

  assert(anchor_map.count(ino) != 0);
  Anchor *anchor = anchor_map[ino];

  while (anchor) {
	anchor->nref++;
	  
	dout(10) << "  inc: record " << ino << " now " << anchor->nref << endl;
	ino = anchor->dirino;
	
	if (ino == 0) break;
	anchor = anchor_map[ino];	  
  }
}

void AnchorTable::dec(inodeno_t ino) 
{
  dout(7) << "dec " << ino << endl;

  assert(anchor_map.count(ino) != 0);
  Anchor *anchor = anchor_map[ino];
  
  while (anchor) {
	anchor->nref--;
	  
	if (anchor->nref == 0) {
	  dout(10) << "  dec: record " << ino << " now 0, removing" << endl;
	  inodeno_t dirino = anchor->dirino;
	  anchor_map.erase(ino);
	  delete anchor;
	  ino = dirino;
	} else {
	  dout(10) << "  dec: record " << ino << " now " << anchor->nref << endl;
	  ino = anchor->dirino;
	}
	
	if (ino == 0) break;
	anchor = anchor_map[ino];	  
  }
}


/* 
 * high level 
 */

void AnchorTable::lookup(inodeno_t ino, vector<Anchor*>& trace)
{
  dout(7) << "lookup " << ino << endl;

  assert(anchor_map.count(ino) == 1);
  Anchor *anchor = anchor_map[ino];
  
  while (anchor) {
	dout(10) << "  record " << anchor->ino << " dirino " << anchor->dirino << " ref_dn " << anchor->ref_dn << endl;
	trace.push_back(anchor);

	if (anchor->dirino == 0) break;
	anchor = anchor_map[anchor->dirino];
  }
}

void AnchorTable::create(inodeno_t ino, vector<Anchor*>& trace)
{
  dout(7) << "create " << ino << endl;
  
  // make sure trace is in table
  for (int i=trace.size()-1; i>=0; i--)
	if (!add(trace[i]->ino, trace[i]->dirino, trace[i]->ref_dn)) break;

  inc(ino);  // ok!
}

void AnchorTable::destroy(inodeno_t ino)
{
  dec(ino);
}



/*
 * messages 
 */

void AnchorTable::proc_message(Message *m)
{
  switch (m->get_type()) {
  case MSG_MDS_ANCHORREQUEST:
	handle_anchor_request((MAnchorRequest*)m);
	break;
		
  case MSG_MDS_ANCHORREPLY:
	handle_anchor_reply((MAnchorReply*)m);
	break;
	
  default:
	assert(0);
  }
}

void AnchorTable::handle_anchor_request(class MAnchorRequest *m)
{
  MAnchorReply *reply = new MAnchorReply(m);
  
  switch (m->get_op()) {

  case ANCHOR_OP_LOOKUP:
	lookup( m->get_ino(), reply->get_trace() );
	break;

  case ANCHOR_OP_CREATE:
	create( m->get_ino(), m->get_trace() );
	break;

  case ANCHOR_OP_DESTROY:
	destroy( m->get_ino() );
	break;

  default:
	assert(0);
  }

  // send reply
  mds->messenger->send_message(reply, m->get_source(), m->get_source_port());
  delete m;
}

void AnchorTable::handle_anchor_reply(class MAnchorReply *m)
{
  switch (m->get_op()) {

  case ANCHOR_OP_LOOKUP:
	{
	  assert(pending_lookup_trace.count(m->get_ino()) == 1);

	  *(pending_lookup_trace[ m->get_ino() ]) = m->get_trace();
	  Context *onfinish = pending_lookup_context[ m->get_ino() ];

	  pending_lookup_trace.erase(m->get_ino());
	  pending_lookup_context.erase(m->get_ino());

	  onfinish->finish(0);
	  delete onfinish;
	}
	break;

  case ANCHOR_OP_CREATE:
  case ANCHOR_OP_DESTROY:
	{
	  assert(pending_op.count(m->get_ino()) == 1);

	  Context *onfinish = pending_op[m->get_ino()];
	  pending_op.erase(m->get_ino());

	  onfinish->finish(0);
	  delete onfinish;
	}
	break;

  default:
	assert(0);
  }

}


/*
 * public async interface
 */

void AnchorTable::lookup(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish)
{
  // me?
  if (mds->get_nodeid() == 0) {
	lookup(ino, trace);
	onfinish->finish(0);
	delete onfinish;
	return;
  }

  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_LOOKUP, ino);

  pending_lookup_trace[ino] = &trace;
  pending_lookup_context[ino] = onfinish;

  mds->messenger->send_message(req, MSG_ADDR_MDS(0), MDS_PORT_ANCHORMGR);
}

void AnchorTable::create(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish)
{
  // me?
  if (mds->get_nodeid() == 0) {
	create(ino, trace);
	onfinish->finish(0);
	delete onfinish;
	return;
  }

  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_CREATE, ino);
  req->set_trace(trace);

  pending_op[ino] = onfinish;

  mds->messenger->send_message(req, MSG_ADDR_MDS(0), MDS_PORT_ANCHORMGR);
}

void AnchorTable::destroy(inodeno_t ino, Context *onfinish)
{
  // me?
  if (mds->get_nodeid() == 0) {
	destroy(ino);
	onfinish->finish(0);
	delete onfinish;
	return;
  }

  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_DESTROY, ino);

  pending_op[ino] = onfinish;

  mds->messenger->send_message(req, MSG_ADDR_MDS(0), MDS_PORT_ANCHORMGR);
}






// primitive load/save for now!

// ** write me
