
#include "AnchorTable.h"
#include "MDS.h"

#include "osd/Filer.h"

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
  assert(dirino < 1000 ||             // system dirino
		 anchor_map.count(dirino));   // have
  
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
  assert(anchor);

  while (1) {
	anchor->nref++;
	  
	dout(10) << "  inc: record " << ino << " now " << anchor->nref << endl;
	ino = anchor->dirino;
	
	if (ino == 0) break;
	if (anchor_map.count(ino) == 0) break;
	anchor = anchor_map[ino];	  
	assert(anchor);
  }
}

void AnchorTable::dec(inodeno_t ino) 
{
  dout(7) << "dec " << ino << endl;

  assert(anchor_map.count(ino) != 0);
  Anchor *anchor = anchor_map[ino];
  assert(anchor);

  while (true) {
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
	if (anchor_map.count(ino) == 0) break;
	anchor = anchor_map[ino];	  
	assert(anchor);
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
  assert(anchor);

  while (true) {
	dout(10) << "  record " << anchor->ino << " dirino " << anchor->dirino << " ref_dn " << anchor->ref_dn << endl;
	trace.insert(trace.begin(), anchor);  // lame FIXME

	if (anchor->dirino < MDS_INO_BASE) break;

	assert(anchor_map.count(anchor->dirino) == 1);
	anchor = anchor_map[anchor->dirino];
	assert(anchor);
  }
}

void AnchorTable::create(inodeno_t ino, vector<Anchor*>& trace)
{
  dout(7) << "create " << ino << endl;
  
  // make sure trace is in table
  for (int i=0; i<trace.size(); i++) 
	add(trace[i]->ino, trace[i]->dirino, trace[i]->ref_dn);

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
  // make sure i'm open!
  if (!opened) {
	dout(7) << "not open yet" << endl;
	
	waiting_for_open.push_back(new C_MDS_RetryMessage(mds,m));
	
	if (!opening) {
	  opening = true;
	  load(0);
	}
	return;
  }

  // go
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
  if (false && mds->get_nodeid() == 0) {
	lookup(ino, trace);
	onfinish->finish(0);
	delete onfinish;
	return;
  }

  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_LOOKUP, ino);

  pending_lookup_trace[ino] = &trace;
  pending_lookup_context[ino] = onfinish;

  mds->messenger->send_message(req, MSG_ADDR_MDS(0), MDS_PORT_ANCHORMGR, MDS_PORT_ANCHORMGR);
}

void AnchorTable::create(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish)
{
  // me?
  if (false && mds->get_nodeid() == 0) {
	create(ino, trace);
	onfinish->finish(0);
	delete onfinish;
	return;
  }

  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_CREATE, ino);
  req->set_trace(trace);

  pending_op[ino] = onfinish;

  mds->messenger->send_message(req, MSG_ADDR_MDS(0), MDS_PORT_ANCHORMGR, MDS_PORT_ANCHORMGR);
}

void AnchorTable::destroy(inodeno_t ino, Context *onfinish)
{
  // me?
  if (false && mds->get_nodeid() == 0) {
	destroy(ino);
	onfinish->finish(0);
	delete onfinish;
	return;
  }

  // send message
  MAnchorRequest *req = new MAnchorRequest(ANCHOR_OP_DESTROY, ino);

  pending_op[ino] = onfinish;

  mds->messenger->send_message(req, MSG_ADDR_MDS(0), MDS_PORT_ANCHORMGR, MDS_PORT_ANCHORMGR);
}






// primitive load/save for now!

// load/save entire table for now!

void AnchorTable::save(Context *onfinish)
{
  dout(7) << "save" << endl;
  if (!opened) return;
  
  // build up write
  crope tab;

  int num = anchor_map.size();
  tab.append((char*)&num, sizeof(int));

  for (hash_map<inodeno_t, Anchor*>::iterator it = anchor_map.begin();
	   it != anchor_map.end();
	   it++) {
	dout(14) << "adding anchor for " << it->first << endl;
	Anchor *a = it->second;
	assert(a);
	a->_rope(tab);
  }

  size_t size = tab.length();
  tab.insert(0, (char*)&size, sizeof(size));

  dout(7) << " " << num << " anchors, " << size << " bytes" << endl;
  
  // write!
  mds->filer->write(MDS_INO_ANCHORTABLE+mds->get_nodeid(),
					tab.length(), 0,
					tab.c_str(), 0, 
					onfinish);
}



class C_AT_Load : public Context {
  AnchorTable *at;
  Context *onfinish;
public:
  size_t size;
  char *dataptr;
  char *freeptr;
  C_AT_Load(size_t size, AnchorTable *at, Context *onfinish) {
	this->size = size;
	this->at = at;
	this->onfinish = onfinish;
  }
  void finish(int result) {
	assert(result > 0);
	crope r;
	r.append(dataptr, result);
	delete freeptr;

	at->load_2(size, r, onfinish);
  }
};

class C_AT_LoadSize : public Context {
  AnchorTable *at;
  MDS *mds;
  Context *onfinish;
public:
  size_t size;
  C_AT_LoadSize(AnchorTable *at, MDS *mds, Context *onfinish) {
	this->at = at;
	this->mds = mds;
	this->onfinish = onfinish;
	size = 0;
  }
  void finish(int r) {
	cout << "r is " << r << " size is " << size << endl;
	if (r > 0 && size > 0) {
	  C_AT_Load *c = new C_AT_Load(size, at, onfinish);
	  mds->filer->read(MDS_INO_ANCHORTABLE+mds->get_nodeid(),
					   size, sizeof(size),
					   &c->dataptr, &c->freeptr,
					   c);
	} else {
	  // fail
	  crope empty;
	  at->load_2(0, empty, onfinish);
	}
  }
};

void AnchorTable::load(Context *onfinish)
{
  dout(7) << "load" << endl;

  assert(!opened);
  
  C_AT_LoadSize *c = new C_AT_LoadSize(this, mds, onfinish);
  mds->filer->read(MDS_INO_ANCHORTABLE+mds->get_nodeid(),
				   sizeof(size_t), 0,
				   (char*)&c->size,
				   c);
}

void AnchorTable::load_2(size_t size, crope& r, Context *onfinish)
{
  // num
  int off = 0;
  int num;
  r.copy(0, sizeof(num), (char*)&num);
  off += sizeof(num);
  
  // parse anchors
  for (int i=0; i<num; i++) {
	Anchor *a = new Anchor;
	a->_unrope(r, off);
	dout(10) << "  load_2 unroped " << a->ino << " dirino " << a->dirino << " ref_dn " << a->ref_dn << endl;
	anchor_map[a->ino] = a;
  }

  dout(7) << "load_2 got " << num << " anchors" << endl;

  opened = true;
  opening = false;

  // finish
  if (onfinish) {
	onfinish->finish(0);
	delete onfinish;
  }
  finish_contexts(waiting_for_open);
}

