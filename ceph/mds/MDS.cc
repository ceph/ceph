
#include "include/types.h"
#include "include/Messenger.h"
#include "include/Clock.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDStore.h"
#include "MDLog.h"
#include "MDCluster.h"
#include "MDBalancer.h"

#include "messages/MPing.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWriteReply.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

#include "events/EInodeUpdate.h"

#include <list>

#include <iostream>
using namespace std;



void C_MDS_RetryMessage::redelegate(MDS *mds, int newmds)
{
  // forward message to new mds
  cout << "mds" << mds->get_nodeid() << " redelegating by forwarding message to mds" << newmds << endl;

  mds->messenger->send_message(m,
							   newmds, m->get_dest_port(),
							   MDS_PORT_MAIN);  // mostly meaningless
}



// extern 
//MDS *g_mds;


// cons/des
MDS::MDS(MDCluster *mdc, int whoami, Messenger *m) {
  this->whoami = whoami;
  mdcluster = mdc;

  messenger = m;

  mdcache = new MDCache(this);
  mdstore = new MDStore(this);
  mdlog = new MDLog(this);
  balancer = new MDBalancer(this);

  mdlog->set_max_events(100);

  shutting_down = false;

  stat_ops = 0;
  last_heartbeat = 0;
  osd_last_tid = 0;
}
MDS::~MDS() {
  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (mdstore) { delete mdstore; mdstore = NULL; }
  if (mdlog) { delete mdlog; mdlog = NULL; }
  if (balancer) { delete balancer; balancer = NULL; }

  if (messenger) { delete messenger; messenger = NULL; }
}


int MDS::init()
{
  // init messenger
  messenger->init(this);
}


int MDS::shutdown_start()
{
  cout << "mds" << whoami << " shutdown_start" << endl;
  shutting_down = true;
  for (int i=0; i<mdcluster->get_num_mds(); i++) {
	if (i == whoami) continue;
	cout << "mds" << whoami << " sending MShutdownStart to mds" << i << endl;
	messenger->send_message(new Message(MSG_MDS_SHUTDOWNSTART),
							i, MDS_PORT_MAIN,
							MDS_PORT_MAIN);
  }

  mdcache->shutdown_pass();
}


void MDS::handle_shutdown_start(Message *m)
{
  cout << "mds" << whoami << " handle_shutdown_start" << endl;
  shutting_down = true;
  delete m;
}

void MDS::handle_shutdown_finish(Message *m)
{
  cout << "mds" << whoami << " handle_shutdown_finish from " << m->get_source() << endl;
  did_shut_down.insert(m->get_source());
  cout << "mds" << whoami << " shut down so far: " << did_shut_down << endl;
  delete m;

  if (did_shut_down.size() == mdcluster->get_num_mds()) {
	shutting_down = false;
  }
}



int MDS::shutdown_final()
{
  // shut down cache
  mdcache->shutdown();
  
  // shut down messenger
  messenger->shutdown();

  return 0;
}



mds_load_t MDS::get_load()
{
  mds_load_t l;
  if (mdcache->get_root()) 
	l.root_pop = mdcache->get_root()->popularity.get();
  else
	l.root_pop = 0;
  l.req_rate = stat_req.get();
  l.rd_rate = stat_read.get();
  l.wr_rate = stat_write.get();
  return l;
}



void MDS::proc_message(Message *m) 
{
  
  //if (whoami == 8)	mdcache->show_imports();

  switch (m->get_type()) {
	// MISC
  case MSG_PING:
	handle_ping((MPing*)m);
	break;

	
	// MDS
  case MSG_MDS_SHUTDOWNSTART:
	handle_shutdown_start(m);
	break;

  case MSG_MDS_SHUTDOWNFINISH:
	handle_shutdown_finish(m);
	break;

	// CLIENTS ===========
  case MSG_CLIENT_REQUEST:
	handle_client_request((MClientRequest*)m);
	break;


	// OSD ===============
  case MSG_OSD_READREPLY:
	cout << "mds" << whoami << " osd_read reply" << endl;
	osd_read_finish(m);
	break;

  case MSG_OSD_WRITEREPLY:
	cout << "mds" << whoami << " write reply!" << endl;
	osd_write_finish(m);
	break;
	
  default:
	cout << "mds" << whoami << " main unknown message " << m->get_type() << endl;
	assert(0);
  }

}


void MDS::dispatch(Message *m)
{
  switch (m->get_dest_port()) {
	
  case MDS_PORT_STORE:
	mdstore->proc_message(m);
	break;
	

  case MDS_PORT_CACHE:
	mdcache->proc_message(m);
	break;

	/*
  case MSG_PORT_MDLOG:
	mymds->logger->proc_message(m);
	break;
	*/
	
  case MDS_PORT_BALANCER:
	balancer->proc_message(m);
	break;
	

  case MDS_PORT_MAIN:
  case MDS_PORT_SERVER:
	proc_message(m);
	break;

  default:
	cout << "MDS dispatch unkown message port" << m->get_dest_port() << endl;
  }

  if (whoami == 0 &&
	  stat_ops >= last_heartbeat + 3000) {
	last_heartbeat = stat_ops;
	balancer->send_heartbeat();
  }

  if (shutting_down) {
	if (mdcache->shutdown_pass())
	  shutting_down = false;
  }

}


void MDS::handle_ping(MPing *m)
{
  cout << "mds" << whoami << " received ping from " << MSG_ADDR_NICE(m->get_source()) << " with ttl " << m->ttl << endl;
  if (m->ttl > 0) {
	//cout << "mds" << whoami << " responding to " << m->get_source() << endl;
	messenger->send_message(new MPing(m->ttl - 1),
							m->get_source(), m->get_source_port(),
							MDS_PORT_MAIN);
  }

  delete m;
}


int MDS::handle_client_request(MClientRequest *req)
{
  cout << "mds" << whoami << " req client" << req->client << '.' << req->tid << " op " << req->op << " on " << req->path <<  endl;

  if (is_shutting_down()) {
	cout << "mds" << whoami << " shutting down, discarding client request." << endl;
	delete req;
	return 0;
  }
  
  if (!mdcache->get_root()) {
	cout << "mds" << whoami << " need open root" << endl;
	open_root(new C_MDS_RetryMessage(this, req));
	return 0;
  }

  
  vector<CInode*> trace;
  
  int r = mdcache->path_traverse(req->path, trace, req, MDS_TRAVERSE_FORWARD);
  if (r > 0) return 0;  // delayed

  CInode *cur = trace[trace.size()-1];
  
  cur->hit();   // bump popularity

  MClientReply *reply = 0;

  switch(req->op) {
  case MDS_OP_READDIR:
	reply = handle_client_readdir(req, cur);
	break;

  case MDS_OP_STAT:
	reply = handle_client_stat(req, cur);
	break;

  case MDS_OP_TOUCH:
	reply = handle_client_touch(req, cur);
	break;

  default:
	cout << "mds" << whoami << " unknown mop " << req->op << endl;
	assert(0);
  }

  if (reply) {  
	// this is convenience, for quick events.  
	// anything delayed has to reply on its own.


	// reply
	messenger->send_message(reply,
							MSG_ADDR_CLIENT(req->client), 0,
							MDS_PORT_SERVER);
	
	// discard request
	delete req;
  }
  
  return 0;
}


MClientReply *MDS::handle_client_stat(MClientRequest *req,
									  CInode *cur)
{
  //if (mdcache->read_start(cur, req))
  //return 0;   // ugh

  cout << "mds" << whoami << " reply to client" << req->client << '.' << req->tid << " stat " << cur->inode.touched << " pop " << cur->popularity.get() << endl;
  MClientReply *reply = new MClientReply(req);
  reply->result = 0;
  reply->set_trace_dist( cur, whoami );

  // FIXME: put inode info in reply...

  //mdcache->read_finish(cur);

  stat_read.hit();
  stat_req.hit();
  stat_ops++;
  return reply;
}


class C_MDS_TouchFinish : public Context {
public:
  CInode *in;
  MClientRequest *req;
  MDS *mds;
  C_MDS_TouchFinish(MDS *mds, MClientRequest *req, CInode *cur) {
	this->mds = mds;
	this->in = cur;
	this->req = req;
  }
  virtual void finish(int result) {
	mds->handle_client_touch_2(req, in);
  }
};

MClientReply *MDS::handle_client_touch(MClientRequest *req,
									   CInode *cur)
{
  int auth = cur->authority(mdcluster);

  if (auth == whoami) {
	
	if (!cur->can_hard_pin()) {
	  // wait
	  cur->add_hard_pin_waiter(new C_MDS_RetryMessage(this, req));
	  return 0;
	}
	
	// lock
	cur->hard_pin();

	// do update
	cur->inode.mtime++; // whatever
	cur->inode.touched++;
	cur->version++;

	// tell replicas
	mdcache->send_inode_updates(cur);

	// log it
	cout << "mds" << whoami << " log for client" << req->client << '.' << req->tid << " touch " << cur->inode.touched << endl;
	mdlog->submit_entry(new EInodeUpdate(cur),
						new C_MDS_TouchFinish(this, req, cur));
	return 0;
  } else {

	// forward
	cout << "mds" << whoami << " forwarding touch to authority " << auth << endl;
	messenger->send_message(req,
							MSG_ADDR_MDS(auth), MDS_PORT_SERVER,
							MDS_PORT_SERVER);
	return 0;
  }
}

						   
void MDS::handle_client_touch_2(MClientRequest *req,
								CInode *cur)
{
  // reply
  cout << "mds" << whoami << " reply to client" << req->client << '.' << req->tid << " touch" << endl;
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  
  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->client), 0,
						  MDS_PORT_SERVER);


  stat_write.hit();
  stat_req.hit();
  stat_ops++;

  // done
  delete req;

  // unpin
  cur->hard_unpin(); 
}


MClientReply *MDS::handle_client_readdir(MClientRequest *req,
										 CInode *cur)
{
  // it's a directory, right?
  if (!cur->is_dir()) {
	// not a dir
	cout << "mds" << whoami << " reply to client" << req->client << '.' << req->tid << " readdir -ENOTDIR" << endl;
	return new MClientReply(req, -ENOTDIR);
  }
	
  if (!cur->dir) cur->dir = new CDir(cur);
  
  // frozen?
  if (cur->dir->is_frozen()) {
	// doh!
	cout << "mds" << whoami << " dir is frozen, waiting" << endl;
	cur->dir->add_freeze_waiter(new C_MDS_RetryMessage(this, req));
	return 0;
  }
  
  // make sure i'm authoritative!
  int dirauth = cur->dir_authority(mdcluster);          // FIXME hashed, etc.
  if (dirauth == whoami) {
	
	if (cur->dir->is_complete()) {
	  // yay, replly
	  MClientReply *reply = new MClientReply(req);
	  
	  // build dir contents
	  CDir_map_t::iterator it;
	  int numfiles = 0;
	  for (it = cur->dir->begin(); it != cur->dir->end(); it++) {
		CInode *in = it->second->inode;
		c_inode_info *i = new c_inode_info;
		i->inode = in->inode;
		in->get_dist_spec(i->dist, whoami);
		i->ref_dn = it->first;
		reply->add_dir_item(i);
		numfiles++;
	  }
	  
	  cout << "mds" << whoami << " reply to client" << req->client << '.' << req->tid << " readdir " << numfiles << " files" << endl;
	  reply->set_trace_dist( cur, whoami );

	  stat_read.hit();
	  stat_req.hit();
	  stat_ops++;

	  return reply;
	} else {
	  // fetch
	  cout << "mds" << whoami << " incomplete dir contents for readdir on " << *cur << ", fetching" << endl;
	  mdstore->fetch_dir(cur, new C_MDS_RetryMessage(this, req));
	  return 0;
	}
  } else {
	if (dirauth < 0) {
	  assert(dirauth >= 0);
	} else {
	  // forward to authority
	  cout << "mds" << whoami << " forwarding readdir to authority " << dirauth << endl;
	  messenger->send_message(req,
							  MSG_ADDR_MDS(dirauth), MDS_PORT_SERVER,
							  MDS_PORT_SERVER);
	  mdcache->show_imports();
	}
	return 0;
  }
}




void split_path(string& path, 
				vector<string>& bits)
{
  int off = 0;
  while (off < path.length()) {
	// skip trailing/duplicate slash(es)
	int nextslash = path.find('/', off);
	if (nextslash == off) {
	  off++;
	  continue;
	}
	if (nextslash < 0) 
	  nextslash = path.length();  // no more slashes

	bits.push_back( path.substr(off,nextslash-off) );
	off = nextslash+1;
  }
}

















// OSD fun

int MDS::osd_read(int osd, object_t oid, size_t len, size_t offset, char **bufptr, size_t *read, Context *c)
{
  osd_last_tid++;
  MOSDRead *m = new MOSDRead(osd_last_tid,
							 oid,
							 len, offset);
  
  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->bufptr = bufptr;
  p->buf = 0;
  p->bytesread = read;
  p->context = c;
  osd_reads[osd_last_tid] = p;

  messenger->send_message(m,
						  MSG_ADDR_OSD(osd),
						  0, MDS_PORT_MAIN);
}


int MDS::osd_read(int osd, object_t oid, size_t len, size_t offset, char *buf, size_t *bytesread, Context *c)
{
  osd_last_tid++;
  MOSDRead *m = new MOSDRead(osd_last_tid,
							 oid,
							 len, offset);

  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->buf = buf;
  p->bytesread = bytesread;
  p->context = c;
  osd_reads[osd_last_tid] = p;

  messenger->send_message(m,
						  MSG_ADDR_OSD(osd),
						  0, MDS_PORT_MAIN);
}


int MDS::osd_read_finish(Message *rawm) 
{
  MOSDReadReply *m = (MOSDReadReply*)rawm;
  
  // get pio
  PendingOSDRead_t *p = osd_reads[ m->tid ];
  osd_reads.erase( m->tid );
  Context *c = p->context;

  if (m->len >= 0) {
	// success!  
	*p->bytesread = m->len;

	if (p->buf) { // user buffer
	  memcpy(p->buf, m->buf, m->len);  // copy
	  delete[] m->buf;                 // free message buf
	} else {      // new buffer
	  *p->bufptr = m->buf;     // steal message's buffer
	}
	m->buf = 0;
  }

  // del pendingOsdRead_t
  delete p;

  long result = m->len;
  delete m;  // del message

  if (c) {
	c->finish(result);
	delete c;
  }
}



// -- osd_write

int MDS::osd_write(int osd, object_t oid, size_t len, size_t offset, char *buf, int flags, Context *c)
{
  osd_last_tid++;

  char *nbuf = new char[len];
  memcpy(nbuf, buf, len);

  MOSDWrite *m = new MOSDWrite(osd_last_tid,
							   oid,
							   len, offset,
							   nbuf, flags);
  osd_writes[ osd_last_tid ] = c;
  cout << "mds: sending MOSDWrite " << m->get_type() << endl;
  messenger->send_message(m,
						  MSG_ADDR_OSD(osd),
						  0, MDS_PORT_MAIN);
}


int MDS::osd_write_finish(Message *rawm)
{
  MOSDWriteReply *m = (MOSDWriteReply *)rawm;

  Context *c = osd_writes[ m->tid ];
  osd_writes.erase(m->tid);

  long result = m->len;
  delete m;

  cout << "mds" << whoami << " finishing osd_write" << endl;

  if (c) {
	c->finish(result);
	delete c;
  }
}



// ---------------------------
// open_root

bool MDS::open_root(Context *c)
{
  mdcache->open_root(c);
}
