
#include "include/types.h"
#include "include/Messenger.h"
#include "include/Clock.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDStore.h"
#include "MDLog.h"
#include "MDCluster.h"
#include "MDBalancer.h"

#include "include/filepath.h"

#include "include/Logger.h"
#include "include/LogType.h"

#include "messages/MPing.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWriteReply.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

#include "messages/MInodeUnlink.h"

#include "events/EInodeUpdate.h"
#include "events/EInodeUnlink.h"

#include <list>

#include <iostream>
using namespace std;


LogType mds_logtype;

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "mds" << whoami << " "
#define  dout3(l,mds)    if (l<=g_conf.debug) cout << "mds" << mds->get_nodeid() << " "



ostream& operator<<(ostream& out, MDS& mds)
{
  out << "mds" << mds.get_nodeid() << " ";
}

void C_MDS_RetryMessage::redelegate(MDS *mds, int newmds)
{
  // forward message to new mds
  dout3(5,mds) << "redelegating context " << this << " by forwarding message " << m << " to mds" << newmds << endl;

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

  mdlog->set_max_events(g_conf.mdlog_max_len);

  shutting_down = false;
  shut_down = false;

  stat_ops = 0;
  last_heartbeat = 0;
  osd_last_tid = 0;

  // log
  string name;
  name = "mds";
  int w = MSG_ADDR_NUM(whoami);
  if (w >= 1000) name += ('0' + ((w/1000)%10));
  if (w >= 100) name += ('0' + ((w/100)%10));
  if (w >= 10) name += ('0' + ((w/10)%10));
  name += ('0' + ((w/1)%10));

  logger = new Logger(name, (LogType*)&mds_logtype);
}

MDS::~MDS() {
  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (mdstore) { delete mdstore; mdstore = NULL; }
  if (mdlog) { delete mdlog; mdlog = NULL; }
  if (balancer) { delete balancer; balancer = NULL; }

  if (logger) { delete logger; logger = 0; }

  if (messenger) { delete messenger; messenger = NULL; }
}


int MDS::init()
{
  // init messenger
  messenger->init(this);
}


int MDS::shutdown_start()
{
  dout(1) << "shutdown_start" << endl;
  for (int i=0; i<mdcluster->get_num_mds(); i++) {
	if (i == whoami) continue;
	dout(1) << "sending MShutdownStart to mds" << i << endl;
	messenger->send_message(new Message(MSG_MDS_SHUTDOWNSTART),
							i, MDS_PORT_MAIN,
							MDS_PORT_MAIN);
  }

  handle_shutdown_start(NULL);
}


void MDS::handle_shutdown_start(Message *m)
{
  dout(1) << " handle_shutdown_start" << endl;

  // set flag
  shutting_down = true;

  mdcache->shutdown_start();
  
  // flush log
  mdlog->set_max_events(0);
  mdlog->trim(NULL);

  if (m) delete m;
}

void MDS::handle_shutdown_finish(Message *m)
{
  int mds = whoami;
  if (m) 
	mds = m->get_source();
						 
  dout(2) << "handle_shutdown_finish from " << mds << endl;
  did_shut_down.insert(mds);
  dout(2) << " shut down so far: " << did_shut_down << endl;
  
  if (did_shut_down.size() == mdcluster->get_num_mds()) {
	shutting_down = false;
	messenger->done();
  }

  // done
  delete m;
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
	l.root_pop = mdcache->get_root()->get_popularity();
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

  case MSG_CLIENT_DONE:
	handle_client_done(m);
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
	osd_read_finish(m);
	break;

  case MSG_OSD_WRITEREPLY:
	osd_write_finish(m);
	break;
	
  default:
	dout(1) << " main unknown message " << m->get_type() << endl;
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
	dout(1) << "MDS dispatch unkown message port" << m->get_dest_port() << endl;
  }

  if (whoami == 0 &&
	  stat_ops >= last_heartbeat + g_conf.mds_heartbeat_op_interval) {
	last_heartbeat = stat_ops;
	balancer->send_heartbeat();
  }

  if (shutting_down && !shut_down) {
	if (mdcache->shutdown_pass()) {
	  shutting_down = false;
	  shut_down = true;
	}
  }

}


void MDS::handle_client_done(Message *m)
{
  int n = MSG_ADDR_NUM(m->get_source());
  dout(3) << "client" << n << " done" << endl;
  done_clients.insert(n);
  if (done_clients.size() == g_conf.num_client) {
	dout(3) << "all clients done, initiating shutdown" << endl;
	shutdown_start();
  }

  delete m;  // done
}


void MDS::handle_ping(MPing *m)
{
  dout(10) << " received ping from " << MSG_ADDR_NICE(m->get_source()) << " with ttl " << m->ttl << endl;
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
  dout(10) << "req " << *req << endl;

  if (is_shutting_down()) {
	dout(5) << " shutting down, discarding client request." << endl;
	delete req;
	return 0;
  }
  
  if (!mdcache->get_root()) {
	dout(5) << "need to open root" << endl;
	open_root(new C_MDS_RetryMessage(this, req));
	return 0;
  }

  
  // operations on ino's or possibly non-existing files

  switch (req->get_op()) {
  case MDS_OP_CLOSE:
	handle_client_close(req);
	return 0;

  case MDS_OP_OPENWRC:
	handle_client_openwrc(req);
	return 0;
  }

  
  // operations that require existing files

  vector<CInode*> trace;
  int r = mdcache->path_traverse(req->get_path(), trace, req, MDS_TRAVERSE_FORWARD);
  if (r > 0) return 0;  // delayed

  logger->inc("chit");

  CInode *cur = trace[trace.size()-1];
  
  balancer->hit_inode(cur, MDS_POP_ANY);   // bump popularity

  MClientReply *reply = 0;

  switch(req->get_op()) {
  case MDS_OP_READDIR:
	reply = handle_client_readdir(req, cur);
	break;

  case MDS_OP_STAT:
	reply = handle_client_stat(req, cur);
	break;

  case MDS_OP_TOUCH:
	reply = handle_client_touch(req, cur);
	break;

  case MDS_OP_CHMOD:
	reply = handle_client_chmod(req, cur);
	break;

  case MDS_OP_OPENRD:
	reply = handle_client_openrd(req, cur);
	break;
  case MDS_OP_OPENWR:
	reply = handle_client_openwr(req, cur);
	break;

  case MDS_OP_UNLINK:
	handle_client_unlink(req, cur);
	break;

  default:
	dout(1) << " unknown mop " << req->get_op() << endl;
	assert(0);
  }

  if (reply) {  
	// this is convenience, for quick events.  
	// anything delayed has to reply and delete the request on its own.

	// reply
	messenger->send_message(reply,
							MSG_ADDR_CLIENT(req->get_client()), 0,
							MDS_PORT_SERVER);
	
	// discard request
	delete req;
  }
  
  return 0;
}


MClientReply *MDS::handle_client_stat(MClientRequest *req,
									  CInode *cur)
{
  if (!mdcache->read_soft_start(cur, req))
	return 0;  // sync

  dout(10) << "reply to " << *req << " stat " << cur->inode.touched << " pop " << cur->get_popularity() << endl;
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );

  // FIXME: put inode info in reply...

  mdcache->read_soft_finish(cur);

  logger->inc("ostat");
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
  if (!cur->can_auth_pin()) {
	// wait
	cur->add_waiter(CINODE_WAIT_AUTHPINNABLE,
					new C_MDS_RetryMessage(this, req));
	return 0;
  }
	
  // write
  if (!mdcache->write_soft_start(cur, req))
	return 0;  // fw or (wait for) sync

  cur->auth_pin();
  
  // do update
  cur->inode.mtime++; // whatever
  cur->inode.touched++;
  cur->mark_dirty();
  
  // tell replicas
  // actually, no!  it's synced by me, or async.  they'll get told upon release.  
  //mdcache->send_inode_updates(cur);
  
  // log it
  dout(10) << "log for " << *req << " touch " << cur->inode.touched << endl;
  mdlog->submit_entry(new EInodeUpdate(cur),
					  new C_MDS_TouchFinish(this, req, cur));
  return 0;
}

						   
void MDS::handle_client_touch_2(MClientRequest *req,
								CInode *cur)
{
  // reply
  dout(10) << "reply to " << *req << " touch" << endl;
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  reply->set_result(0);
  
  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->get_client()), 0,
						  MDS_PORT_SERVER);

  logger->inc("otouch");
  stat_write.hit();
  stat_req.hit();
  stat_ops++;

  // done
  delete req;

  // unpin
  cur->auth_unpin();
  mdcache->write_soft_finish(cur);
}



class C_MDS_ChmodFinish : public Context {
public:
  CInode *in;
  MClientRequest *req;
  MDS *mds;
  C_MDS_ChmodFinish(MDS *mds, MClientRequest *req, CInode *cur) {
	this->mds = mds;
	this->in = cur;
	this->req = req;
  }
  virtual void finish(int result) {
	mds->handle_client_chmod_2(req, in);
  }
};


MClientReply *MDS::handle_client_chmod(MClientRequest *req,
									   CInode *cur)
{
  if (!cur->can_auth_pin()) {
	// wait
	cur->add_waiter(CINODE_WAIT_AUTHPINNABLE,
					new C_MDS_RetryMessage(this, req));
	return 0;
  }
	
  // write
  if (!mdcache->write_hard_start(cur, req))
	return 0;  // fw or (wait for) lock

  cur->auth_pin();
  
  // do update
  cur->inode.mtime++; // whatever
  cur->inode.touched++; // blah
  cur->mark_dirty();

  // log it
  dout(10) << "log for " << *req << " chmod" << endl;
  mdlog->submit_entry(new EInodeUpdate(cur),
					  new C_MDS_ChmodFinish(this, req, cur));
  return 0;
}


void MDS::handle_client_chmod_2(MClientRequest *req,
								CInode *cur)
{
  // reply
  dout(10) << "handle_client_chmod_2 reply to " << *req << " chmod " << endl;
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  reply->set_result(0);
  
  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->get_client()), 0,
						  MDS_PORT_SERVER);
  
  logger->inc("otouch");
  stat_write.hit();
  stat_req.hit();
  stat_ops++;

  // done
  delete req;
  
  // unpin
  cur->auth_unpin();
  mdcache->write_hard_finish(cur);
}



MClientReply *MDS::handle_client_readdir(MClientRequest *req,
										 CInode *cur)
{
  if (!mdcache->read_hard_try(cur,req))
	return NULL;

  // it's a directory, right?
  if (!cur->is_dir()) {
	// not a dir
	dout(10) << "reply to " << *req << " readdir -ENOTDIR" << endl;
	return new MClientReply(req, -ENOTDIR);
  }

  // make sure i'm authoritative!
  int dirauth = cur->dir_authority(mdcluster);          // FIXME hashed, etc.
  if (dirauth == whoami) {
	
	if (!cur->dir) cur->dir = new CDir(cur, whoami);
	assert(cur->dir->is_auth());
	
	// frozen?
	if (cur->dir->is_frozen()) {
	  // doh!
	  dout(10) << " dir is frozen, waiting" << endl;
	  cur->dir->add_waiter(CDIR_WAIT_UNFREEZE,
						   new C_MDS_RetryMessage(this, req));
	  return 0;
	}
  
	if (cur->dir->is_complete()) {
	  // yay, reply
	  MClientReply *reply = new MClientReply(req);
	  
	  // FIXME: need to sync all inodes in this dir.  blech!

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
	  
	  dout(10) << "reply to " << *req << " readdir " << numfiles << " files" << endl;
	  reply->set_trace_dist( cur, whoami );
	  reply->set_result(0);

	  logger->inc("ordir");
	  stat_read.hit();
	  stat_req.hit();
	  stat_ops++;

	  return reply;
	} else {
	  // fetch
	  dout(10) << " incomplete dir contents for readdir on " << *cur << ", fetching" << endl;
	  mdstore->fetch_dir(cur, new C_MDS_RetryMessage(this, req));
	  return 0;
	}
  } else {
	if (cur->dir) assert(!cur->dir->is_auth());

	if (dirauth < 0) {
	  assert(dirauth >= 0);
	} else {
	  // forward to authority
	  dout(10) << " forwarding readdir to authority " << dirauth << endl;
	  messenger->send_message(req,
							  MSG_ADDR_MDS(dirauth), MDS_PORT_SERVER,
							  MDS_PORT_SERVER);
	  //mdcache->show_imports();
	}
	return 0;
  }
}


MClientReply *MDS::handle_client_openrd(MClientRequest *req,
										CInode *cur)
{
  dout(10) << "open (read) on " << *cur << endl;
  
  // hmm, check permissions or something.
 
  // add reader
  int client = req->get_client();
  cur->open_read_add(client);

  // reply
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  return reply;
}

void MDS::handle_client_close(MClientRequest *req) 
{
  CInode *cur = mdcache->get_inode(req->get_ino());
  assert(cur);

  dout(10) << "close on " << *cur << endl;
  
  // hmm, check permissions or something.
  
  // verify on read or write list
  int client = req->get_client();
  if (!cur->open_remove(client)) {
	dout(1) << "close on unopen file " << *cur << endl;
	assert(2+2==5);
  }
  
  // reply
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );

  messenger->send_message(reply,
						  req->get_source(), 0, MDS_PORT_SERVER);

  // done
  delete req;
}


MClientReply *MDS::handle_client_openwr(MClientRequest *req,
										CInode *cur)
{
  if (!cur->is_auth()) {
	if (!cur->is_softasync()) {
	  int auth = cur->authority(get_cluster());
	  assert(auth != whoami);
	  dout(10) << "open (write) [replica] " << *cur << " on replica, fw to auth " << auth << endl;
	  
	  messenger->send_message(req,
							  MSG_ADDR_MDS(auth), MDS_PORT_SERVER,
							  MDS_PORT_SERVER);
	  return 0;
	}

	dout(10) << "open (write) [replica shared write] " << *cur << endl;
	assert(0);
  }

  dout(10) << "open (write) [auth] " << *cur << endl;

  // hmm, check permissions!
  
  // add to writer list.
  int client = req->get_client();
  cur->open_write_add(client);
	  
  // reply
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  return reply;
}


/*
int path_depth(string& p)
{
  int d = 0;
  for (const char *c = p.c_str(); *c; c++)
	if (*c == '/') d++;
  return d;
}
*/

void MDS::handle_client_openwrc(MClientRequest *req)
{
  
  // see if file exists
  vector<CInode*> trace;
  int r = mdcache->path_traverse(req->get_path(), trace, req, MDS_TRAVERSE_FORWARD);
  if (r > 0) return;  // delayed

  if (r < 0) {
	// problems:
	
	if (r == -ENOENT) {
	  // on the last bit?
	  filepath path = req->get_path();
	  int depth = path.depth();
	  if (trace.size() == depth) { // everything but the file
		// create dentry, file!
		CInode *idir = trace[trace.size()-1];
		assert(idir->dir->is_auth() || idir->dir->is_hashed());  // path_traverse should have fwd if not!
		string dname = path.last_bit();

		dout(7) << "handle_client_openwrc -ENOENT on target file, creating " << dname << endl;

		// verify i am authoritative for this dentry (should have fwd if not)
		int auth = idir->dir->dentry_authority(dname, get_cluster());
		assert(auth == whoami);

		// create inode and link
		CInode *in = mdcache->create_inode();
		mdcache->link_inode( idir, dname, in );

		in->mark_dirty();

		// log it
		dout(10) << "log for " << *req << " create " << in->ino() << endl;
		mdlog->submit_entry(new EInodeUpdate(in),                    // FIXME should be differnet log entry
							new C_MDS_RetryMessage(this, req));
		return;
	  } else {
		dout(7) << "handle_client_openwrc -ENOENT on containing dir; fail!" << endl;
	  }
	}
	
	// send error response
	dout(7) << "handle_client_openwrc error " << r << " replying to client" << endl;
	MClientReply *reply = new MClientReply(req, r);
	messenger->send_message(reply,
							MSG_ADDR_CLIENT(req->get_client()), 0,
							MDS_PORT_SERVER);
	delete req;
	return;
  }

  // file exists!  do it.

  logger->inc("chit");
  CInode *cur = trace[trace.size()-1];
  balancer->hit_inode(cur, MDS_POP_ANY);   // bump popularity
  
  MClientReply *reply = handle_client_openwr(req,cur);
  
  if (reply) {
	messenger->send_message(reply,
							MSG_ADDR_CLIENT(req->get_client()), 0,
							MDS_PORT_SERVER);
	
	// discard request
	delete req;
  }
}




class C_MDS_UnlinkInode : public Context {
public:
  MDS *mds;
  CInode *in;
  MClientRequest *req;
  C_MDS_UnlinkInode(MDS *mds, CInode *in, MClientRequest *req) {
	this->mds = mds;
	this->in = in;
	this->req = req;
  }
  virtual void finish(int r) {
	mds->handle_client_unlink_2(req, in);
  }
};

void MDS::handle_client_unlink(MClientRequest *req, 
							   CInode *in)
{
  // regular files only
  if (in->is_dir()) {
	dout(7) << "handle_client_unlink on dir " << *in << ", returning error" << endl;
	messenger->send_message(new MClientReply(req, -EISDIR),
							MSG_ADDR_CLIENT(req->get_client()), 0, MDS_PORT_SERVER);
	delete req;
	return;
  }
  
  // am i auth?
  if (!in->is_auth()) {
	// not auth; forward!
	int auth = in->authority(get_cluster());
	dout(7) << "handle_client_unlink not auth for " << *in << ", fwd to " << auth << endl;
	messenger->send_message(req,
							MSG_ADDR_MDS(auth), MDS_PORT_SERVER, MDS_PORT_SERVER);
	return;
  }

  dout(7) << "handle_client_unlink on " << *in << endl;
  mdcache->inode_unlink(in, 
						new C_MDS_UnlinkInode(this,in,req));
}



void MDS::handle_client_unlink_2(MClientRequest *req,
								 CInode *in)
{
  dout(7) << "handle_client_unlink_2 done unlinking inode " << *in << endl;
  
  // reply
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist(in, whoami);
  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->get_client()), 0, MDS_PORT_SERVER);

  // done
  delete req;
}













// OSD fun ------------------------

int 
MDS::osd_read(int osd, 
			  object_t oid, 
			  size_t len, 
			  size_t offset, 
			  crope *buffer, 
			  Context *c) 
{
  osd_last_tid++;
  MOSDRead *m = new MOSDRead(osd_last_tid,
							 oid,
							 len, offset);
  
  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->buffer = buffer;
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
  PendingOSDRead_t *p = osd_reads[ m->get_tid() ];
  osd_reads.erase( m->get_tid() );
  Context *c = p->context;

  p->buffer->clear();
  p->buffer->append( m->get_buffer() );
  p->buffer = 0;
  long result = m->get_len();

  delete p;   // del pendingOsdRead_t
  delete m;   // del message
  
  if (c) {
	c->finish(result);
	delete c;
  }
}



// -- osd_write
int 
MDS::osd_write(int osd, 
			   object_t oid, 
			   size_t len, 
			   size_t offset, 
			   crope& buffer, 
			   int flags, 
			   Context *c)
{
  osd_last_tid++;

  MOSDWrite *m = new MOSDWrite(osd_last_tid,
							   oid,
							   len, offset,
							   buffer, flags);
  osd_writes[ osd_last_tid ] = c;

  dout(10) << "sending MOSDWrite " << m->get_type() << endl;
  messenger->send_message(m,
						  MSG_ADDR_OSD(osd),
						  0, MDS_PORT_MAIN);
}


int MDS::osd_write_finish(Message *rawm)
{
  MOSDWriteReply *m = (MOSDWriteReply *)rawm;

  Context *c = osd_writes[ m->get_tid() ];
  osd_writes.erase(m->get_tid());

  long result = m->get_result();
  delete m;

  dout(10) << " finishing osd_write" << endl;

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
