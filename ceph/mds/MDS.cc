
#include "include/types.h"
#include "common/Clock.h"

#include "msg/Messenger.h"

#include "osd/OSDCluster.h"
#include "osd/Filer.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDStore.h"
#include "MDLog.h"
#include "MDCluster.h"
#include "MDBalancer.h"
#include "IdAllocator.h"
#include "AnchorTable.h"
#include "OSDMonitor.h"

#include "include/filepath.h"

#include "common/Logger.h"
#include "common/LogType.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MGenericMessage.h"

#include "messages/MOSDGetClusterAck.h"

#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

#include "messages/MLock.h"
#include "messages/MInodeWriterClosed.h"

#include "messages/MInodeLink.h"

#include "events/EInodeUpdate.h"

#include <errno.h>
#include <fcntl.h>

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
  messenger->set_dispatcher(this);

  mdcache = new MDCache(this);
  mdstore = new MDStore(this);
  mdlog = new MDLog(this);
  balancer = new MDBalancer(this);
  anchormgr = new AnchorTable(this);
  osdmonitor = new OSDMonitor(this);


  // <HACK set up OSDCluster from g_conf>
  osdcluster = new OSDCluster();
  OSDGroup osdg;
  osdg.num_osds = g_conf.num_osd;
  for (int i=0; i<osdg.num_osds; i++) osdg.osds.push_back(i);
  osdg.weight = 100;
  osdcluster->add_group(osdg);
  // </HACK>

  filer = new Filer(messenger, osdcluster);

  mdlog->set_max_events(g_conf.mds_log_max_len);

  shutting_down = false;
  shut_down = false;

  stat_ops = 0;
  last_heartbeat = 0;

  // log
  string name;
  name = "mds";
  int w = MSG_ADDR_NUM(whoami);
  if (w >= 1000) name += ('0' + ((w/1000)%10));
  if (w >= 100) name += ('0' + ((w/100)%10));
  if (w >= 10) name += ('0' + ((w/10)%10));
  name += ('0' + ((w/1)%10));

  logger = new Logger(name, (LogType*)&mds_logtype);

  // alloc
  idalloc = new IdAllocator(this);

}

MDS::~MDS() {
  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (mdstore) { delete mdstore; mdstore = NULL; }
  if (mdlog) { delete mdlog; mdlog = NULL; }
  if (balancer) { delete balancer; balancer = NULL; }

  if (logger) { delete logger; logger = 0; }

  if (messenger) { delete messenger; messenger = NULL; }

  if (idalloc) { delete idalloc; idalloc = NULL; }
}


int MDS::init()
{
}


int MDS::shutdown_start()
{
  dout(1) << "shutdown_start" << endl;
  for (int i=0; i<mdcluster->get_num_mds(); i++) {
	if (i == whoami) continue;
	dout(1) << "sending MShutdownStart to mds" << i << endl;
	messenger->send_message(new MGenericMessage(MSG_MDS_SHUTDOWNSTART),
							i, MDS_PORT_MAIN,
							MDS_PORT_MAIN);
  }

  if (idalloc) idalloc->shutdown();
  
  handle_shutdown_start(NULL);
}


void MDS::handle_shutdown_start(Message *m)
{
  dout(1) << " handle_shutdown_start" << endl;

  // set flag
  shutting_down = true;

  mdcache->shutdown_start();
  
  // save anchor table
  anchormgr->save(0);  // FIXME FIXME

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
	// MDS's all shut down!

	// shut down osd's
	for (int i=0; i<g_conf.num_osd; i++) {
	  messenger->send_message(new MGenericMessage(MSG_SHUTDOWN),
							  MSG_ADDR_OSD(i), 0, 0);
	}

	// shut myself down.
	shutting_down = false;

	shutdown_final();
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
  timepair_t now = g_clock.gettimepair();
  mds_load_t l;
  if (mdcache->get_root()) {
	l.root_pop = mdcache->get_root()->popularity[MDS_POP_ANYDOM].get(now);
  } else
	l.root_pop = 0;
  l.req_rate = stat_req.get(now);
  l.rd_rate = stat_read.get(now);
  l.wr_rate = stat_write.get(now);
  return l;
}


class C_MDS_Unpause : public Context {
public:
  MDS *mds;
  C_MDS_Unpause(MDS *mds) {
	this->mds = mds;
  }
  void finish(int r) {
	mds->mds_paused = false;
	mds->queue_finished(mds->waiting_for_unpause);
  }
};


void MDS::proc_message(Message *m) 
{
  switch (m->get_type()) {
	// OSD ===============
  case MSG_OSD_OPREPLY:
	filer->handle_osd_op_reply((class MOSDOpReply*)m);
	return;

  case MSG_OSD_GETCLUSTER:
	handle_osd_getcluster(m);
	return;

	// MDS
  case MSG_MDS_SHUTDOWNSTART:
	handle_shutdown_start(m);
	return;

  case MSG_MDS_SHUTDOWNFINISH:
	handle_shutdown_finish(m);
	return;

  case MSG_PING:
	handle_ping((MPing*)m);
	return;
  }


  // paused?
  if (mds_paused) {
	dout(3) << "paused" << endl;
	waiting_for_unpause.push_back(new C_MDS_RetryMessage(this, m));
	return;
  }


  switch (m->get_type()) {
  case MSG_CLIENT_MOUNT:
	handle_client_mount((MClientMount*)m);
	return;
  case MSG_CLIENT_UNMOUNT:
	handle_client_unmount(m);
	return;
  }
 

  // need idalloc
  if (!idalloc->is_open()) {
	dout(3) << "idalloc not open yet" << endl;

	mds_paused = true;
	idalloc->load(new C_MDS_Unpause(this));
	
	// defer
	waiting_for_unpause.push_back(new C_MDS_RetryMessage(this, m));
	return;
  }


  switch (m->get_type()) {
  case MSG_CLIENT_REQUEST:
	handle_client_request((MClientRequest*)m);
	return;
  }

  dout(1) << " main unknown message " << m->get_type() << endl;
  assert(0);
}


void MDS::dispatch(Message *m)
{
  mds_lock.Lock();

  my_dispatch(m);

  mds_lock.Unlock();
}

void MDS::my_dispatch(Message *m)
{

  switch (m->get_dest_port()) {
	
  case MDS_PORT_STORE:
	mdstore->proc_message(m);
	break;

  case MDS_PORT_ANCHORMGR:
	anchormgr->proc_message(m);
	break;
	
  case MDS_PORT_OSDMON:
	osdmonitor->proc_message(m);
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
	dout(1) << "MDS dispatch unknown message port" << m->get_dest_port() << endl;
	assert(0);
  }


  // HACK FOR NOW
  /*
  static bool did_heartbeat_hack = false;
  if (!shutting_down && !shut_down &&
	  false && 
	  !did_heartbeat_hack) {
	osdmonitor->initiate_heartbeat();
	did_heartbeat_hack = true;
  }
  */
	

  // finish any triggered contexts
  if (finished_queue.size()) {
	dout(7) << "mds has " << finished_queue.size() << " queued contexts" << endl;
	list<Context*> ls;
	ls.splice(ls.begin(), finished_queue);
	assert(finished_queue.empty());
	finish_contexts(ls);
  }

  // balance?
  if (true && whoami == 0 &&
	  stat_ops >= last_heartbeat + g_conf.mds_bal_interval) {
	last_heartbeat = stat_ops;
	balancer->send_heartbeat();
  }

  if (whoami == 0) {
	static bool didit = false;
	
	// 7 to 1
	CInode *in = mdcache->get_inode(1001);
	if (in && in->is_dir() && !didit) {
	  CDir *dir = in->get_or_open_dir(this);
	  if (dir->is_auth()) {
		dout(1) << "FORCING EXPORT" << endl;
		mdcache->export_dir(dir,1);
		didit = true;
	  }
	}
  }

  // shut down?
  if (shutting_down && !shut_down) {
	if (mdcache->shutdown_pass()) {
	  shutting_down = false;
	  shut_down = true;
	  if (whoami) shutdown_final();
	}
  }

}


void MDS::handle_osd_getcluster(Message *m)
{
  dout(7) << "osd_getcluster from " << MSG_ADDR_NICE(m->get_source()) << endl;
  
  messenger->send_message(new MOSDGetClusterAck(osdcluster),
						  m->get_source());
  delete m;
}


void MDS::handle_client_mount(MClientMount *m)
{
  // mkfs?  (sorta hack!)
  if (int cmd = m->get_mkfs()) {
	dout(3) << "MKFS flag is set" << endl;
	if (mdcache->get_root()) {
	  dout(3) << "   root inode is already open" << endl;
	} else {
	  dout(3) << "   root inode isn't open yet, inventing a fresh filesystem" << endl;
	  
	  mdcache->open_root(0);
	  CInode *root = mdcache->get_root();
	  assert(root);

	  // force empty root dir
	  CDir *dir = root->dir;
	  dir->mark_complete();
	  dir->mark_dirty();

	  if (cmd == MDS_MKFS_FULL) {
		// wipe osds too
		dout(3) << "wiping osds too" << endl;
		mds_paused = true;
		filer->mkfs(new C_MDS_Unpause(this));
	  	waiting_for_unpause.push_back(new C_MDS_RetryMessage(this, m));
	  	return;
	  } else {
		// fake out idalloc (reset, pretend loaded)
		idalloc->reset();
	  }
	  
	}
  }

  int n = MSG_ADDR_NUM(m->get_source());
  dout(3) << "mount by client" << n << endl;
  mounted_clients.insert(n);

  assert(whoami == 0);  // mds0 mounts/unmounts



  // ack
  messenger->send_message(new MClientMountAck(m, osdcluster), 
						  m->get_source(), m->get_source_port());
  delete m;
}

void MDS::handle_client_unmount(Message *m)
{
  int n = MSG_ADDR_NUM(m->get_source());
  dout(3) << "unmount by client" << n << endl;

  assert(whoami == 0);  // mds0 mounts/unmounts

  assert(mounted_clients.count(n));
  mounted_clients.erase(n);

  if (mounted_clients.empty()) {
	dout(3) << "all clients done, initiating shutdown" << endl;
	shutdown_start();
  }

  // ack by sending back to client
  messenger->send_message(m, m->get_source(), m->get_source_port());
}


void MDS::handle_ping(MPing *m)
{
  dout(10) << " received ping from " << MSG_ADDR_NICE(m->get_source()) << " with seq " << m->seq << endl;

  messenger->send_message(new MPingAck(m),
						  m->get_source(), m->get_source_port(),
						  MDS_PORT_MAIN);
  
  delete m;
}


/*******
 * some generic stuff for finishing off requests
 */

/** C_MDS_CommitRequest
 */

class C_MDS_CommitRequest : public Context {
  MDS *mds;
  MClientRequest *req;
  MClientReply *reply;
  CInode *tracei;    // inode to include a trace for
  bool pinned;
  LogEvent *event;
public:
  C_MDS_CommitRequest(MDS *mds, 
					  MClientRequest *req, MClientReply *reply, CInode *tracei,
					  LogEvent *event = 0,
					  bool pinned=false) {
	this->mds = mds;
	this->req = req;
	this->tracei = tracei;
	this->pinned = pinned;
	this->reply = reply;
	this->event = event;
  }
  void finish(int r) {
	if (r == 0) {
	  // success.  log and reply.
	  mds->commit_request(req, reply, tracei, event);
	} else {
	  // failure.  set failure code and reply.
	  reply->set_result(r);
	  mds->reply_request(req, reply, tracei);
	}
  }
};

/*
 * send generic response (just and error code)
 */
void MDS::reply_request(MClientRequest *req, int r, CInode *tracei)
{
  reply_request(req, new MClientReply(req, r), tracei);
}

/*
 * send given reply
 * include a trace to tracei
 */
void MDS::reply_request(MClientRequest *req, MClientReply *reply, CInode *tracei) {
  dout(10) << "reply_request r=" << reply->get_result() << " " << *req << endl;

  // include trace
  if (tracei)
	reply->set_trace_dist( tracei, whoami );
  
  // send reply
  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->get_client()), 0,
						  MDS_PORT_SERVER);

  // discard request
  mdcache->request_finish(req);

  // stupid stats crap (FIXME)
  stat_ops++;
}

/* 
 * commit event(s) to the metadata journal, then reply.
 * or, be sloppy and do it concurrently (see g_conf.mds_log_before_reply)
 */
void MDS::commit_request(MClientRequest *req,
						 MClientReply *reply,
						 CInode *tracei,
						 LogEvent *event,
						 LogEvent *event2) 
{	  
  if (g_conf.mds_log_before_reply) {
	// SAFE mode!
	
	if (event) {
	  // log, then reply

	  // pin inode so it doesn't go away!
	  if (tracei) mdcache->request_pin_inode(req, tracei);
	  
	  // pass event2 as event1 (so we chain together!)
	  /*
		WARNING: by chaining back to CommitRequest we may get
		something not quite right if the log commit fails.  what 
		happens (to the whole system!) then?   ** FIXME **
	  */
	  dout(10) << "commit_request submitting log entry" << endl;
	  mdlog->submit_entry(event, 
						  new C_MDS_CommitRequest(this, req, reply, tracei, event2, true));  // inode is pinned
	}
	else {
	  // just reply, no log entry (anymore).
	  reply_request(req, reply, tracei);
	}
  } else {
	// SLOPPY mode!

	// log
	if (event) mdlog->submit_entry(event, NULL);
	if (event2) mdlog->submit_entry(event2, NULL);

	// reply
	reply_request(req, reply, tracei);
  }
}



/***
 * process a client request
 */

void MDS::handle_client_request(MClientRequest *req)
{
  dout(4) << "req " << *req << endl;

  if (is_shutting_down()) {
	dout(5) << " shutting down, discarding client request." << endl;
	delete req;
	return;
  }
  
  if (!mdcache->get_root()) {
	dout(5) << "need to open root" << endl;
	mdcache->open_root(new C_MDS_RetryMessage(this, req));
	return;
  }

  // okay, i want
  CInode           *ref = 0;
  vector<CDentry*> trace;      // might be blank, for fh guys

  bool follow_trailing_symlink = false;

  // operations on fh's or other non-files
  switch (req->get_op()) {
	/*
  case MDS_OP_FSTAT:
	reply = handle_client_fstat(req, cur);
	break; ****** fiX ME ***
	*/
	
  case MDS_OP_TRUNCATE:
	if (!req->get_iarg()) break;   // can be called w/ either fh OR path
	
  case MDS_OP_CLOSE:
  case MDS_OP_FSYNC:
	ref = mdcache->get_inode(req->get_ino());   // fixme someday no ino needed?

	if (!ref) {
	  int next = whoami + 1;
	  if (next >= mdcluster->get_num_mds()) next = 0;
	  dout(10) << "got request on ino we don't have, passing buck to " << next << endl;
	  messenger->send_message(req, 
							  MSG_ADDR_MDS(next), MDS_PORT_SERVER, MDS_PORT_SERVER);
	  return;
	}
  }

  if (!ref) {
	// we need to traverse a path
	filepath refpath = req->get_filepath();
	
	// ops on non-existing files --> directory paths
	switch (req->get_op()) {
	case MDS_OP_OPEN:
	  if (!(req->get_iarg() & O_CREAT)) break;
	  
	case MDS_OP_MKNOD:
	case MDS_OP_MKDIR:
	case MDS_OP_SYMLINK:
	case MDS_OP_TRUNCATE:
	case MDS_OP_LINK:
	case MDS_OP_UNLINK:   // also wrt parent dir, NOT the unlinked inode!!
	case MDS_OP_RMDIR:
	case MDS_OP_RENAME:
	  // remove last bit of path
	  refpath = refpath.prefixpath(refpath.depth()-1);
	  break;
	}
	dout(10) << "refpath = " << refpath << endl;
	
	Context *ondelay = new C_MDS_RetryMessage(this, req);
	
	if (req->get_op() == MDS_OP_LSTAT) {
	  follow_trailing_symlink = false;
	}

	// do trace
	int r = mdcache->path_traverse(refpath, trace, follow_trailing_symlink,
								   req, ondelay,
								   MDS_TRAVERSE_FORWARD);
	
	if (r > 0) return; // delayed
	if (r == -ENOENT ||
		r == -ENOTDIR ||
		r == -EISDIR) {
	  // error! 
	  dout(10) << " path traverse error " << r << ", replying" << endl;
	  
	  // send error
	  messenger->send_message(new MClientReply(req, r),
							  MSG_ADDR_CLIENT(req->get_client()), 0,
							  MDS_PORT_SERVER);
	  delete req;
	  return;
	}
	
	if (trace.size()) 
	  ref = trace[trace.size()-1]->inode;
	else
	  ref = mdcache->get_root();
  }
  
  dout(10) << "ref is " << *ref << endl;
  
  // rename doesn't pin src path (initially)
  if (req->get_op() == MDS_OP_RENAME) trace.clear();

  // register
  if (!mdcache->request_start(req, ref, trace))
	return;
  
  // process
  dispatch_request(req, ref);
}



void MDS::dispatch_request(Message *m, CInode *ref)
{
  MClientRequest *req = 0;

  // MLock or MClientRequest?
  /* this is a little weird.
	 client requests and mlocks both initial dentry xlocks, path pins, etc.,
	 and thus both make use of the context C_MDS_RetryRequest.
  */
  switch (m->get_type()) {
  case MSG_CLIENT_REQUEST:
	req = (MClientRequest*)m;
	break; // continue below!

  case MSG_MDS_LOCK:
	mdcache->handle_lock_dn((MLock*)m);
	return; // done

  default:
	assert(0);  // shouldn't get here
  }

  // MClientRequest.

  switch(req->get_op()) {
	
	// files
  case MDS_OP_OPEN:
	if (req->get_iarg() & O_CREAT) 
	  handle_client_openc(req, ref);
	else 
	  handle_client_open(req, ref);
	break;
	/*
  case MDS_OP_TRUNCATE:
	handle_client_truncate(req, ref);
	break;
  case MDS_OP_FSYNC:
	handle_client_fsync(req, ref);
	break;
	*/
  case MDS_OP_CLOSE:
	handle_client_close(req, ref);
	break;

	// inodes
  case MDS_OP_STAT:
  case MDS_OP_LSTAT:
	handle_client_stat(req, ref);
	break;
  case MDS_OP_UTIME:
	handle_client_utime(req, ref);
	break;
  case MDS_OP_CHMOD:
	handle_client_chmod(req, ref);
	break;
  case MDS_OP_CHOWN:
	handle_client_chown(req, ref);
	break;

	// namespace
  case MDS_OP_READDIR:
	handle_client_readdir(req, ref);
	break;
  case MDS_OP_MKNOD:
	handle_client_mknod(req, ref);
	break;
  case MDS_OP_LINK:
	handle_client_link(req, ref);
	break;
  case MDS_OP_UNLINK:
	handle_client_unlink(req, ref);
	break;
  case MDS_OP_RENAME:
	handle_client_rename(req, ref);
	break;
  case MDS_OP_RMDIR:
	handle_client_unlink(req, ref);
	break;
  case MDS_OP_MKDIR:
	handle_client_mkdir(req, ref);
	break;
  case MDS_OP_SYMLINK:
	handle_client_symlink(req, ref);
	break;



  default:
	dout(1) << " unknown client op " << req->get_op() << endl;
	assert(0);
  }

  return;
}




// STAT

void MDS::handle_client_stat(MClientRequest *req,
							 CInode *ref)
{
  if (!mdcache->inode_soft_read_start(ref, req))
	return;  // sync

  dout(10) << "reply to " << *req << " stat " << ref->inode.mtime << endl;
  MClientReply *reply = new MClientReply(req);

  // inode info is in the trace

  mdcache->inode_soft_read_finish(ref);

  timepair_t now = g_clock.gettimepair();
  stat_read.hit(now);
  stat_req.hit(now);

  balancer->hit_inode(ref);   

  // reply
  reply_request(req, reply, ref);
}



// INODE UPDATES

// utime

void MDS::handle_client_utime(MClientRequest *req,
							  CInode *cur)
{
  // write
  if (!mdcache->inode_soft_write_start(cur, req))
	return;  // fw or (wait for) sync

  // do update
  time_t mtime = req->get_targ();
  time_t atime = req->get_targ2();
  cur->inode.mtime = mtime;
  cur->inode.atime = mtime;
  if (cur->is_auth())
	cur->mark_dirty();

  mdcache->inode_soft_write_finish(cur);
  
  balancer->hit_inode(cur);   

  // init reply
  MClientReply *reply = new MClientReply(req, 0);
  reply->set_result(0);

  // commit
  commit_request(req, reply, cur,
				 new EInodeUpdate(cur));
}

						   

// HARD

// chmod

void MDS::handle_client_chmod(MClientRequest *req,
							  CInode *cur)
{
  // write
  if (!mdcache->inode_hard_write_start(cur, req))
	return;  // fw or (wait for) lock

 
  // check permissions
  
  // do update
  int mode = req->get_iarg();
  cur->inode.mode &= ~04777;
  cur->inode.mode |= (mode & 04777);
  cur->mark_dirty();

  mdcache->inode_hard_write_finish(cur);

  balancer->hit_inode(cur);   

  // start reply
  MClientReply *reply = new MClientReply(req, 0);

  // commit
  commit_request(req, reply, cur,
				 new EInodeUpdate(cur));
}

// chown

void MDS::handle_client_chown(MClientRequest *req,
							  CInode *cur)
{
  // write
  if (!mdcache->inode_hard_write_start(cur, req))
	return;  // fw or (wait for) lock

  // check permissions

  // do update
  int uid = req->get_iarg();
  int gid = req->get_iarg2();
  cur->inode.uid = uid;
  cur->inode.gid = gid;
  cur->mark_dirty();

  mdcache->inode_hard_write_finish(cur);

  balancer->hit_inode(cur);   

  // start reply
  MClientReply *reply = new MClientReply(req, 0);

  // commit
  commit_request(req, reply, cur,
				 new EInodeUpdate(cur));
}





// DIRECTORY and NAMESPACE OPS


void MDS::handle_client_readdir(MClientRequest *req,
								CInode *cur)
{
  // it's a directory, right?
  if (!cur->is_dir()) {
	// not a dir
	dout(10) << "reply to " << *req << " readdir -ENOTDIR" << endl;
	reply_request(req, -ENOTDIR);
	return;
  }


  // auth?
  if (!cur->dir_is_auth()) {
	int dirauth = cur->authority();
	if (cur->dir)
	  dirauth = cur->dir->authority();
	assert(dirauth >= 0);
	assert(dirauth != whoami);
	
	// forward to authority
	dout(10) << " forwarding readdir to authority " << dirauth << endl;
	mdcache->request_forward(req, dirauth);
	return;
  }
  
  cur->get_or_open_dir(this);
  assert(cur->dir->is_auth());

  // frozen?
  if (cur->dir->is_frozen()) {
	// doh!
	dout(10) << " dir is frozen, waiting" << endl;
	cur->dir->add_waiter(CDIR_WAIT_UNFREEZE,
						 new C_MDS_RetryRequest(this, req, cur));
	return;
  }

  // check perm
  if (!mdcache->inode_hard_read_start(cur,req))
	return;
  mdcache->inode_hard_read_finish(cur);


  if (!cur->dir->is_complete()) {
	// fetch
	dout(10) << " incomplete dir contents for readdir on " << *cur->dir << ", fetching" << endl;
	mdstore->fetch_dir(cur->dir, new C_MDS_RetryRequest(this, req, cur));
	return;
  }
  
  // yay, reply
  MClientReply *reply = new MClientReply(req);
  
  // build dir contents
  CDir_map_t::iterator it;
  int numfiles = 0;
  for (it = cur->dir->begin(); it != cur->dir->end(); it++) {
	CDentry *dn = it->second;
	
	// is dentry readable?
	if (dn->is_xlocked()) {
	  // ***** FIXME *****
	  dout(10) << "warning, returning xlocked dentry, we are technically WRONG" << endl;
	}
	
	CInode *in = dn->inode;
	if (!in) continue;  // null dentry?

	dout(12) << "including inode " << *in << endl;

	// add this item
	// note: c_inode_info makes note of whether inode data is readable.
	reply->add_dir_item(new c_inode_info(in, whoami, it->first));
	numfiles++;
  }
  
  dout(10) << "reply to " << *req << " readdir " << numfiles << " files" << endl;
  reply->set_result(0);
  
  timepair_t now = g_clock.gettimepair();
  stat_read.hit(now);
  stat_req.hit(now);

  balancer->hit_dir(cur->dir);

  // reply
  reply_request(req, reply, cur);
}


// MKNOD

void MDS::handle_client_mknod(MClientRequest *req, CInode *ref)
{
  // make dentry and inode, link.  
  CInode *newi = mknod(req, ref);
  if (!newi) return;

  // it's a file!
  newi->inode.mode = req->get_iarg();
  newi->inode.mode &= ~INODE_TYPE_MASK;
  newi->inode.mode |= INODE_MODE_FILE;
  
  balancer->hit_inode(newi);

  // commit
  commit_request(req, new MClientReply(req, 0), ref,
				 new EInodeUpdate(newi));  // FIXME this is the wrong message
}

// mknod(): used by handle_client_mkdir, handle_client_mknod, which are mostly identical.

CInode *MDS::mknod(MClientRequest *req, CInode *diri, bool okexist) 
{
  dout(10) << "mknod " << req->get_filepath() << " in " << *diri << endl;

  // get containing directory (without last bit)
  filepath dirpath = req->get_filepath().prefixpath(req->get_filepath().depth() - 1);
  string name = req->get_filepath().last_bit();
  
  // did we get to parent?
  dout(10) << "dirpath is " << dirpath << " depth " << dirpath.depth() << endl;

  // make sure parent is a dir?
  if (!diri->is_dir()) {
	dout(7) << "not a dir" << endl;
	reply_request(req, -ENOTDIR);
	return 0;
  }

  // am i not open, not auth?
  if (!diri->dir && !diri->is_auth()) {
	int dirauth = diri->authority();
	dout(7) << "don't know dir auth, not open, auth is i think " << dirauth << endl;
	mdcache->request_forward(req, dirauth);
	return 0;
  }
  
  CDir *dir = diri->get_or_open_dir(this);
  
  // make sure it's my dentry
  int dnauth = dir->dentry_authority(name);  
  if (dnauth != get_nodeid()) {
	// fw
	
	dout(7) << "mknod on " << req->get_path() << ", dentry " << *dir << " dn " << name << " not mine, fw to " << dnauth << endl;
	mdcache->request_forward(req, dnauth);
	return 0;
  }
  // ok, done passing buck.


  // make sure name doesn't already exist
  CDentry *dn = dir->lookup(name);
  if (dn) {
	if (!dn->can_read(req)) {
	  dout(10) << "waiting on (existing!) dentry " << *dn << endl;
	  dir->add_waiter(CDIR_WAIT_DNREAD, name, new C_MDS_RetryRequest(this, req, diri));
	  return 0;
	}

	if (!dn->is_null()) {
	  // name already exists
	  if (okexist) {
		dout(10) << "dentry " << name << " exists in " << *dir << endl;
		return dn->inode;
	  } else {
		dout(10) << "dentry " << name << " exists in " << *dir << endl;
		reply_request(req, -EEXIST);
		return 0;
	  }
	}
  }

  // make sure dir is complete
  if (!dir->is_complete()) {
	dout(7) << " incomplete dir contents for " << *dir << ", fetching" << endl;
	mdstore->fetch_dir(dir, new C_MDS_RetryRequest(this, req, diri));
	return 0;
  }

  // create!
  CInode *newi = mdcache->create_inode();
  newi->inode.uid = req->get_caller_uid();
  newi->inode.gid = req->get_caller_gid();
  newi->inode.ctime = newi->inode.mtime = newi->inode.atime = g_clock.gettime();   // now

  // link
  if (!dn) 
	dn = dir->add_dentry(name, newi);
  else
	dir->link_inode(dn, newi);
  
  // mark dirty
  dn->mark_dirty();
  newi->mark_dirty();
  
  // ok!
  return newi;
}


// LINK

class C_MDS_LinkTraverse : public Context {
  MDS *mds;
  MClientRequest *req;
  CInode *ref;
public:
  vector<CDentry*> trace;
  C_MDS_LinkTraverse(MDS *mds, MClientRequest *req, CInode *ref) {
	this->mds = mds;
	this->req = req;
	this->ref = ref;
  }
  void finish(int r) {
	mds->handle_client_link_2(r, req, ref, trace);
  }
};

void MDS::handle_client_link(MClientRequest *req, CInode *ref)
{
  // figure out name
  string dname = req->get_filepath().last_bit();
  dout(7) << "dname is " << dname << endl;
  
  // make sure parent is a dir?
  if (!ref->is_dir()) {
	dout(7) << "not a dir " << *ref << endl;
	reply_request(req, -EINVAL);
	return;
  }

  // am i not open, not auth?
  if (!ref->dir && !ref->is_auth()) {
	int dirauth = ref->authority();
	dout(7) << "don't know dir auth, not open, srcdir auth is probably " << dirauth << endl;
	mdcache->request_forward(req, dirauth);
	return;
  }
  
  CDir *dir = ref->get_or_open_dir(this);
  dout(7) << "handle_client_link dir is " << *dir << endl;
  
  // make sure it's my dentry
  int dauth = dir->dentry_authority(dname);  
  if (dauth != get_nodeid()) {
	// fw
	dout(7) << "link on " << req->get_path() << ", dn " << dname << " in " << *dir << " not mine, fw to " << dauth << endl;
	mdcache->request_forward(req, dauth);
	return;
  }
  // ok, done passing buck.
  

  // exists?
  CDentry *dn = dir->lookup(dname);
  if (dn && (!dn->is_null() || dn->is_xlockedbyother(req))) {
	dout(7) << "handle_client_link dn exists " << *dn << endl;
	reply_request(req, -EEXIST);
	return;
  }

  // keep src dir in memory
  mdcache->request_pin_dir(req, dir);

  // discover link target
  filepath target = req->get_sarg();

  dout(7) << "handle_client_link discovering target " << target << endl;

  C_MDS_LinkTraverse *onfinish = new C_MDS_LinkTraverse(this, req, ref);
  Context *ondelay = new C_MDS_RetryRequest(this, req, ref);
  
  mdcache->path_traverse(target, onfinish->trace, false,
						 req, ondelay,
						 MDS_TRAVERSE_DISCOVER,  //XLOCK, 
						 onfinish);
}


class C_MDS_RemoteLink : public Context {
  MDS *mds;
  MClientRequest *req;
  CInode *ref;
  CDentry *dn;
  CInode *targeti;
public:
  C_MDS_RemoteLink(MDS *mds, MClientRequest *req, CInode *ref, CDentry *dn, CInode *targeti) {
	this->mds = mds;
	this->req = req;
	this->ref = ref;
	this->dn = dn;
	this->targeti = targeti;
  }
  void finish(int r) {
	if (r > 0) { // success
	  // yay
	  mds->handle_client_link_finish(req, ref, dn, targeti);
	} 
	else if (r == 0) {
	  // huh?  retry!
	  assert(0);
	  mds->dispatch_request(req, ref);	  
	} else {
	  // link failed
	  mds->reply_request(req, r);
	}
  }
};

void MDS::handle_client_link_2(int r, MClientRequest *req, CInode *ref, vector<CDentry*>& trace)
{
  // target dne?
  if (r < 0) {
	dout(7) << "target " << req->get_sarg() << " dne" << endl;
	reply_request(req, r);
	return;
  }
  assert(r == 0);

  CInode *targeti = mdcache->get_root();
  if (trace.size()) targeti = trace[trace.size()-1]->inode;
  assert(targeti);

  // dir?
  dout(7) << "target is " << *targeti << endl;
  if (targeti->is_dir()) {
	dout(7) << "target is a dir, failing" << endl;
	reply_request(req, -EINVAL);
	return;
  }
  
  // keep target inode in memory
  mdcache->request_pin_inode(req, targeti);

  dout(7) << "dir is " << *ref << endl;

  // xlock the dentry
  CDir *dir = ref->dir;
  assert(dir);
  
  string dname = req->get_filepath().last_bit();
  int dauth = dir->dentry_authority(dname);
  if (get_nodeid() != dauth) {
	// ugh, exported out from under us
	dout(7) << "ugh, forwarded out from under us, dentry auth is " << dauth << endl;
	mdcache->request_forward(req, dauth);
	return;
  }
  
  CDentry *dn = dir->lookup(dname);
  if (dn && (!dn->is_null() || dn->is_xlockedbyother(req))) {
	dout(7) << "handle_client_link dn exists " << *dn << endl;
	reply_request(req, -EEXIST);
	return;
  }

  if (!dn) dn = dir->add_dentry(dname);
  
  if (!dn->is_xlockedbyme(req)) {
	if (!mdcache->dentry_xlock_start(dn, req, ref)) {
	  if (dn->is_clean() && dn->is_null() && dn->is_sync()) dir->remove_dentry(dn);
	  return;
	}
  }

  
  // ok xlocked!
  if (targeti->is_auth()) {
	// mine
	if (targeti->is_anchored()) {
	  dout(7) << "target anchored already (nlink=" << targeti->inode.nlink << "), sweet" << endl;
	} else {
	  assert(targeti->inode.nlink == 1);
	  dout(7) << "target needs anchor, nlink=" << targeti->inode.nlink << ", creating anchor" << endl;
	  
	  mdcache->anchor_inode(targeti,
							new C_MDS_RetryRequest(this, req, ref));
	  return;
	}

	// ok, inc link!
	targeti->inode.nlink++;
	dout(7) << "nlink++, now " << targeti->inode.nlink << " on " << *targeti << endl;
	targeti->mark_dirty();
	
  } else {
	// remote: send nlink++ request, wait
	dout(7) << "target is remote, sending InodeLink" << endl;
	messenger->send_message(new MInodeLink(targeti->ino(), whoami),
							MSG_ADDR_MDS(targeti->authority()), MDS_PORT_CACHE, MDS_PORT_CACHE);
	
	// wait
	targeti->add_waiter(CINODE_WAIT_LINK,
						new C_MDS_RemoteLink(this, req, ref, dn, targeti));
	return;
  }

  handle_client_link_finish(req, ref, dn, targeti);
}

void MDS::handle_client_link_finish(MClientRequest *req, CInode *ref,
									CDentry *dn, CInode *targeti)
{
  // create remote link
  dn->dir->link_inode(dn, targeti->ino());
  dn->link_remote( targeti );   // since we have it
  dn->mark_dirty();
  
  balancer->hit_dir(dn->dir);

  // done!
  commit_request(req, new MClientReply(req, 0), ref,
				 0);          // FIXME i should log something
}


// UNLINK

void MDS::handle_client_unlink(MClientRequest *req, 
							   CInode *diri)
{
  // rmdir or unlink
  bool rmdir = false;
  if (req->get_op() == MDS_OP_RMDIR) rmdir = true;
  
  // find it
  if (req->get_filepath().depth() == 0) {
	dout(7) << "can't rmdir root" << endl;
	reply_request(req, -EINVAL);
	return;
  }
  string name = req->get_filepath().last_bit();
  
  // make sure parent is a dir?
  if (!diri->is_dir()) {
	dout(7) << "not a dir" << endl;
	reply_request(req, -ENOTDIR);
	return;
  }

  // am i not open, not auth?
  if (!diri->dir && !diri->is_auth()) {
	int dirauth = diri->authority();
	dout(7) << "don't know dir auth, not open, auth is i think " << dirauth << endl;
	mdcache->request_forward(req, dirauth);
	return;
  }
  
  CDir *dir = diri->get_or_open_dir(this);
  int dnauth = dir->dentry_authority(name);  

  // does it exist?
  CDentry *dn = dir->lookup(name);
  if (!dn) {
	if (dnauth == whoami) {
	  dout(7) << "handle_client_rmdir/unlink dne " << name << " in " << *dir << endl;
	  reply_request(req, -ENOENT);
	} else {
	  // send to authority!
	  dout(7) << "handle_client_rmdir/unlink fw, don't have " << name << " in " << *dir << endl;
	  mdcache->request_forward(req, dnauth);
	}
	return;
  }

  // have it.  locked?
  if (!dn->can_read(req)) {
	dout(10) << " waiting on " << *dn << endl;
	dir->add_waiter(CDIR_WAIT_DNREAD,
					name,
					new C_MDS_RetryRequest(this, req, diri));
	return;
  }

  // null?
  if (dn->is_null()) {
	dout(10) << "unlink on null dn " << *dn << endl;
	reply_request(req, -ENOENT);
	return;
  }

  // ok!
  CInode *in = dn->inode;
  assert(in);
  if (rmdir) {
	dout(7) << "handle_client_rmdir on dir " << *in << endl;
  } else {
	dout(7) << "handle_client_unlink on non-dir " << *in << endl;
  }

  // dir stuff 
  if (in->is_dir()) {
	if (rmdir) {
	  // rmdir
	  
	  // open dir?
	  if (in->is_auth() && !in->dir) in->get_or_open_dir(this);

	  // not dir auth?  (or not open, which implies the same!)
	  if (!in->dir) {
		dout(7) << "handle_client_rmdir dir not open for " << *in << ", sending to dn auth " << dnauth << endl;
		mdcache->request_forward(req, dnauth);
		return;
	  }
	  if (!in->dir->is_auth()) {
		int dirauth = in->dir->authority();
		dout(7) << "handle_client_rmdir not auth for dir " << *in->dir << ", sending to dir auth " << dnauth << endl;
		mdcache->request_forward(req, dirauth);
		return;
	  }

	  assert(in->dir);
	  assert(in->dir->is_auth());

	  // dir size check on dir auth (but not necessarily dentry auth)?

	  // should be empty
	  if (in->dir->get_size() == 0 && !in->dir->is_complete()) {
		dout(7) << "handle_client_rmdir on dir " << *in->dir << ", empty but not complete, fetching" << endl;
		mdstore->fetch_dir(in->dir, 
						   new C_MDS_RetryRequest(this, req, diri));
		return;
	  }
	  if (in->dir->get_size() > 0) {
		dout(7) << "handle_client_rmdir on dir " << *in->dir << ", not empty" << endl;
		reply_request(req, -ENOTEMPTY);
		return;
	  }
		
	  dout(7) << "handle_client_rmdir dir is empty!" << endl;

	  // export sanity check
	  if (!in->is_auth()) {
		// i should be exporting this now/soon, since the dir is empty.
		dout(7) << "handle_client_rmdir dir is auth, but not inode." << endl;
		if (!in->dir->is_freezing() && in->dir->is_frozen()) {
		  assert(in->dir->is_import());
		  mdcache->export_empty_import(in->dir);		  
		} else {
		  dout(7) << "apparently already exporting" << endl;
		}
		in->dir->add_waiter(CDIR_WAIT_UNFREEZE,
							new C_MDS_RetryRequest(this, req, diri));
		return;
	  }

	} else {
	  // unlink
	  dout(7) << "handle_client_unlink on dir " << *in << ", returning error" << endl;
	  reply_request(req, -EISDIR);
	  return;
	}
  } else {
	if (rmdir) {
	  // unlink
	  dout(7) << "handle_client_rmdir on non-dir " << *in << ", returning error" << endl;
	  reply_request(req, -ENOTDIR);
	  return;
	}
  }

  // am i dentry auth?
  if (dnauth != get_nodeid()) {
	// not auth; forward!
	dout(7) << "handle_client_unlink not auth for " << *dir << " dn " << dn->name << ", fwd to " << dnauth << endl;
	mdcache->request_forward(req, dnauth);
	return;
  }
    
  dout(7) << "handle_client_unlink/rmdir on " << *in << endl;
  
  // xlock dentry
  if (!mdcache->dentry_xlock_start(dn, req, diri))
	return;

  // is this a remote link?
  if (dn->is_remote() && !dn->inode) {
	CInode *in = mdcache->get_inode(dn->get_remote_ino());
	if (in) {
	  dn->link_remote(in);
	} else {
	  // open inode
	  dout(7) << "opening target inode first, ino is " << dn->get_remote_ino() << endl;
	  mdcache->open_remote_ino(dn->get_remote_ino(), req, 
							   new C_MDS_RetryRequest(this, req, diri));
	  return;
	}
  }

	
  balancer->hit_dir(dn->dir);

  // it's locked, unlink!
  MClientReply *reply = new MClientReply(req,0);
  mdcache->dentry_unlink(dn,
						 new C_MDS_CommitRequest(this, req, reply, diri,
												 new EInodeUpdate(diri))); // FIXME WRONG EVENT
  return;
}






// RENAME

class C_MDS_RenameTraverseDst : public Context {
  MDS *mds;
  MClientRequest *req;
  CInode *ref;
  CInode *srcdiri;
  CDir *srcdir;
  CDentry *srcdn;
  filepath destpath;
public:
  vector<CDentry*> trace;
  
  C_MDS_RenameTraverseDst(MDS *mds, 
						  MClientRequest *req, 
						  CInode *ref,
						  CInode *srcdiri,
						  CDir *srcdir,
						  CDentry *srcdn,
						  filepath& destpath) {
	this->mds = mds;
	this->req = req;
	this->ref = ref;
	this->srcdiri = srcdiri;
	this->srcdir = srcdir;
	this->srcdn = srcdn;
	this->destpath = destpath;
  }
  void finish(int r) {
	mds->handle_client_rename_2(req, ref,
								srcdiri, srcdir, srcdn, destpath,
								trace, r);
  }
};


/*
  
  weirdness iwith rename:
    - ref inode is what was originally srcdiri, but that may change by the tiem
      the rename actually happens.  for all practical purpose, ref is useless except
      for C_MDS_RetryRequest

 */
void MDS::handle_client_rename(MClientRequest *req,
							   CInode *ref)
{
  dout(7) << "handle_client_rename on " << *req << endl;

  // sanity checks
  if (req->get_filepath().depth() == 0) {
	dout(7) << "can't rename root" << endl;
	reply_request(req, -EINVAL);
	return;
  }
  // mv a/b a/b/c  -- meaningless
  if (req->get_sarg().compare( 0, req->get_path().length(), req->get_path()) == 0 &&
	  req->get_sarg().c_str()[ req->get_path().length() ] == '/') {
	dout(7) << "can't rename to underneath myself" << endl;
	reply_request(req, -EINVAL);
	return;
  }

  // mv blah blah  -- also meaningless
  if (req->get_sarg() == req->get_path()) {
	dout(7) << "can't rename something to itself (or into itself)" << endl;
	reply_request(req, -EINVAL);
	return;
  }
  
  // traverse to source
  /*
	this is abnoraml, just for rename.  since we don't pin source path 
    (because we don't want to screw up the lock ordering) the ref inode 
	(normally/initially srcdiri) may move, and this may fail.
 -> so, re-traverse path.  and make sure we request_finish in the case of a forward!
   */
  filepath refpath = req->get_filepath();
  string srcname = refpath.last_bit();
  refpath = refpath.prefixpath(refpath.depth()-1);

  dout(7) << "handle_client_rename src traversing to srcdir " << refpath << endl;
  vector<CDentry*> trace;
  int r = mdcache->path_traverse(refpath, trace, true,
								 req, new C_MDS_RetryRequest(this, req, ref),
								 MDS_TRAVERSE_FORWARD);
  if (r == 2) {
	dout(7) << "path traverse forwarded, ending request, doing manual request_cleanup" << endl;
	dout(7) << "(pseudo) request_forward to 9999 req " << *req << endl;
	mdcache->request_cleanup(req);  // not _finish (deletes) or _forward (path_traverse did that)
	return;
  }
  if (r > 0) return;
  if (r < 0) {   // dne or something.  got renamed out from under us, probably!
	dout(7) << "traverse r=" << r << endl;
	reply_request(req, r);
	return;
  }
  
  CInode *srcdiri;
  if (trace.size()) 
	srcdiri = trace[trace.size()-1]->inode;
  else
	srcdiri = mdcache->get_root();

  dout(7) << "handle_client_rename srcdiri is " << *srcdiri << endl;

  dout(7) << "handle_client_rename srcname is " << srcname << endl;

  // make sure parent is a dir?
  if (!srcdiri->is_dir()) {
	dout(7) << "srcdiri not a dir " << *srcdiri << endl;
	reply_request(req, -EINVAL);
	return;
  }

  // am i not open, not auth?
  if (!srcdiri->dir && !srcdiri->is_auth()) {
	int dirauth = srcdiri->authority();
	dout(7) << "don't know dir auth, not open, srcdir auth is probably " << dirauth << endl;
	mdcache->request_forward(req, dirauth);
	return;
  }
  
  CDir *srcdir = srcdiri->get_or_open_dir(this);
  dout(7) << "handle_client_rename srcdir is " << *srcdir << endl;
  
  // make sure it's my dentry
  int srcauth = srcdir->dentry_authority(srcname);  
  if (srcauth != get_nodeid()) {
	// fw
	dout(7) << "rename on " << req->get_path() << ", dentry " << *srcdir << " dn " << srcname << " not mine, fw to " << srcauth << endl;
	mdcache->request_forward(req, srcauth);
	return;
  }
  // ok, done passing buck.

  // src dentry
  CDentry *srcdn = srcdir->lookup(srcname);

  // xlocked?
  if (srcdn && !srcdn->can_read(req)) {
	dout(10) << " waiting on " << *srcdn << endl;
	srcdir->add_waiter(CDIR_WAIT_DNREAD,
					   srcname,
					   new C_MDS_RetryRequest(this, req, srcdiri));
	return;
  }
  
  if ((srcdn && !srcdn->inode) ||
	  (!srcdn && srcdir->is_complete())) {
	dout(10) << "handle_client_rename src dne " << endl;
	reply_request(req, -EEXIST);
	return;
  }
  
  if (!srcdn && !srcdir->is_complete()) {
	dout(10) << "readding incomplete dir" << endl;
	mdstore->fetch_dir(srcdir,
					   new C_MDS_RetryRequest(this, req, srcdiri));
	return;
  }
  assert(srcdn && srcdn->inode);


  dout(10) << "handle_client_rename srcdn is " << *srcdn << endl;
  dout(10) << "handle_client_rename srci is " << *srcdn->inode << endl;

  // pin src in cache (so it won't expire)
  mdcache->request_pin_inode(req, srcdn->inode);
  
  // find the destination, normalize
  // discover, etc. on the way... just get it on the local node.
  filepath destpath = req->get_sarg();   

  C_MDS_RenameTraverseDst *onfinish = new C_MDS_RenameTraverseDst(this, req, ref, srcdiri, srcdir, srcdn, destpath);
  Context *ondelay = new C_MDS_RetryRequest(this, req, ref);
  
  /*
   * use DISCOVERXLOCK mode:
   *   the dest may not exist, and may be xlocked from a remote host
   *   we want to succeed if we find the xlocked dentry
   * ??
   */
  mdcache->path_traverse(destpath, onfinish->trace, false,
						 req, ondelay,
						 MDS_TRAVERSE_DISCOVER,  //XLOCK, 
						 onfinish);
}

void MDS::handle_client_rename_2(MClientRequest *req,
								 CInode *ref,
								 CInode *srcdiri,
								 CDir *srcdir,
								 CDentry *srcdn,
								 filepath& destpath,
								 vector<CDentry*>& trace,
								 int r)
{
  dout(7) << "handle_client_rename_2 on " << *req << endl;
  dout(12) << " r = " << r << " trace depth " << trace.size() << "  destpath depth " << destpath.depth() << endl;

  CInode *srci = srcdn->inode;
  assert(srci);
  CDir*  destdir = 0;
  string destname;
  bool result;
  
  // what is the dest?  (dir or file or complete filename)
  // note: trace includes root, destpath doesn't (include leading /)
  if (trace.size() && trace[trace.size()-1]->inode == 0) {
	dout(10) << "dropping null dentry from tail of trace" << endl;
	trace.pop_back();    // drop it!
  }
  
  CInode *d;
  if (trace.size()) 
	d = trace[trace.size()-1]->inode;
  else
	d = mdcache->get_root();
  assert(d);
  dout(10) << "handle_client_rename_2 traced to " << *d << ", trace size = " << trace.size() << ", destpath = " << destpath.depth() << endl;
  
  // make sure i can open the dir?
  if (d->is_dir() && !d->dir_is_auth() && !d->dir) {
	// discover it
	mdcache->open_remote_dir(d,
							 new C_MDS_RetryRequest(this, req, ref));
	return;
  }

  if (trace.size() == destpath.depth()) {
	if (d->is_dir()) {
	  // mv /some/thing /to/some/dir 
	  destdir = d->get_or_open_dir(this);         // /to/some/dir
	  destname = req->get_filepath().last_bit();  // thing
	  destpath.add_dentry(destname);
	} else {
	  // mv /some/thing /to/some/existing_filename
	  destdir = trace[trace.size()-1]->dir;       // /to/some
	  destname = destpath.last_bit();             // existing_filename
	}
  }
  else if (trace.size() == destpath.depth()-1) {
	if (d->is_dir()) {
	  // mv /some/thing /to/some/place_that_maybe_dne     (we might be replica)
	  destdir = d->get_or_open_dir(this);   // /to/some
	  destname = destpath.last_bit();       // place_that_MAYBE_dne
	} else {
	  dout(7) << "dest dne" << endl;
	  reply_request(req, -EINVAL);
	  return;
	}
  }
  else {
	assert(trace.size() < destpath.depth()-1);
	// check traverse return value
	if (r > 0) {
	  return;  // discover, readdir, etc.
	}

	// ??
	assert(r < 0 || trace.size() == 0);  // musta been an error

	// error out
	dout(7) << " rename dest " << destpath << " dne" << endl;
	reply_request(req, -EINVAL);
	return;
  }

  string srcpath = req->get_path();
  dout(10) << "handle_client_rename_2 srcpath " << srcpath << endl;
  dout(10) << "handle_client_rename_2 destpath " << destpath << endl;

  // src == dest?
  if (srcdn->get_dir() == destdir && srcdn->name == destname) {
	dout(7) << "rename src=dest, same file " << endl;
	reply_request(req, -EINVAL);
	return;
  }

  // does destination exist?  (is this an overwrite?)
  CDentry *destdn = destdir->lookup(destname);
  CInode  *oldin = 0;
  if (destdn) {
	oldin = destdn->get_inode();
	
	if (oldin) {
	  // make sure it's also a file!
	  // this can happen, e.g. "mv /some/thing /a/dir" where /a/dir/thing exists and is a dir.
	  if (oldin->is_dir()) {
		// fail!
		dout(7) << "dest exists and is dir" << endl;
		reply_request(req, -EISDIR);
		return;
	  }

	  if (srcdn->inode->is_dir() &&
		  !oldin->is_dir()) {
		dout(7) << "cannot overwrite non-directory with directory" << endl;
		reply_request(req, -EISDIR);
		return;
	  }
	}

	dout(7) << "dest exists " << *destdn << endl;
	if (destdn->get_inode()) {
	  dout(7) << "destino is " << *destdn->get_inode() << endl;
	} else {
	  dout(7) << "dest dn is a NULL stub" << endl;
	}
  } else {
	dout(7) << "dest dn dne (yet)" << endl;
  }
  

  // local or remote?
  int srcauth = srcdir->dentry_authority(srcdn->name);
  int destauth = destdir->dentry_authority(destname);
  dout(7) << "handle_client_rename_2 destname " << destname << " destdir " << *destdir << " auth " << destauth << endl;
  
  // 
  if (srcauth != get_nodeid() || 
	  destauth != get_nodeid()) {
	dout(7) << "rename has remote dest " << destauth << endl;

	dout(7) << "FOREIGN RENAME" << endl;
	
	// punt?
	if (false && srcdn->inode->is_dir()) {
	  reply_request(req, -EINVAL);  
	  return; 
	}

  } else {
	dout(7) << "rename is local" << endl;
  }

  handle_client_rename_local(req, ref,
							 srcpath, srcdiri, srcdn, 
							 destpath.get_path(), destdir, destdn, destname);
  return;
}




void MDS::handle_client_rename_local(MClientRequest *req,
									 CInode *ref,
									 string& srcpath,
									 CInode *srcdiri,
									 CDentry *srcdn,
									 string& destpath,
									 CDir *destdir,
									 CDentry *destdn,
									 string& destname)
{
  bool everybody = false;
  //if (true || srcdn->inode->is_dir()) {
	/* overkill warning: lock w/ everyone for simplicity.  FIXME someday!  along with the foreign rename crap!
	   i could limit this to cases where something beneath me is exported.
	   could possibly limit the list.    (maybe.)
	   Underlying constraint is that, regardless of the order i do the xlocks, and whatever
	   imports/exports might happen in the process, the destdir _must_ exist on any node
	   importing something beneath me when rename finishes, or else mayhem ensues when
	   their import is dangling in the cache.
	 */
	/*
	  having made a proper mess of this on the first pass, here is my plan:
	  
	  - xlocks of src, dest are done in lex order
	  - xlock is optional.. if you have the dentry, lock it, if not, don't.
	  - if you discover an xlocked dentry, you get the xlock.

	  possible trouble:
	  - you have an import beneath the source, and don't have the dest dir.
	    - when the actual rename happens, you discover the dest
		- actually, do this on any open dir, so we don't detach whole swaths
		  of our cache.
	  
	  notes:
	  - xlocks are initiated from authority, as are discover_replies, so replicas are 
	    guaranteed to either not have dentry, or to have it xlocked. 
	  - 
	  - foreign xlocks are eventually unraveled by the initiator on success or failure.

	  todo to make this work:
	  - hose bool everybody param crap
	  /- make handle_lock_dn not discover, clean up cases
	  /- put dest path in MRenameNotify
	  /- make rename_notify discover if its a dir
	  /  - this will catch nested imports too, obviously
	  /- notify goes to merged list on local rename
	  /- notify goes to everybody on a foreign rename 
	  /- handle_notify needs to gracefully ignore spurious notifies
	*/
  //dout(7) << "handle_client_rename_local: overkill?  doing xlocks with _all_ nodes" << endl;
  //everybody = true;
  //}

  bool srclocal = srcdn->dir->dentry_authority(srcdn->name) == whoami;
  bool destlocal = destdir->dentry_authority(destname) == whoami;

  dout(7) << "handle_client_rename_local: src local=" << srclocal << " " << *srcdn << endl;
  if (destdn) {
	dout(7) << "handle_client_rename_local: dest local=" << destlocal << " " << *destdn << endl;
  } else {
	dout(7) << "handle_client_rename_local: dest local=" << destlocal << " dn dne yet" << endl;
  }

  /* lock source and dest dentries, in lexicographic order.
   */
  bool dosrc = srcpath < destpath;
  for (int i=0; i<2; i++) {
	if (dosrc) {

	  // src
	  if (srclocal) {
		if (!srcdn->is_xlockedbyme(req) &&
			!mdcache->dentry_xlock_start(srcdn, req, ref))
		  return;  
	  } else {
		if (!srcdn || srcdn->xlockedby != req) {
		  mdcache->dentry_xlock_request(srcdn->dir, srcdn->name, false, req, new C_MDS_RetryRequest(this, req, ref));
		  return;
		}
	  }
	  dout(7) << "handle_client_rename_local: srcdn is xlock " << *srcdn << endl;
	  
	} else {

	  if (destlocal) {
		// dest
		if (!destdn) destdn = destdir->add_dentry(destname);
		if (!destdn->is_xlockedbyme(req) &&
			!mdcache->dentry_xlock_start(destdn, req, ref)) {
		  if (destdn->is_clean() && destdn->is_null() && destdn->is_sync()) destdir->remove_dentry(destdn);
		  return;
		}
	  } else {
		if (!destdn || destdn->xlockedby != req) {
		  /* NOTE: require that my xlocked item be a leaf/file, NOT a dir.  in case
		   * my traverse and determination of dest vs dest/srcfilename was out of date.
		   */
		  mdcache->dentry_xlock_request(destdir, destname, true, req, new C_MDS_RetryRequest(this, req, ref));
		  return;
		}
	  }
	  dout(7) << "handle_client_rename_local: destdn is xlock " << *destdn << endl;

	}
	
	dosrc = !dosrc;
  }

  
  // final check: verify if dest exists that src is a file

  // FIXME: is this necessary?

  if (destdn->inode) {
	if (destdn->inode->is_dir()) {
	  dout(7) << "handle_client_rename_local failing, dest exists and is a dir: " << *destdn->inode << endl;
	  assert(0);
	  reply_request(req, -EINVAL);  
	  return; 
	}
	if (srcdn->inode->is_dir()) {
	  dout(7) << "handle_client_rename_local failing, dest exists and src is a dir: " << *destdn->inode << endl;
	  assert(0);
	  reply_request(req, -EINVAL);  
	  return; 
	}
  } else {
	// if destdn->inode is null, then we know it's a non-existent dest,
	// why?  because if it's local, it dne.  and if it's remote, we xlocked with 
	// REQXLOCKC, which will only allow you to lock a file.
	// so we know dest is a file, or non-existent
	if (!destlocal) {
	  if (srcdn->inode->is_dir()) { 
		// help: maybe the dest exists and is a file?   ..... FIXME
	  } else {
		// we're fine, src is file, dest is file|dne
	  }
	}
  }
  
  balancer->hit_dir(srcdn->dir);
  balancer->hit_dir(destdn->dir);

  // we're golden.
  // everything is xlocked by us, we rule, etc.
  MClientReply *reply = new MClientReply(req, 0);
  mdcache->file_rename( srcdn, destdn,
						new C_MDS_CommitRequest(this, req, reply, srcdn->inode,
												new EInodeUpdate(srcdn->inode)) );  // FIXME WRONG EVENT
}







// MKDIR

void MDS::handle_client_mkdir(MClientRequest *req, CInode *diri)
{
  // make dentry and inode, link.  
  CInode *newi = mknod(req, diri);
  if (!newi) return;
  
  // make my new inode a dir.
  newi->inode.mode = req->get_iarg();
  newi->inode.mode &= ~INODE_TYPE_MASK;
  newi->inode.mode |= INODE_MODE_DIR;
  
  // init dir to be empty
  CDir *newdir = newi->get_or_open_dir(this);
  newdir->mark_complete();
  newdir->mark_dirty();
  
  balancer->hit_dir(newdir);

  // commit
  commit_request(req, new MClientReply(req, 0), diri,
				 new EInodeUpdate(newi));                  // FIXME should be differnet log entry
  
  // schedule a commit for good measure 
  // NOTE: not strictly necessary.. it's in the log!
  // but, if fakemds crashes we'll be less likely to corrupt osddata/* (in leiu of a real recovery mechanism)
  mdstore->commit_dir(newdir, NULL);
  return;
}





// SYMLINK

void MDS::handle_client_symlink(MClientRequest *req, CInode *diri)
{
  // make dentry and inode, link.  
  CInode *newi = mknod(req, diri);
  if (!newi) return;

  // make my new inode a symlink
  newi->inode.mode &= ~INODE_TYPE_MASK;
  newi->inode.mode |= INODE_MODE_SYMLINK;
  
  // set target
  newi->symlink = req->get_sarg();
  
  balancer->hit_inode(newi);

  // commit
  commit_request(req, new MClientReply(req, 0), diri,
				 new EInodeUpdate(newi));                   // FIXME should be differnet log entry
}










// ===========================
// open, openc, close

void MDS::handle_client_open(MClientRequest *req,
							 CInode *cur)
{
  int flags = req->get_iarg();

  dout(7) << "open " << flags << " on " << *cur << endl;

  // is it a file?
  if (!(cur->inode.mode & INODE_MODE_FILE)) {
	dout(7) << "not a regular file" << endl;
	reply_request(req, -EINVAL);                 // FIXME what error do we want?
	return;
  }

  // mode!
  int mode = 0;
  if (flags & O_WRONLY) 
	mode = CFILE_MODE_W;
  else if (flags & O_RDWR) 
	mode = CFILE_MODE_RW;
  else if (flags & O_APPEND)
	mode = CFILE_MODE_W;
  else
	mode = CFILE_MODE_R;

  dout(10) << " flags = " << flags << "  mode = " << mode << endl;

  // auth only
  if (!cur->is_auth()) {
	int auth = cur->authority();
	assert(auth != whoami);
	dout(9) << "open [replica] " << *cur << " on replica, fw to auth " << auth << endl;
	
	mdcache->request_forward(req, auth);
	return;
  }
  assert(cur->is_auth());

  // writer?
  if (mode == CFILE_MODE_W ||
	  mode == CFILE_MODE_RW) {
	if (!mdcache->inode_soft_write_start(cur, req)) return;
  }


  // hmm, check permissions or something.


  // can we issue the caps they want?
  int caps = mdcache->issue_file_caps(cur, mode,
									  new C_MDS_RetryRequest(this, req, cur));
  if (!caps) return; // can't issue (yet), so wait!

  // create fh
  CFile *f = new CFile;
  f->mode = mode;
  f->client = req->get_client();
  f->fh = idalloc->get_id(ID_FH);
  f->pending_caps = f->confirmed_caps = caps;
  f->last_sent = f->last_recv = 0;   // none yet!
  cur->add_fh(f);

  dout(12) << "new fh " << f->fh << " gets caps " << caps << endl;

  if (mode == CFILE_MODE_W ||
	  mode == CFILE_MODE_RW) {
	mdcache->inode_soft_write_finish(cur);
  }
  
  balancer->hit_inode(cur);

  // reply
  MClientReply *reply = new MClientReply(req, f->fh);   // fh # is return code
  reply->set_file_caps(caps);
  reply_request(req, reply, cur);
}



void MDS::handle_client_openc(MClientRequest *req, CInode *ref)
{
  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << endl;

  CInode *in = mknod(req, ref, true);
  if (!in) return;

  in->inode.mode = 0644;              // wtf FIXME
  in->inode.mode |= INODE_MODE_FILE;

  handle_client_open(req, in);
}




void MDS::handle_client_close(MClientRequest *req, CInode *cur) 
{
  // auth only
  if (!cur->is_auth()) {
	int auth = cur->authority();
	assert(auth != whoami);
	dout(9) << "close " << *cur << " on replica, fw to auth " << auth << endl;
	
	mdcache->request_forward(req, auth);
	return;
  }
  assert(cur->is_auth());

  // verify on read or write list
  int client = req->get_client();
  int fh = req->get_iarg();
  CFile *f = cur->get_fh(fh);
  if (!f) {
	dout(1) << "close on unopen fh " << fh << " inode " << *cur << endl;
	assert(0);
  }

  dout(10) << "close on " << *cur << ", fh=" << f->fh << " mode=" << f->mode << endl;

  // update soft metadata
  if (f->confirmed_caps & (CFILE_CAP_WR|CFILE_CAP_WRBUFFER)) {
	assert(cur->softlock.can_write(true));   // otherwise we're toast???
	if (!mdcache->inode_soft_write_start(cur, req))
	  return;  // wait

	// update size, mtime
	time_t mtime = req->get_targ();
	size_t size = req->get_sizearg();
	dout(10) << "mtime is " << mtime << " size is " << size << endl;
	if (mtime > cur->inode.mtime) {
	  cur->inode.mtime = mtime;
	  dout(10) << " extended mtime to " << mtime << endl;
	  cur->mark_dirty();
	}
	if (size > cur->inode.size) {
	  cur->inode.size = size;
	  dout(10) << " extended size to " << size << endl;
	  cur->mark_dirty();
	}
  }

  // atime?  ****
  
  // close it.
  cur->remove_fh(f);

  // reclaim fh
  idalloc->reclaim_id(ID_FH, f->fh);

  // ok we're done
  if (f->confirmed_caps & (CFILE_CAP_WR|CFILE_CAP_WRBUFFER)) {
	dout(7) << "soft write fnish" << endl;

	mdcache->inode_soft_write_finish(cur); 
	mdcache->inode_soft_eval(cur);
  }

  mdcache->eval_file_caps(cur);

  // hose CFile
  delete f;

  // XXX what about atime?

  // commit
  commit_request(req, new MClientReply(req, 0), cur,
				 new EInodeUpdate(cur));               // FIXME wrong message?
}











