
#include "include/types.h"
#include "include/Clock.h"

#include "msg/Messenger.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDStore.h"
#include "MDLog.h"
#include "MDCluster.h"
#include "MDBalancer.h"
#include "IdAllocator.h"


#include "include/filepath.h"

#include "include/Logger.h"
#include "include/LogType.h"

#include "messages/MPing.h"
#include "messages/MGenericMessage.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWriteReply.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

#include "messages/MLock.h"
#include "messages/MInodeWriterClosed.h"


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
  // init messenger
  messenger->set_dispatcher(this);
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
	// MDS's all shut down!
	
	// shut down osd's
	for (int i=0; i<g_conf.num_osd; i++) {
	  messenger->send_message(new MGenericMessage(MSG_SHUTDOWN),
							  MSG_ADDR_OSD(i), 0, 0);
	}

	// shut myself down.
	shutting_down = false;
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

  // finish any triggered contexts
  if (finished_queue.size()) {
	dout(7) << "mds has " << finished_queue.size() << " queued contexts" << endl;
	list<Context*> ls;
	ls.splice(ls.begin(), finished_queue);
	assert(finished_queue.empty());
	finish_contexts(ls);
  }

  // balance?
  /*
  if (whoami == 0 &&
	  stat_ops >= last_heartbeat + g_conf.mds_heartbeat_op_interval) {
	last_heartbeat = stat_ops;
	balancer->send_heartbeat();
  }
  */
  if (whoami == 0) {
	static bool didit = false;
	
	// 7 to 1
	CInode *in = mdcache->get_inode(2);
	if (in && !didit) {
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
	  shutdown_final();
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


void MDS::reply_request(MClientRequest *req, int r)
{	
  // send error
  messenger->send_message(new MClientReply(req, r),
						  MSG_ADDR_CLIENT(req->get_client()), 0,
						  MDS_PORT_SERVER);
  
  // discard request
  mdcache->request_finish(req);
}



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
	open_root(new C_MDS_RetryMessage(this, req));
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

  // bump popularity
  balancer->hit_inode(ref, MDS_POP_ANY);   
  
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
	/*
  case MDS_OP_LINK:
	handle_client_link(req, ref);
	break;
	*/
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

  dout(10) << "reply to " << *req << " stat " << ref->inode.mtime << " pop " << ref->get_popularity() << endl;
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( ref, whoami );

  // FIXME: put inode info in reply...

  mdcache->inode_soft_read_finish(ref);

  logger->inc("ostat");
  stat_read.hit();
  stat_req.hit();
  stat_ops++;

  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->get_client()), 0,
						  MDS_PORT_SERVER);

  mdcache->request_finish(req);
}



// INODE UPDATES

// SOFT

class C_MDS_InodeSoftUpdateFinish : public Context {
public:
  CInode *in;
  MClientRequest *req;
  MDS *mds;
  MClientReply *reply;
  C_MDS_InodeSoftUpdateFinish(MDS *mds, MClientRequest *req, CInode *cur, MClientReply *reply) {
	this->mds = mds;
	this->in = cur;
	this->req = req;
	this->reply = reply;
  }
  virtual void finish(int result) {
	mds->handle_client_inode_soft_update_2(req, reply, in);
  }
};

void MDS::handle_client_inode_soft_update_2(MClientRequest *req,
											MClientReply *reply,
											CInode *cur)
{
  // reply
  dout(10) << "reply to " << *req << " inode soft update " << *cur << endl;
  
  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->get_client()), 0,
						  MDS_PORT_SERVER);
  
  logger->inc("otouch");
  stat_write.hit();
  stat_req.hit();
  stat_ops++;
  
  mdcache->inode_soft_write_finish(cur);

  mdcache->request_finish(req);
}




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
  
  // init reply
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  reply->set_result(0);

  // wait for log to finish
  dout(10) << "log for " << *req << " utime " << cur->inode.mtime << endl;
  mdlog->submit_entry(new EInodeUpdate(cur),
					  new C_MDS_InodeSoftUpdateFinish(this, req, cur, reply));
  return;
}

						   

// HARD

class C_MDS_InodeHardUpdateFinish : public Context {
public:
  CInode *in;
  MClientRequest *req;
  MDS *mds;
  MClientReply *reply;
  C_MDS_InodeHardUpdateFinish(MDS *mds, MClientRequest *req, CInode *cur, MClientReply *reply) {
	this->mds = mds;
	this->in = cur;
	this->req = req;
	this->reply = reply;
  }
  virtual void finish(int result) {
	mds->handle_client_inode_hard_update_2(req, reply, in);
  }
};

void MDS::handle_client_inode_hard_update_2(MClientRequest *req,
											MClientReply *reply,
											CInode *cur)
{
  // reply
  dout(10) << "reply to " << *req << " inode hard update " << *cur << endl;
  
  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->get_client()), 0,
						  MDS_PORT_SERVER);
  
  logger->inc("otouch");
  stat_write.hit();
  stat_req.hit();
  stat_ops++;
  
  // done
  mdcache->inode_hard_write_finish(cur);

  mdcache->request_finish(req);
}


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

  // start reply
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  reply->set_result(0);

  // wait for log to finish
  dout(10) << "log for " << *req << " chmod" << endl;
  mdlog->submit_entry(new EInodeUpdate(cur),
					  new C_MDS_InodeHardUpdateFinish(this, req, cur, reply));
  return;
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

  // start reply
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  reply->set_result(0);

  // wait for log to finish
  dout(10) << "log for " << *req << " chown" << endl;
  mdlog->submit_entry(new EInodeUpdate(cur),
					  new C_MDS_InodeHardUpdateFinish(this, req, cur, reply));
  return;
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

  
  if (cur->dir->is_complete()) {
	// yay, reply
	MClientReply *reply = new MClientReply(req);
	
	// FIXME: need to sync all inodes in this dir.  blech!
	
	// build dir contents
	CDir_map_t::iterator it;
	int numfiles = 0;
	for (it = cur->dir->begin(); it != cur->dir->end(); it++) {
	  //string name = it->first;
	  CDentry *dn = it->second;
	  CInode *in = dn->inode;
	  if (!in) continue;  // null
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
	
	//mdcache->path_unpin(trace, req);
	
	messenger->send_message(reply,
							MSG_ADDR_CLIENT(req->get_client()), 0,
							MDS_PORT_SERVER);
	mdcache->request_finish(req);
  } else {
	// fetch
	dout(10) << " incomplete dir contents for readdir on " << *cur->dir << ", fetching" << endl;
	mdstore->fetch_dir(cur->dir, new C_MDS_RetryRequest(this, req, cur));
  }
}


// MKNOD

void MDS::handle_client_mknod(MClientRequest *req, CInode *ref)
{
  // make dentry and inode, link.  
  CInode *newi = mknod(req, ref);
  if (!newi) return;
  
  // log it
  dout(10) << "log for " << *req << " mknod " << newi->ino() << endl;
  mdlog->submit_entry(new EInodeUpdate(newi),                    // FIXME should be differnet log entry
					  NULL);
  
  // reply
  reply_request(req, 0);
  return;
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
  newi->inode.mode = req->get_iarg();
  newi->inode.uid = req->get_caller_uid();
  newi->inode.gid = req->get_caller_gid();
  newi->inode.ctime = 1;  // now, FIXME
  newi->inode.mtime = 1;  // now, FIXME
  newi->inode.atime = 1;  // now, FIXME

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


// UNLINK

class C_MDS_Unlink : public Context {
public:
  MDS *mds;
  CDentry *dn;
  MClientRequest *req;
  C_MDS_Unlink(MDS *mds, CDentry *dn, MClientRequest *req) {
	this->mds = mds;
	this->dn = dn;
	this->req = req;
  }
  virtual void finish(int r) {
	// reply
	MClientReply *reply = new MClientReply(req);
	mds->messenger->send_message(reply,
								 MSG_ADDR_CLIENT(req->get_client()), 0, MDS_PORT_SERVER);
	
	// done.
	mds->mdcache->request_finish(req);
  }
};

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
		dout(7) << "handle_client_rmdir dir is auth, but not inode.  i should be exporting!" << endl;
		assert(in->dir->is_freezing() || in->dir->is_frozen());
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
	
  // it's locked, unlink!
  mdcache->dentry_unlink(dn,
						 new C_MDS_Unlink(this,dn,req));
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
	dout(7) << "forwarded, ending request" << endl;
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
  CDentry *srcdn = srcdir->lookup(srcname);     // FIXME for hard links

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
  
  // find the destination, normalize
  // discover, etc. on the way... just get it on the local node.
  filepath destpath = req->get_sarg();   

  C_MDS_RenameTraverseDst *onfinish = new C_MDS_RenameTraverseDst(this, req, ref, srcdiri, srcdir, srcdn, destpath);
  Context *ondelay = new C_MDS_RetryRequest(this, req, ref);
  
  mdcache->path_traverse(destpath, onfinish->trace, false,
						 req, ondelay,
						 MDS_TRAVERSE_DISCOVER, 
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
  int destauth = -1;
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
	dout(7) << "rename src=dest, same file " << destauth << endl;
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
  destauth = destdir->dentry_authority(destname);
  dout(7) << "handle_client_rename_2 destname " << destname << " destdir " << *destdir << " auth " << destauth << endl;
  
  if (destauth != get_nodeid()) {
	dout(7) << "rename has remote dest " << destauth << endl;
	dout(7) << "FOREIGN RENAME" << endl;
	//return;
  } else {
	dout(7) << "rename is local" << endl;
  }

  handle_client_rename_local(req, ref,
							 srcpath, srcdiri, srcdn, 
							 destpath.get_path(), destdir, destdn, destname);
  return;
}



class C_MDS_RenameFinish : public Context{
  MDS *mds;
  MClientRequest *req;
  CInode *renamedi;
public:
  C_MDS_RenameFinish(MDS *mds, MClientRequest *req, CInode *renamedi) {
	this->mds = mds;
	this->req = req;
	this->renamedi = renamedi;
  }
  virtual void finish(int r) {
	MClientReply *reply = new MClientReply(req, r);
	
	// include trace of renamed inode (so client can update their cache structure)
	reply->set_trace_dist( renamedi, mds->get_nodeid() );

	mds->messenger->send_message(reply,
								 MSG_ADDR_CLIENT(req->get_client()), 0,
								 MDS_PORT_SERVER);
	mds->mdcache->request_finish(req);
  }
};

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
  if (true || srcdn->inode->is_dir()) {
	/* overkill warning: lock w/ everyone for simplicity.  FIXME someday!  along with the foreign rename crap!
	   i could limit this to cases where something beneath me is exported.
	   could possibly limit the list.    (maybe.)
	   Underlying constraint is that, regardless of the order i do the xlocks, and whatever
	   imports/exports might happen in the process, the destdir _must_ exist on any node
	   importing something beneath me when rename finishes, or else mayhem ensues when
	   their import is dangling in the cache.
	 */
	dout(7) << "handle_client_rename_local: overkill?  doing xlocks with _all_ nodes" << endl;
	everybody = true;
  }

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
			!mdcache->dentry_xlock_start(srcdn, req, ref, everybody))
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
			!mdcache->dentry_xlock_start(destdn, req, ref, everybody)) {
		  if (destdn->is_clean() && destdn->is_null() && destdn->is_sync()) destdir->remove_dentry(destdn);
		  return;
		}
	  } else {
		if (!destdn || destdn->xlockedby != req) {
		  mdcache->dentry_xlock_request(destdir, destname, true, req, new C_MDS_RetryRequest(this, req, ref));
		  return;
		}
	  }
	  dout(7) << "handle_client_rename_local: destdn is xlock " << *destdn << endl;

	}
	
	dosrc = !dosrc;
  }

  // we're golden (everything is xlocked by us, we rule, etc.)
  mdcache->file_rename( srcdn, destdn,
						new C_MDS_RenameFinish(this, req, srcdn->inode),
						everybody );
}







// MKDIR

void MDS::handle_client_mkdir(MClientRequest *req, CInode *diri)
{
  // make dentry and inode, link.  
  CInode *newi = mknod(req, diri);
  if (!newi) return;

  // make my new inode a dir.
  newi->inode.mode |= INODE_MODE_DIR;
  
  // init dir to be empty
  CDir *newdir = newi->get_or_open_dir(this);
  newdir->mark_complete();
  newdir->mark_dirty();
  
  // log it
  dout(10) << "log for " << *req << " mkdir " << newi->ino() << endl;
  mdlog->submit_entry(new EInodeUpdate(newi),                    // FIXME should be differnet log entry
					  NULL);
  
  // schedule a commit for good measure 
  // NOTE: not strictly necessary.. it's in the log!
  // but, if test crashes we'll be less likely to corrupt osddata/* (in leiu of a real recovery mechanism)
  mdstore->commit_dir(newdir, NULL);

  // reply
  reply_request(req, 0);
  return;
}





// SYMLINK

void MDS::handle_client_symlink(MClientRequest *req, CInode *diri)
{
  // make dentry and inode, link.  
  CInode *newi = mknod(req, diri);
  if (!newi) return;

  // make my new inode a symlink
  newi->inode.mode |= INODE_MODE_SYMLINK;
  
  // set target
  newi->symlink = req->get_sarg();
  
  // log it
  dout(10) << "log for " << *req << " symlink " << newi->ino() << endl;
  mdlog->submit_entry(new EInodeUpdate(newi),                    // FIXME should be differnet log entry
					  NULL);
  
  // reply
  reply_request(req, 0);
  return;
}










// ===========================
// open, openc, close

void MDS::handle_client_open(MClientRequest *req,
							 CInode *cur)
{
  int flags = req->get_iarg();

  dout(7) << "open " << flags << " on " << *cur << endl;

  int mode;
  if (flags & O_RDONLY || flags == O_RDONLY) {
	mode = CFILE_MODE_R;
  }
  else if (flags & O_WRONLY) {
	mode = CFILE_MODE_W;

	if (!cur->is_auth()) {
	  int auth = cur->authority();
	  assert(auth != whoami);
	  dout(9) << "open (write) [replica] " << *cur << " on replica, fw to auth " << auth << endl;
	  
	  mdcache->request_forward(req, auth);
	  return;
	}

	// only writers on auth, for now.
	assert(cur->is_auth());
	
	// switch to async mode!
	mdcache->inode_soft_mode(cur, LOCK_MODE_ASYNC);   // don't forget to eval softlock state below
  }
  else if (flags & O_RDWR) {
	assert(0);  // WR not implemented yet  
  } else {
	assert(0);  // no mode specified.  this should actually be an error code back to client.
  }

  // hmm, check permissions or something.

  // create fh
  CFile *f = new CFile;
  f->mode = mode;
  f->client = req->get_client();
  f->fh = idalloc->get_id(ID_FH);
  cur->add_fh(f);
  
  // eval our locking mode?
  if (cur->is_auth())
	mdcache->inode_soft_eval(cur);

  // reply
  MClientReply *reply = new MClientReply(req, f->fh);   // fh # is return code
  reply->set_trace_dist( cur, whoami );

  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->get_client()), 0,
						  MDS_PORT_SERVER);
  
  // discard request
  mdcache->request_finish(req);
}



void MDS::handle_client_openc(MClientRequest *req, CInode *ref)
{
  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << endl;

  CInode *in = mknod(req, ref, true);
  if (!in) return;

  handle_client_open(req, in);
}




void MDS::handle_client_close(MClientRequest *req, CInode *cur) 
{
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
  if (f->mode != CFILE_MODE_R) {
	assert(cur->softlock.get_mode() == LOCK_MODE_ASYNC);  // otherwise we're toast?
	if (!mdcache->inode_soft_write_start(cur, req))
	  return;  // wait
  }
  
  // update size, mtime
  // XXX

  /*
  // mark dirty
  cur->mark_dirty();

  // log it
  dout(10) << "log for " << *req << " touch " << cur->inode.mtime << endl;
  mdlog->submit_entry(new EInodeUpdate(cur),
					  new C_MDS_TouchFinish(this, req, cur, reply));
  */

  // close it.
  cur->remove_fh(f);

  // reclaim fh
  idalloc->reclaim_id(ID_FH, f->fh);

  // ok we're done
  if(f->mode != CFILE_MODE_R) {
	if (!cur->is_auth() &&
		!cur->is_open_write()) {
	  // we were a replica writer!
	  int a = cur->authority();
	  dout(7) << "last writer closed, telling " << *cur << " auth " << a << endl;
	  MInodeWriterClosed *m = new MInodeWriterClosed(cur->ino(), whoami);
	  messenger->send_message(m,
							  a, MDS_PORT_CACHE, MDS_PORT_SERVER);
	}

	dout(7) << "soft write fnish" << endl;
	mdcache->inode_soft_write_finish(cur); 
	mdcache->inode_soft_eval(cur);
  }

  // hose CFile
  delete f;

  // XXX what about atime?


  // reply
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  //reply->set_iarg( req->get_iarg() );
  messenger->send_message(reply,
						  req->get_source(), 0, MDS_PORT_SERVER);

  // done
  mdcache->request_finish(req);
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
