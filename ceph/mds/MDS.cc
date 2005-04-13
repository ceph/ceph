
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

#include "messages/MOSDRead.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWriteReply.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

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


void MDS::reply_request(MClientRequest *req, int r)
{	
  // send error
  messenger->send_message(new MClientReply(req, r),
						  MSG_ADDR_CLIENT(req->get_client()), 0,
						  MDS_PORT_SERVER);
  
  // discard request
  delete req;
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

  
  // operations on fh's, or possibly non-existing files

  switch (req->get_op()) {
	// files
  case MDS_OP_OPEN:
	if (req->get_iarg() & O_CREAT) {
	  handle_client_openc(req);
	  return 0;
	}
	break;
	/*
  case MDS_OP_TRUNCATE:
	handle_client_truncate(req);
	return 0;
  case MDS_OP_FSYNC:
	handle_client_fsync(req);
	return 0;
	*/
  case MDS_OP_CLOSE:
	handle_client_close(req);
	return 0;

	// namespace
  case MDS_OP_MKNOD:
	handle_client_mknod(req);
	return 0;
	/*
  case MDS_OP_LINK:
	handle_client_link(req);
	return 0;
	*/
  case MDS_OP_MKDIR:
	handle_client_mkdir(req);
	return 0;
  case MDS_OP_SYMLINK:
	handle_client_symlink(req);
	return 0;
  }

  
  // operations that require existing files

  vector<CInode*> trace;
  int r = mdcache->path_traverse(req->get_filepath(), trace, req, MDS_TRAVERSE_FORWARD);
  if (r > 0) return 0;  // delayed
  if (r == -ENOENT ||
	  r == -ENOTDIR ||
	  r == -EISDIR) {
	// error! 
	dout(10) << " path traverse error " << r << ", replying" << endl;
	reply_request(req, r);
	return 0;
  }

  logger->inc("chit");

  CInode *cur = trace[trace.size()-1];
  
  balancer->hit_inode(cur, MDS_POP_ANY);   // bump popularity

  MClientReply *reply = 0;

  switch(req->get_op()) {
	// fs
	/*
  case MDS_OP_FSTAT:
	reply = handle_client_fstat(req, cur);
	break;
	*/
	
	// inodes
  case MDS_OP_STAT:
	reply = handle_client_stat(req, cur);
	break;
  case MDS_OP_TOUCH:
	reply = handle_client_touch(req, cur);
	break;
  case MDS_OP_UTIME:
	reply = handle_client_utime(req, cur);
	break;
  case MDS_OP_CHMOD:
	reply = handle_client_chmod(req, cur);
	break;
  case MDS_OP_CHOWN:
	reply = handle_client_chown(req, cur);
	break;

	// namespace
  case MDS_OP_READDIR:
	reply = handle_client_readdir(req, cur);
	break;
  case MDS_OP_UNLINK:
	handle_client_unlink(req, cur);
	break;
  case MDS_OP_RENAME:
	handle_client_rename(req, cur);
	break;
  case MDS_OP_RMDIR:
	handle_client_unlink(req, cur);
	break;
	
	// files
  case MDS_OP_OPEN:
	handle_client_open(req, cur);
	break;


  default:
	dout(1) << " unknown client op " << req->get_op() << endl;
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


// STAT

MClientReply *MDS::handle_client_stat(MClientRequest *req,
									  CInode *cur)
{
  if (!mdcache->inode_soft_read_start(cur, req))
	return 0;  // sync

  dout(10) << "reply to " << *req << " stat " << cur->inode.mtime << " pop " << cur->get_popularity() << endl;
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );

  // FIXME: put inode info in reply...

  mdcache->inode_soft_read_finish(cur);

  logger->inc("ostat");
  stat_read.hit();
  stat_req.hit();
  stat_ops++;
  return reply;
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
  
  // done
  delete req;
  
  mdcache->inode_soft_write_finish(cur);
}


// touch

MClientReply *MDS::handle_client_touch(MClientRequest *req,
									   CInode *cur)
{
  // write
  if (!mdcache->inode_soft_write_start(cur, req))
	return 0;  // fw or (wait for) sync

  // do update
  cur->inode.mtime++; // whatever
  if (cur->is_auth())
	cur->mark_dirty();
  
  // init reply
  MClientReply *reply = new MClientReply(req);
  reply->set_trace_dist( cur, whoami );
  reply->set_result(0);

  // wait for log to finish
  dout(10) << "log for " << *req << " touch " << cur->inode.mtime << endl;
  mdlog->submit_entry(new EInodeUpdate(cur),
					  new C_MDS_InodeSoftUpdateFinish(this, req, cur, reply));
  return 0;
}

// utime

MClientReply *MDS::handle_client_utime(MClientRequest *req,
									   CInode *cur)
{
  // write
  if (!mdcache->inode_soft_write_start(cur, req))
	return 0;  // fw or (wait for) sync

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
  return 0;
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
  delete req;
  
  mdcache->inode_hard_write_finish(cur);
}


// chmod

MClientReply *MDS::handle_client_chmod(MClientRequest *req,
									   CInode *cur)
{
  // write
  if (!mdcache->inode_hard_write_start(cur, req))
	return 0;  // fw or (wait for) lock

 
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
  return 0;
}

// chown

MClientReply *MDS::handle_client_chown(MClientRequest *req,
									   CInode *cur)
{
  // write
  if (!mdcache->inode_hard_write_start(cur, req))
	return 0;  // fw or (wait for) lock

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
  return 0;
}





// DIRECTORY and NAMESPACE OPS


MClientReply *MDS::handle_client_readdir(MClientRequest *req,
										 CInode *cur)
{
  // check perm
  if (!mdcache->inode_hard_read_start(cur,req))
	return NULL;
  mdcache->inode_hard_read_finish(cur);

  // it's a directory, right?
  if (!cur->is_dir()) {
	// not a dir
	dout(10) << "reply to " << *req << " readdir -ENOTDIR" << endl;
	return new MClientReply(req, -ENOTDIR);
  }

  // make sure i'm authoritative!
  if (cur->dir_is_auth()) {
	
	cur->get_or_open_dir(this);
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
	  dout(10) << " incomplete dir contents for readdir on " << *cur->dir << ", fetching" << endl;
	  mdstore->fetch_dir(cur->dir, new C_MDS_RetryMessage(this, req));
	  return 0;
	}
  } else {
	int dirauth = cur->authority();
	if (cur->dir)
	  dirauth = cur->dir->authority();
	assert(dirauth >= 0);
	assert(dirauth != whoami);

	// forward to authority
	dout(10) << " forwarding readdir to authority " << dirauth << endl;
	messenger->send_message(req,
							MSG_ADDR_MDS(dirauth), MDS_PORT_SERVER,
							MDS_PORT_SERVER);
	//mdcache->show_imports();
	return 0;
  }
}


// MKNOD

void MDS::handle_client_mknod(MClientRequest *req)
{
  // make dentry and inode, link.  
  CInode *newi = mknod(req);
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

CInode *MDS::mknod(MClientRequest *req) 
{
  dout(10) << "mknod " << req->get_filepath() << " depth " << req->get_filepath().depth() << endl;

  // get containing directory (without last bit)
  filepath dirpath = req->get_filepath().prefixpath(req->get_filepath().depth() - 1);
  string name = req->get_filepath().last_bit();
  
  // trace to full path...
  vector<CInode*> trace;
  int r = mdcache->path_traverse(req->get_filepath(), trace, req, MDS_TRAVERSE_FORWARD);
  if (r > 0) return 0;  // forwarded or opening
  
  if (r == 0) {
	dout(10) << "already exists, replying" << endl;
	reply_request(req, r);
	return 0;
  }

  // did we make it to the containing dir?
  if (r < 0 && !(r == -ENOENT && trace.size() == dirpath.depth()+1)) {  // containing path dne
	assert(trace.size() < dirpath.depth()+1);
	dout(10) << "container dne, error" << r << ", replying" << endl;
	reply_request(req, r);
	return 0;
  }
  
  // did we get to parent?
  dout(10) << "dirpath is " << dirpath << " depth " << dirpath.depth() << endl;
  dout(10) << "traced to " << *trace[trace.size()-1] << " size " << trace.size() << endl;
  assert(trace.size() == dirpath.depth()+1);

  // make sure parent is a dir
  CInode *diri = trace[trace.size()-1];
  assert(diri->is_dir());
  CDir *dir = diri->get_or_open_dir(this);

  // make sure it's my dentry
  if (dir->dentry_authority(name) != get_nodeid()) {
	assert(0);
	// fw
	int auth = dir->dentry_authority(name);
	dout(7) << "mknod on " << req->get_path() << ", dentry " << *dir << " dn " << name << " not mine, fw to " << auth << endl;
	messenger->send_message(req,
							MSG_ADDR_MDS(auth), MDS_PORT_SERVER, MDS_PORT_SERVER);
	return 0;
  }

  // make sure name doesn't already exist
  CDentry *dn = dir->lookup(name);
  if (dn != 0) {
	if (dn->is_xlockedbyother(req)) {
	  dout(10) << "waiting, dentry " << name << " locked in " << *dir << endl;
	  dir->add_waiter(CDIR_WAIT_DNREAD, name, new C_MDS_RetryMessage(this, req));
	  return 0;
	}

	// name already exists
	dout(10) << "dentry " << name << " exists in " << *dir << endl;
	reply_request(req, -EEXIST);
	return 0;
  }

  // make sure dir is complete
  if (!dir->is_complete()) {
	dout(7) << " incomplete dir contents for " << *dir << ", fetching" << endl;
	mdstore->fetch_dir(dir, new C_MDS_RetryMessage(this, req));
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
  mdcache->link_inode(dir, name, newi);
  
  // dirty
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
	delete req;
  }
};

void MDS::handle_client_unlink(MClientRequest *req, 
							   CInode *in)
{
  // rmdir or unlink
  bool rmdir = false;
  if (req->get_op() == MDS_OP_RMDIR) rmdir = true;
  
  if (rmdir) {
	dout(7) << "handle_client_rmdir on dir " << *in << endl;
  } else {
	dout(7) << "handle_client_unlink on non-dir " << *in << endl;
  }

  // what are we talking about
  CDentry *dn = in->get_parent_dn();         // XXX FIXME for hard links... XXX

  // can't unlink or rmdir root
  if (!dn) {
	assert(in->ino() == 1);
	dout(7) << "handle_client_rmdir can't rmdir root" << endl;
	reply_request(req, -EPERM);
	return;
  }

  CDir *dir = dn->dir;

  // dn auth
  int dnauth = dir->dentry_authority(dn->name);

  // dir stuff 
  if (in->is_dir()) {
	if (rmdir) {
	  // rmdir
	  
	  // open dir?
	  if (in->is_auth() && !in->dir) in->get_or_open_dir(this);

	  // not dir auth?  (or not open, which implies the same!)
	  if (!in->dir) {
		dout(7) << "handle_client_rmdir dir not open for " << *in << ", sending to dn auth " << dnauth << endl;
		messenger->send_message(req,
								MSG_ADDR_MDS(dnauth), MDS_PORT_SERVER, MDS_PORT_SERVER);
		return;
	  }
	  if (!in->dir->is_auth()) {
		int dirauth = in->dir->authority();
		dout(7) << "handle_client_rmdir not auth for dir " << *in->dir << ", sending to dir auth " << dnauth << endl;
		messenger->send_message(req,
								MSG_ADDR_MDS(dirauth), MDS_PORT_SERVER, MDS_PORT_SERVER);
		return;
	  }

	  assert(in->dir);
	  assert(in->dir->is_auth());

	  // dir size check on dir auth (but not necessarily dentry auth)?

	  // should be empty
	  if (in->dir->get_size() == 0 && !in->dir->is_complete()) {
		dout(7) << "handle_client_rmdir on dir " << *in->dir << ", empty but not complete, fetching" << endl;
		mdstore->fetch_dir(in->dir, 
						   new C_MDS_RetryMessage(this, req));
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
							new C_MDS_RetryMessage(this, req));
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
	messenger->send_message(req,
							MSG_ADDR_MDS(dnauth), MDS_PORT_SERVER, MDS_PORT_SERVER);
	return;
  }
    
  dout(7) << "handle_client_unlink/rmdir on " << *in << endl;
  
  // xlock dentry
  if (!mdcache->dentry_xlock_start(dn, req))
	return;
	
  // it's locked, unlink!
  mdcache->dentry_unlink(dn,
						 new C_MDS_Unlink(this,dn,req));
  return;
}


// RENAME

void MDS::handle_client_rename(MClientRequest *req,
							   CInode *cur)
{
  // src dentry
  CDentry *srcdn = cur->get_parent_dn();     // FIXME for hard links

  if (!srcdn) {
	dout(10) << "handle_client_rename can't rename root " << *cur << endl;
	reply_request(req, -EINVAL);
	return;
  }

  CDir    *srcdir = srcdn->dir;

  // make sure i'm auth on source
  int srcauth = srcdn->dir->dentry_authority(srcdn->get_name());
  if (srcauth != get_nodeid()) {
	// forward
	dout(10) << "handle_client_rename " << *cur << "  not auth, fwd to " << srcauth << endl;
	int auth = cur->authority();
	assert(auth != get_nodeid());
	messenger->send_message(req,
							MSG_ADDR_MDS(auth), MDS_PORT_SERVER,
							MDS_PORT_SERVER);
	return;
  }
  
  dout(10) << "handle_client_rename " << *srcdn << " to " << req->get_sarg() << endl;
  
  // find the destination, normalize
  // discover, etc. on the way... just get it on the local node.
  filepath destpath = req->get_sarg();   
  vector<CInode*> trace;
  int r = mdcache->path_traverse(destpath, trace, req, MDS_TRAVERSE_DISCOVER);
  dout(12) << " r = " << r << " trace size " << trace.size() << "  destpath depth " << destpath.depth() << endl;
  if (r > 0)   // waiting.. discover, or readdir
	return; 

  CDir*  destdir = 0;
  string destname;
  int destauth = -1;
  
  // what is the dest?  (dir or file or complete filename)
  // note: trace includes root, destpath doesn't (include leading /)
  if (trace.size() == destpath.depth()+1) {
	CInode *d = trace[trace.size()-1];
	if (d->is_dir()) {
	  // mv /some/thing /to/some/dir 
	  destdir = d->get_or_open_dir(this);         // /to/some/dir
	  destname = req->get_filepath().last_bit();  // thing
	} else {
	  // mv /some/thing /to/some/existing_filename
	  destdir = trace[trace.size()-2]->get_or_open_dir(this);       // /to/some
	  destname = destpath.last_bit();                               // existing_filename
	}
  }
  else if (trace.size() == destpath.depth()) {
	CInode *d = trace[trace.size()-1];
	if (d->is_dir()) {
	  // mv /some/thing /to/some/place_that_maybe_dne     (we might be replica)
	  destdir = d->get_or_open_dir(this);   // /to/some
	  destname = destpath.last_bit();       // place_that_MAYBE_dne
	}
  }
  else {
	assert(trace.size() < destpath.depth());
	// check traverse return value
	if (r > 0) 
	  return;  // discover, readdir, etc.

	// ??
	assert(r < 0 || trace.size() <= 1);  // musta been an error

	// error out
	dout(7) << " rename dest " << destpath << " dne" << endl;
	goto fail;
  }


  dout(7) << "srcino is " << *srcdn->get_inode() << endl;

  destauth = destdir->dentry_authority(destname);
  dout(7) << "destname " << destname << " destdir " << *destdir << " auth " << destauth << endl;
  

  if (srcdn->get_dir() == destdir && srcdn->name == destname) {
	dout(7) << "rename src=dest, same file " << destauth << endl;
	goto fail;
  }


  // local or remote?
  if (destauth != get_nodeid()) {
	dout(7) << "rename has remote dest " << destauth << endl;
	
	// implement me
	//assert(0);
	
	// *** IMPLEMENT ME ***
	
	dout(7) << "rename NOT FULLY IMPLEMENTED" << endl;
	
	goto fail;
  }

  // file or dir?
  if (cur->is_dir()) {

	//handle_client_rename_dir(req,cur);
	// *** IMPLEMENT ME ***

	dout(7) << "rename NOT FULLY IMPLEMENTED" << endl;
	goto fail;
  } else {
	handle_client_rename_file(req, srcdn, destdir, destname);
  }

  return;

 fail:
  // make sure we unlock (if we locked);
  if (srcdn->is_xlockedbyme(req)) 
	mdcache->dentry_xlock_finish(srcdn);
  reply_request(req, -EINVAL);
  return;
}


/*

locking

if (!locked && flag=renaming)
(maybe) if (!locked && flag=renamingto)



basic protocol with replicas:

 > Lock    (possibly x2?)
 < LockAck (possibly x2?)
 > Rename
    src ino
    dst dir
    either dst ino (is unlinked)
        or dst name
 < RenameAck
    (implicitly unlocks, unlinks, etc.)

*/

class C_MDS_RenameFinish : public Context{
  MDS *mds;
  MClientRequest *req;
public:
  C_MDS_RenameFinish(MDS *mds, MClientRequest *req) {
	this->mds = mds;
	this->req = req;
  }
  virtual void finish(int r) {
	MClientReply *reply = new MClientReply(req, r);
	
	// include trace?
	// XXX FIXME?

	mds->messenger->send_message(reply,
								 MSG_ADDR_CLIENT(req->get_client()), 0,
								 MDS_PORT_SERVER);
	delete req;
  }
};

void MDS::handle_client_rename_file(MClientRequest *req,
									CDentry *srcdn,
									CDir *destdir,
									string& destname)
{
  bool must_wait_for_lock = false;
  
  // does destination exist?  (is this an overwrite?)
  CDentry *destdn = destdir->lookup(destname);
  CInode  *oldin = 0;
  if (destdn) {
	oldin = destdn->get_inode();
	
	// make sure it's also a file!
	// this can happen, e.g. "mv /some/thing /a/dir" where /a/dir/thing exists and is a dir.
	if (oldin->is_dir()) {
	  // fail!
	  dout(7) << "dest exists and is dir" << endl;
	  if (srcdn->is_xlockedbyme(req)) mdcache->dentry_xlock_finish(srcdn);
	  reply_request(req, -EISDIR);
	  return;
	}

	dout(7) << "dest exists " << *destdn << endl;
	dout(7) << "destino is " << *destdn->get_inode() << endl;
  } else {
	dout(7) << "dest dne" << endl;
  }

  // lock src
  if (!srcdn->is_xlockedbyme(req) &&
	  !mdcache->dentry_xlock_start(srcdn, req))
	return;  

  dout(7) << "srcdn is xlock " << *srcdn << endl;

  // dest
  if (destdn) {
	// lock dest
	if (!mdcache->dentry_xlock_start(destdn, req))
	  return;

	dout(7) << "destdn is xlock " << *srcdn << endl;
  } else {
	// dest dne.
  }

  // we're a go.
  mdcache->file_rename( srcdn,
						destdir, destname, destdn,
						new C_MDS_RenameFinish(this, req) );
}

/*
void MDS::handle_client_rename_dir(MClientRequest *req,
									CInode *from,
									CDir *destdir,
									string name)
{

  // ...


  // make sure dest isn't nested under src
  CDir *td = destdir;
  while (td) {
	if (from->dir == td) {
	  // error out
	  dout(7) << " dest " << *destdir << " nested under src " << *from << endl;
	  reply_request(req, -EINVAL);
	  return;
	}
	td = td->get_parent_dir();
  }
  


  // ..

  // ok do it
  mdcache->rename_dir(from, destdir, name);



  

}
*/






// MKDIR

void MDS::handle_client_mkdir(MClientRequest *req) 
{
  // make dentry and inode, link.  
  CInode *newi = mknod(req);
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

void MDS::handle_client_symlink(MClientRequest *req) 
{
  // make dentry and inode, link.  
  CInode *newi = mknod(req);
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
	  
	  messenger->send_message(req,
							  MSG_ADDR_MDS(auth), MDS_PORT_SERVER,
							  MDS_PORT_SERVER);
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
  delete req;
}



void MDS::handle_client_openc(MClientRequest *req)
{
  dout(7) << "open w/ O_CREAT on " << req->get_filepath() << endl;

  // see if file exists  
  vector<CInode*> trace;
  int r = mdcache->path_traverse(req->get_filepath(), trace, req, MDS_TRAVERSE_FORWARD);
  if (r > 0) return;  // delayed

  if (r == 0) {
	// it exists, behave normally.
	CInode *cur = trace[trace.size()-1];
	handle_client_open(req, cur);
	return;
  }

  assert (r < 0);

  // what happened?
  if (r == -ENOENT) {

	CInode *cur = mknod(req);
	if (!cur) return;

	// log it
	dout(10) << "log for " << *req << " mknod " << cur->ino() << endl;
	mdlog->submit_entry(new EInodeUpdate(cur),                    // FIXME should be differnet log entry
						new C_MDS_RetryMessage(this, req));
	
	return;
  }
  
  // send error response
  dout(7) << "handle_client_openc error " << r << " replying to client" << endl;
  reply_request(req, r);
  return;
}



void MDS::handle_client_close(MClientRequest *req) 
{
  CInode *cur = mdcache->get_inode(req->get_ino());
  assert(cur);

  // verify on read or write list
  int client = req->get_client();
  int fh = req->get_iarg();
  CFile *f = cur->get_fh(fh);
  if (!f) {
	dout(1) << "close on unopen file " << *cur << endl;
	assert(2+2==5);
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
  reply->set_iarg( req->get_iarg() );
  messenger->send_message(reply,
						  req->get_source(), 0, MDS_PORT_SERVER);

  // done
  delete req;
}







// WRITE ME


/*
void MDS::handle_client_rmdir(MClientRequest *req,
							  CInode *cur)
{
  assert(cur->is_auth());
  assert(cur->is_dir());
  
  if (cur->dir_is_auth()) {
	// ok good, i have the dir and the inode.  
	CDir *dir = cur->get_or_open_dir(this);
	
	// empty?
	if (dir->get_size() > 0) {
	  // not empty
	  MClientReply *reply = new MClientReply(req, -ENOTEMPTY);
	  return reply;
	} else {
	  if (!dir->is_complete()) {
		// fetch
		mdstore->fetch_dir(cur->dir, new C_MDS_RetryMessage(this, req));
		return 0;
	  }
	  
	  // else... complete and empty!
	}
	assert(dir->is_complete());
	assert(dir->get_size() == 0);

	// close any dups?
	if (dir->is_open_by_anyone()) {
	  // ***
	}

	// remove
		
	
  } else {
	// ugh: reimport, but only if empty, etc.
	
  }

  
}

*/








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
