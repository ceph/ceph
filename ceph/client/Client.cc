
// ceph stuff
#include "Client.h"

// unix-ey fs stuff
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <utime.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientFileCaps.h"

#include "messages/MGenericMessage.h"

#include "osd/Filer.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Logger.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_client) cout << "client" << whoami << "." << pthread_self() << " "

#define  tout       if (g_conf.client_trace) cout << "trace: " 


// static logger
LogType client_logtype;
Logger  *client_logger = 0;



// cons/des

Client::Client(MDCluster *mdc, int id, Messenger *m)
{
  mdcluster = mdc;
  whoami = id;

  mounted = false;

  // 
  all_files_closed = false;
  root = 0;

  set_cache_size(g_conf.client_cache_size);

  // file handles
  free_fh_set.map_insert(10, 1<<30);

  // set up messengers
  messenger = m;
  messenger->set_dispatcher(this);

  // osd interfaces
  osdcluster = new OSDCluster();     // initially blank.. see mount()
  filer = new Filer(messenger, osdcluster);
}


Client::~Client() 
{
  if (messenger) { delete messenger; messenger = 0; }
  if (filer) { delete filer; filer = 0; }
  if (osdcluster) { delete osdcluster; osdcluster = 0; }

  tear_down_cache();
}

void Client::tear_down_cache()
{
  // fh's
  for (hash_map<fh_t, Fh*>::iterator it = fh_map.begin();
	   it != fh_map.end();
	   it++) {
	Fh *fh = it->second;
	dout(1) << "tear_down_cache forcing close of fh " << it->first << " ino " << fh->inode->inode.ino << endl;
	put_inode(fh->inode);
	delete fh;
  }
  fh_map.clear();

  // empty lru
  lru.lru_set_max(0);
  unsigned last = 0;
  while (lru.lru_get_size() != last) {
	last = lru.lru_get_size();
	dout(10) << "trim pass, size is " << last << endl;
	//dump_cache();
	trim_cache();
  }

  // close root ino
  assert(inode_map.size() <= 1);
  if (root && inode_map.size() == 1) {
	delete root;
	root = 0;
	inode_map.clear();
  }

  assert(inode_map.empty());
}



// debug crapola

void Client::dump_inode(Inode *in, set<Inode*>& did)
{
  dout(1) << "inode " << in << " ref " << in->ref << " dir " << in->dir << endl;

  if (in->dir) {
	dout(1) << "  dir size " << in->dir->dentries.size() << endl;
	//for (hash_map<const char*, Dentry*, hash<const char*>, eqstr>::iterator it = in->dir->dentries.begin();
	for (hash_map<string, Dentry*>::iterator it = in->dir->dentries.begin();
		 it != in->dir->dentries.end();
		 it++) {
	  dout(1) << "    dn " << it->first << " ref " << it->second->ref << endl;
	  dump_inode(it->second->inode, did);
	}
  }
}

void Client::dump_cache()
{
  set<Inode*> did;

  if (root) dump_inode(root, did);

  for (hash_map<inodeno_t, Inode*>::iterator it = inode_map.begin();
	   it != inode_map.end();
	   it++) {
	if (did.count(it->second)) continue;
	
	dout(1) << "inode " << it->first << " ref " << it->second->ref << " dir " << it->second->dir << endl;
	if (it->second->dir) {
	  dout(1) << "  dir size " << it->second->dir->dentries.size() << endl;
	}
  }
 
}


void Client::init() {
  
}

void Client::shutdown() {
  dout(1) << "shutdown" << endl;
  messenger->shutdown();
}




// ===================
// metadata cache stuff

void Client::trim_cache()
{
  while (lru.lru_get_size() > lru.lru_get_max()) {
	Dentry *dn = (Dentry*)lru.lru_expire();
	if (!dn) break;  // done

	//dout(10) << "unlinking dn " << dn->name << " in dir " << dn->dir->inode->inode.ino << endl;
	unlink(dn);
  }
}

// insert inode info into metadata cache

Inode* Client::insert_inode_info(Dir *dir, c_inode_info *in_info)
{
  string dname = in_info->ref_dn;
  Dentry *dn = NULL;
  if (dir->dentries.count(dname))
	dn = dir->dentries[dname];
  dout(12) << "insert_inode_info " << dname << " ino " << in_info->inode.ino << "  size " << in_info->inode.size << endl;
  
  if (dn) {
	if (dn->inode->inode.ino == in_info->inode.ino) {
	  touch_dn(dn);
	  dout(12) << " had dentry " << dname << " with correct ino " << dn->inode->inode.ino << endl;
	} else {
	  dout(12) << " had dentry " << dname << " with WRONG ino " << dn->inode->inode.ino << endl;
	  unlink(dn);
	  dn = NULL;
	}
  }
  
  if (!dn) {
	// have inode linked elsewhere?  -> unlink and relink!
	if (inode_map.count(in_info->inode.ino)) {
	  Inode *in = inode_map[in_info->inode.ino];
	  assert(in);

	  if (in->dn) {
		dout(12) << " had ino " << in->inode.ino << " linked at wrong position, unlinking" << endl;
		dn = relink(in->dn, dir, dname);
	  } else {
		// link
		dout(12) << " had ino " << in->inode.ino << " unlinked, linking" << endl;
		dn = link(dir, dname, in);
	  }
	}
  }
  
  if (!dn) {
	Inode *in = new Inode;
	inode_map[in_info->inode.ino] = in;
	dn = link(dir, dname, in);
	dout(12) << " new dentry+node with ino " << in_info->inode.ino << endl;
  }

  // OK, we found it!
  assert(dn && dn->inode);

  // actually update info
  dn->inode->inode = in_info->inode;
  
  // or do we have newer size/mtime from writing?
  if (dn->inode->file_caps & CAP_FILE_WR) {
	if (dn->inode->file_wr_size > dn->inode->inode.size)
	  dn->inode->inode.size = dn->inode->file_wr_size;
	if (dn->inode->file_wr_mtime > dn->inode->inode.mtime)
	  dn->inode->inode.mtime = dn->inode->file_wr_mtime;
  }

  // symlink?
  if ((dn->inode->inode.mode & INODE_TYPE_MASK) == INODE_MODE_SYMLINK) {
	if (!dn->inode->symlink) 
	  dn->inode->symlink = new string;
	*(dn->inode->symlink) = in_info->symlink;
  }

  // take note of latest distribution on mds's
  if (in_info->spec_defined) {
	if (in_info->dist.empty() && !dn->inode->mds_contacts.empty()) {
	  dout(9) << "lost dist spec for " << dn->inode->inode.ino << " " << in_info->dist << endl;
	}
	if (!in_info->dist.empty() && dn->inode->mds_contacts.empty()) {
	  dout(9) << "got dist spec for " << dn->inode->inode.ino << " " << in_info->dist << endl;
	}
	dn->inode->mds_contacts = in_info->dist;
	dn->inode->mds_dir_auth = in_info->dir_auth;
  }

  return dn->inode;
}

// insert trace of reply into metadata cache

void Client::insert_trace(const vector<c_inode_info*>& trace)
{
  Inode *cur = root;
  time_t now = time(NULL);

  if (trace.empty()) return;
  
  for (unsigned i=0; i<trace.size(); i++) {
    if (i == 0) {
      if (!root) {
        cur = root = new Inode();
        root->inode = trace[i]->inode;
        inode_map[root->inode.ino] = root;
      }
	  if (trace[i]->spec_defined) {
		root->mds_contacts = trace[i]->dist;
		root->mds_dir_auth = trace[i]->dir_auth;
	  }
	  root->last_updated = now;
      dout(12) << "insert_trace trace " << i << " root" << endl;
    } else {
      dout(12) << "insert_trace trace " << i << endl;
      Dir *dir = cur->open_dir();
	  cur = this->insert_inode_info(dir, trace[i]);
	  cur->last_updated = now;

	  // move to top of lru!
	  if (cur->dn) lru.lru_touch(cur->dn);

    }    
  }
}




Dentry *Client::lookup(filepath& path)
{
  dout(14) << "lookup " << path << endl;

  Inode *cur = root;
  if (!cur) return NULL;

  Dentry *dn = 0;
  for (unsigned i=0; i<path.depth(); i++) {
	dout(14) << " seg " << i << " = " << path[i] << endl;
	if (cur->inode.mode & INODE_MODE_DIR &&
		cur->dir) {
	  // dir, we can descend
	  Dir *dir = cur->dir;
	  if (dir->dentries.count(path[i])) {
		dn = dir->dentries[path[i]];
		dout(14) << " hit dentry " << path[i] << " inode is " << dn->inode << " last_updated " << dn->inode->last_updated<< endl;
	  } else {
		dout(14) << " dentry " << path[i] << " dne" << endl;
		return NULL;
	  }
	  cur = dn->inode;
	  assert(cur);
	} else {
	  return NULL;  // not a dir
	}
  }
  
  if (dn) {
	dout(11) << "lookup '" << path << "' found " << dn->name << " inode " << dn->inode->inode.ino << " last_updated " << dn->inode->last_updated<< endl;
  }

  return dn;
}

// -------

MClientReply *Client::make_request(MClientRequest *req, 
								   bool auth_best, 
								   int use_auth)  // this param is icky!
{
  // send to what MDS?  find deepest known prefix
  Inode *cur = root;
  for (unsigned i=0; i<req->get_filepath().depth(); i++) {
	if (cur && cur->inode.mode & INODE_MODE_DIR && cur->dir) {
	  Dir *dir = cur->dir;
	  if (dir->dentries.count( req->get_filepath()[i] ) == 0) 
		break;
	  
	  dout(7) << " have path seg " << i << " on " << cur->mds_dir_auth << " ino " << cur->inode.ino << " " << req->get_filepath()[i] << endl;
	  cur = dir->dentries[ req->get_filepath()[i] ]->inode;
	  assert(cur);
	} else
	  break;
  }
  
  int mds = 0;
  if (cur) {
	if (!auth_best && cur->get_replicas().size()) {
	  // try replica(s)
	  dout(9) << "contacting replica from deepest inode " << cur->inode.ino << " " << req->get_filepath() << ": " << cur->get_replicas() << endl;
	  set<int>::iterator it = cur->get_replicas().begin();
	  if (cur->get_replicas().size() == 1)
		mds = *it;
	  else {
		int r = rand() % cur->get_replicas().size();
		while (r--) it++;
		mds = *it;
	  }
	} else {
	  // try auth
	  mds = cur->authority();
	  //if (!auth_best && req->get_filepath().get_path()[0] == '/')
		dout(9) << "contacting auth mds " << mds << " auth_best " << auth_best << " for " << req->get_filepath() << endl;
	}
  } else {
	dout(9) << "i have no idea where " << req->get_filepath() << " is" << endl;
  }

  // force use of a particular mds auth?
  if (use_auth >= 0)
	mds = use_auth;

  // drop mutex for duration of call
  client_lock.Unlock();  
  utime_t start = g_clock.now();

  
  bool nojournal = false;
  if (req->get_op() == MDS_OP_STAT ||
	  req->get_op() == MDS_OP_LSTAT ||
	  req->get_op() == MDS_OP_READDIR ||
	  req->get_op() == MDS_OP_OPEN ||    // not quite true!  a lie actually!
	  req->get_op() == MDS_OP_RELEASE)
	nojournal = true;

  MClientReply *reply = (MClientReply*)messenger->sendrecv(req,
														   MSG_ADDR_MDS(mds), 
														   MDS_PORT_SERVER);


  if (client_logger) {
	utime_t lat = g_clock.now();
	lat -= start;
	client_logger->finc("lsum",(double)lat);
	client_logger->inc("lnum");

	if (nojournal) {
	  client_logger->finc("lrsum",(double)lat);
	  client_logger->inc("lrnum");
	} else {
	  client_logger->finc("lwsum",(double)lat);
	  client_logger->inc("lwnum");
	}
  }

  client_lock.Lock();  
  return reply;
}



// ------------------------
// incoming messages

void Client::dispatch(Message *m)
{
  client_lock.Lock();

  switch (m->get_type()) {
	// osd
  case MSG_OSD_OPREPLY:
	filer->handle_osd_op_reply((MOSDOpReply*)m);
	break;
	
	// client
  case MSG_CLIENT_FILECAPS:
	handle_file_caps((MClientFileCaps*)m);
	break;

  default:
	cout << "dispatch doesn't recognize message type " << m->get_type() << endl;
	assert(0);  // fail loudly
	break;
  }

  client_lock.Unlock();
}

/*
 * flush inode (write cached) buffers to disk
 */
class C_Client_FileFlushFinish : public Context {
public:
  Bufferhead *bh;
  C_Client_FileFlushFinish(Bufferhead *bh) {
	this->bh = bh;
  }
  void finish(int r) {
    bh->flush_finish();
    if (bh->fc->inflight_buffers.empty()) 
	  bh->fc->wakeup_inflight_waiters();
  }
};


void Client::flush_inode_buffers(Inode *in)
{
  if (!in->inflight_buffers.empty()) {
    dout(7) << "inflight buffers of sync write, waiting" << endl;
    Cond cond;
    in->waitfor_flushed.push_back(&cond);
    cond.Wait(client_lock);
    assert(in->inflight_buffers.empty());
    dout(7) << "inflight buffers flushed" << endl;
  } 
  else if (g_conf.client_bcache &&
		   !bc.get_fc(in->inode.ino)->dirty_buffers.empty()) {
    Filecache *fc = bc.get_fc(in->inode.ino);
    dout(7) << "bc: flush_inode_buffers: inode " << in->inode.ino << " has " << fc->dirty_buffers.size() << " dirty buffers" << endl;
    //fc->simplify();
    dout(10) << "bc: flush_inode_buffers: after simplify: inode " << in->inode.ino << " has " << fc->dirty_buffers.size() << " dirty buffers" << endl;
    set<Bufferhead*> to_flush = fc->dirty_buffers;
    for (set<Bufferhead*>::iterator it = to_flush.begin();
         it != to_flush.end();
         it++) {
      (*it)->flush_start(); // Note: invalidates dirty_buffer entries!!!
      C_Client_FileFlushFinish *onfinish = new C_Client_FileFlushFinish(*it);
      filer->write(in->inode.ino, g_OSD_FileLayout, (*it)->bl.length(), (*it)->offset, (*it)->bl, 0, onfinish);
    }
    dout(7) << "flush_inode_buffers: dirty buffers, waiting" << endl;
    fc->wait_for_inflight(client_lock);
  } 
  else {
	dout(7) << "no inflight buffers" << endl;
  }
}

class C_Client_FlushFinish : public Context {
public:
  Bufferhead *bh;
  C_Client_FlushFinish(Bufferhead *bh) {
    this->bh = bh;
  }
  void finish(int r) {
    bh->flush_finish();
    if (bh->bc->inflight_buffers.empty()) 
	  bh->bc->wakeup_inflight_waiters();
  }
};

void Client::flush_buffers(int ttl, int dirty_size)
{
  // ttl = 0 or dirty_size = 0: flush all
  if (!bc.dirty_buffers.empty()) {
    dout(6) << "bc: flush_buffers ttl: " << ttl << " dirty_size: " << dirty_size << endl;
    set<Bufferhead*> expired;
    bc.dirty_buffers.get_expired(ttl, dirty_size, expired);
    assert(!expired.empty());
    for (set<Bufferhead*>::iterator it = expired.begin();
	 it != expired.end();
	 it++) {
      (*it)->flush_start();
      C_Client_FlushFinish *onfinish = new C_Client_FlushFinish(*it);
      filer->write((*it)->ino, g_OSD_FileLayout, (*it)->bl.length(), (*it)->offset, (*it)->bl, 0, onfinish);
    }
    dout(7) << "flush_buffers: dirty buffers, waiting" << endl;
    assert(!bc.inflight_buffers.empty());
    bc.wait_for_inflight(client_lock);
  } else {
	dout(7) << "no dirty buffers" << endl;
  }
}

void Client::trim_bcache()
{
  if (bc.get_total_size() > (unsigned) g_conf.client_bcache_size) {
    // need to free buffers 
    if (bc.get_dirty_size() > (unsigned)g_conf.client_bcache_hiwater * (unsigned)g_conf.client_bcache_size / 100UL) {
      // flush buffers until we have low water mark
      int want_target_size = g_conf.client_bcache_lowater * g_conf.client_bcache_size / 100;
      flush_buffers(g_conf.client_bcache_ttl, want_target_size);
    }
    // Now reclaim buffers
    dout(6) << "bc: trim_bcache: reclaim: " << bc.get_total_size() - g_conf.client_bcache_size * g_conf.client_bcache_hiwater / 100 << endl;
    bc.reclaim(bc.get_total_size() - g_conf.client_bcache_size * g_conf.client_bcache_hiwater / 100);
  }
}
      
                                                                                  

/*
 * release inode (read cached) buffers from memory
 */
void Client::release_inode_buffers(Inode *in)
{
  if (g_conf.client_bcache) {
	// Check first we actually cached the file
	if (bc.bcache_map.count(in->inode.ino)) 
	  bc.release_file(in->inode.ino);
  }
}


void Client::handle_file_caps(MClientFileCaps *m)
{
  Inode *in = inode_map[ m->get_ino() ];
  assert(in);

  // auth?
  if (m->get_mds() >= 0) {
	in->file_mds = m->get_mds();
	dout(5) << "handle_file_caps on in " << m->get_ino() << " mds now " << in->file_mds << endl;
  }
	
  in->file_caps = m->get_caps();
  dout(5) << "handle_file_caps on in " << m->get_ino() << " caps now " << in->file_caps << endl;
	
  // update inode
  in->inode = m->get_inode();      // might have updated size... FIXME this is overkill!
	
  // flush buffers?
  if (in->file_caps & CAP_FILE_WRBUFFER == 0)
	flush_inode_buffers(in);

  // release buffers?
  if (in->file_caps & CAP_FILE_RDCACHE == 0)
	release_inode_buffers(in);
  
  // ack
  if (m->needs_ack()) {
	dout(5) << "acking" << endl;
	messenger->send_message(m, m->get_source(), m->get_source_port());
	return;
  } 

  // wake up waiters?
  if (in->file_caps & CAP_FILE_RD) {
	for (list<Cond*>::iterator it = in->waitfor_read.begin();
		 it != in->waitfor_read.end();
		 it++) {
	  dout(5) << "signaling read waiter " << *it << endl;
	  (*it)->Signal();
	}
	in->waitfor_read.clear();
  }
  if (in->file_caps & CAP_FILE_WR) {
	for (list<Cond*>::iterator it = in->waitfor_write.begin();
		 it != in->waitfor_write.end();
		 it++) {
	  dout(5) << "signaling write waiter " << *it << endl;
	  (*it)->Signal();
	}
	in->waitfor_write.clear();
  }

  delete m;
}




// -------------------
// fs ops

int Client::mount(int mkfs)
{
  client_lock.Lock();

  assert(!mounted);  // caller is confused?

  dout(2) << "mounting" << endl;
  MClientMount *m = new MClientMount();
  if (mkfs) m->set_mkfs(mkfs);

  client_lock.Unlock();
  MClientMountAck *reply = (MClientMountAck*)messenger->sendrecv(m, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  client_lock.Lock();
  assert(reply);

  // we got osdcluster!
  osdcluster->decode(reply->get_osd_cluster_state());

  dout(2) << "mounted" << endl;
  mounted = true;

  delete reply;

  client_lock.Unlock();

  /*
  dout(3) << "op: // client trace data structs" << endl;
  dout(3) << "op: struct stat st;" << endl;
  dout(3) << "op: struct utimbuf utim;" << endl;
  dout(3) << "op: int readlinkbuf_len = 1000;" << endl;
  dout(3) << "op: char readlinkbuf[readlinkbuf_len];" << endl;
  dout(3) << "op: map<string, inode_t*> dir_contents;" << endl;
  dout(3) << "op: map<fh_t, fh_t> open_files;" << endl;
  dout(3) << "op: fh_t fh;" << endl;
  */
  return 0;
}

int Client::unmount()
{
  client_lock.Lock();

  assert(mounted);  // caller is confused?

  dout(2) << "unmounting" << endl;
  Message *req = new MGenericMessage(MSG_CLIENT_UNMOUNT);
  client_lock.Unlock();
  Message *reply = messenger->sendrecv(req, MSG_ADDR_MDS(0), MDS_PORT_SERVER);
  client_lock.Lock();
  assert(reply);
  mounted = false;
  dout(2) << "unmounted" << endl;

  delete reply;

  client_lock.Unlock();
  return 0;
}



// namespace ops

int Client::link(const char *existing, const char *newname) 
{
  client_lock.Lock();
  dout(3) << "op: client->link(\"" << existing << "\", \"" << newname << "\");" << endl;
  tout << "link" << endl;
  tout << existing << endl;
  tout << newname << endl;


  // main path arg is new link name
  // sarg is target (existing file)


  MClientRequest *req = new MClientRequest(MDS_OP_LINK, whoami);
  req->set_path(newname);
  req->set_sarg(existing);
  
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  
  this->insert_trace(reply->get_trace());
  delete reply;
  dout(10) << "link result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}


int Client::unlink(const char *path)
{
  client_lock.Lock();
  dout(3) << "op: client->unlink\(\"" << path << "\");" << endl;
  tout << "unlink" << endl;
  tout << path << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_UNLINK, whoami);
  req->set_path(path);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  if (res == 0) {
	// remove from local cache
	filepath fp(path);
	Dentry *dn = lookup(fp);
	if (dn) unlink(dn);
  }
  this->insert_trace(reply->get_trace());
  delete reply;
  dout(10) << "unlink result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::rename(const char *from, const char *to)
{
  client_lock.Lock();
  dout(3) << "op: client->rename(\"" << from << "\", \"" << to << "\");" << endl;
  tout << "rename" << endl;
  tout << from << endl;
  tout << to << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_RENAME, whoami);
  req->set_path(from);
  req->set_sarg(to);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());
  delete reply;
  dout(10) << "rename result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}

// dirs

int Client::mkdir(const char *path, mode_t mode)
{
  client_lock.Lock();
  dout(3) << "op: client->mkdir(\"" << path << "\", " << mode << ");" << endl;
  tout << "mkdir" << endl;
  tout << path << endl;
  tout << mode << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_MKDIR, whoami);
  req->set_path(path);
  req->set_iarg( (int)mode );
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());
  delete reply;
  dout(10) << "mkdir result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::rmdir(const char *path)
{
  client_lock.Lock();
  dout(3) << "op: client->rmdir(\"" << path << "\");" << endl;
  tout << "rmdir" << endl;
  tout << path << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_RMDIR, whoami);
  req->set_path(path);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  if (res == 0) {
	// remove from local cache
	filepath fp(path);
	Dentry *dn = lookup(fp);
	if (dn) {
	  if (dn->inode->dir && dn->inode->dir->is_empty()) 
		close_dir(dn->inode->dir);  // FIXME: maybe i shoudl proactively hose the whole subtree from cache?
	  unlink(dn);
	}
  }
  this->insert_trace(reply->get_trace());  
  delete reply;
  dout(10) << "rmdir result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}

// symlinks
  
int Client::symlink(const char *target, const char *link)
{
  client_lock.Lock();
  dout(3) << "op: client->symlink(\"" << target << "\", \"" << link << "\");" << endl;
  tout << "symlink" << endl;
  tout << target << endl;
  tout << link << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_SYMLINK, whoami);
  req->set_path(link);
  req->set_sarg(target);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  //FIXME assuming trace of link, not of target
  delete reply;
  dout(10) << "symlink result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::readlink(const char *path, char *buf, size_t size) 
{ 
  client_lock.Lock();
  dout(3) << "op: client->readlink(\"" << path << "\", readlinkbuf, readlinkbuf_len);" << endl;
  tout << "readlink" << endl;
  tout << path << endl;
  client_lock.Unlock();

  // stat first  (FIXME, PERF access cache directly) ****
  struct stat stbuf;
  int r = this->lstat(path, &stbuf);
  if (r != 0) return r;

  client_lock.Lock();

  // pull symlink content from cache
  Inode *in = inode_map[stbuf.st_ino];
  assert(in);  // i just did a stat
  
  // copy into buf (at most size bytes)
  unsigned res = in->symlink->length();
  if (res > size) res = size;
  memcpy(buf, in->symlink->c_str(), res);

  trim_cache();
  client_lock.Unlock();
  return res;  // return length in bytes (to mimic the system call)
}



// inode stuff

int Client::lstat(const char *path, struct stat *stbuf)
{
  client_lock.Lock();
  dout(3) << "op: client->lstat(\"" << path << "\", &st);" << endl;
  tout << "lstat" << endl;
  tout << path << endl;


  // FIXME, PERF request allocation convenient but not necessary for cache hit
  MClientRequest *req = new MClientRequest(MDS_OP_STAT, whoami);
  req->set_path(path);
  
  // check whether cache content is fresh enough
  int res = 0;
  Dentry *dn = lookup(req->get_filepath());
  inode_t inode;
  time_t now = time(NULL);
  if (dn && 
	  ((now - dn->inode->last_updated) < g_conf.client_cache_stat_ttl)) {
	inode = dn->inode->inode;
	dout(10) << "lstat cache hit, age is " << (now - dn->inode->last_updated) << endl;
	delete req;  // don't need this
  } else {  
	// FIXME where does FUSE maintain user information
	req->set_caller_uid(getuid());
	req->set_caller_gid(getgid());
	
	MClientReply *reply = make_request(req);
	res = reply->get_result();
	dout(10) << "lstat res is " << res << endl;
	if (res == 0) {
	  //Transfer information from reply to stbuf
	  vector<c_inode_info*> trace = reply->get_trace();
	  inode = trace[trace.size()-1]->inode;
	  
	  //Update metadata cache
	  this->insert_trace(trace);
	}

	delete reply;
  }
     
  if (res == 0) {
	memset(stbuf, 0, sizeof(struct stat));
	//stbuf->st_dev = 
	stbuf->st_ino = inode.ino;
	stbuf->st_mode = inode.mode;
	stbuf->st_nlink = inode.nlink;
	stbuf->st_uid = inode.uid;
	stbuf->st_gid = inode.gid;
	stbuf->st_ctime = inode.ctime;
	stbuf->st_atime = inode.atime;
	stbuf->st_mtime = inode.mtime;
	stbuf->st_size = (off_t) inode.size; //FIXME off_t is signed 64 vs size is unsigned 64
	stbuf->st_blocks = (inode.size - 1) / 1024 + 1;
	stbuf->st_blksize = 1024;
	//stbuf->st_flags =
	//stbuf->st_gen =
	
	dout(10) << "stat sez size = " << inode.size << "   uid = " << inode.uid << " ino = " << stbuf->st_ino << endl;
  }

  trim_cache();
  client_lock.Unlock();
  return res;
}



int Client::chmod(const char *path, mode_t mode)
{
  client_lock.Lock();
  dout(3) << "op: client->chmod(\"" << path << "\", " << mode << ");" << endl;
  tout << "chmod" << endl;
  tout << path << endl;
  tout << mode << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_CHMOD, whoami);
  req->set_path(path); 
  req->set_iarg( (int)mode );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  
  delete reply;
  dout(10) << "chmod result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::chown(const char *path, uid_t uid, gid_t gid)
{
  client_lock.Lock();
  dout(3) << "op: client->chown(\"" << path << "\", " << uid << ", " << gid << ");" << endl;
  tout << "chown" << endl;
  tout << path << endl;
  tout << uid << endl;
  tout << gid << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_CHOWN, whoami);
  req->set_path(path); 
  req->set_iarg( (int)uid );
  req->set_iarg2( (int)gid );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?

  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  
  delete reply;
  dout(10) << "chown result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::utime(const char *path, struct utimbuf *buf)
{
  client_lock.Lock();
  dout(3) << "op: utim.actime = " << buf->actime << "; utim.modtime = " << buf->modtime << ";" << endl;
  dout(3) << "op: client->utime(\"" << path << "\", &utim);" << endl;
  tout << "utime" << endl;
  tout << path << endl;
  tout << buf->actime << endl;
  tout << buf->modtime << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_UTIME, whoami);
  req->set_path(path); 
  req->set_targ( buf->modtime );
  req->set_targ2( buf->actime );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  
  delete reply;
  dout(10) << "utime result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}



int Client::mknod(const char *path, mode_t mode) 
{ 
  client_lock.Lock();
  dout(3) << "op: client->mknod(\"" << path << "\", " << mode << ");" << endl;
  tout << "mknod" << endl;
  tout << path << endl;
  tout << mode << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_MKNOD, whoami);
  req->set_path(path); 
  req->set_iarg( mode );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  

  dout(10) << "mknod result is " << res << endl;

  delete reply;

  trim_cache();
  client_lock.Unlock();
  return res;
}



  
//readdir usually include inode info for each entry except of locked entries

//
// getdir

// fyi: typedef int (*dirfillerfunc_t) (void *handle, const char *name, int type, inodeno_t ino);

int Client::getdir(const char *path, map<string,inode_t*>& contents) 
{
  client_lock.Lock();
  dout(3) << "op: client->getdir(\"" << path << "\", dir_contents);" << endl;
  tout << "getdir" << endl;
  tout << path << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_READDIR, whoami);
  req->set_path(path); 

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  vector<c_inode_info*> trace = reply->get_trace();
  this->insert_trace(trace);  

  if (res == 0) {

	// dir contents to cache!
	inodeno_t ino = trace[trace.size()-1]->inode.ino;
	Inode *diri = inode_map[ ino ];
	assert(diri);
	assert(diri->inode.mode & INODE_MODE_DIR);

	if (reply->get_dir_contents().size()) {
	  // only open dir if we're actually adding stuff to it!
	  Dir *dir = diri->open_dir();
	  assert(dir);
	  time_t now = time(NULL);
	  for (vector<c_inode_info*>::iterator it = reply->get_dir_contents().begin(); 
		   it != reply->get_dir_contents().end(); 
		   it++) {
		// put in cache
		Inode *in = this->insert_inode_info(dir, *it);
		in->last_updated = now;
		
		// contents to caller too!
		contents[(*it)->ref_dn] = &in->inode;
	  }
	}

	// FIXME: remove items in cache that weren't in my readdir
	// ***
  }

  delete reply;     //fix thing above first

  client_lock.Unlock();
  return res;
}




/****** file i/o **********/

int Client::open(const char *path, int mode) 
{
  client_lock.Lock();
  dout(3) << "op: fh = client->open(\"" << path << "\", " << mode << ");" << endl;
  tout << "open" << endl;
  tout << path << endl;
  tout << mode << endl;

  int cmode = 0;
  if (mode & O_WRONLY) 
	cmode = FILE_MODE_W;
  else if (mode & O_RDWR) 
	cmode = FILE_MODE_RW;
  else if (mode & O_APPEND)
	cmode = FILE_MODE_W;
  else
	cmode = FILE_MODE_R;

  // go
  MClientRequest *req = new MClientRequest(MDS_OP_OPEN, whoami);
  req->set_path(path); 
  req->set_iarg(mode);

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  MClientReply *reply = make_request(req, true);
  assert(reply);
  dout(3) << "op: open_files[" << reply->get_result() << "] = fh;  // fh = " << reply->get_result() << endl;
  tout << reply->get_result() << endl;

  vector<c_inode_info*> trace = reply->get_trace();
  this->insert_trace(trace);  
  int result = reply->get_result();

  // success?
  fh_t fh = 0;
  if (result >= 0) {
	// yay
	Fh *f = new Fh;
	memset(f, 0, sizeof(*f));
	f->mode = cmode;

	// inode
	f->inode = inode_map[trace[trace.size()-1]->inode.ino];
	assert(f->inode);
	f->inode->get();
	f->inode->file_mds = reply->get_source();

	if (cmode & FILE_MODE_R) 	
	  f->inode->num_rd++;
	if (cmode & FILE_MODE_W)
	  f->inode->num_wr++;

	// caps
	if (f->inode->file_caps_seq == 0)
	  f->inode->get();
	f->inode->file_caps = reply->get_file_caps();
	assert(f->inode->file_caps_seq < reply->get_file_caps_seq());   // ordered delivery
	f->inode->file_caps_seq = reply->get_file_caps_seq();
	
	// put in map
	result = fh = get_fh();
	assert(fh_map.count(fh) == 0);
	fh_map[fh] = f;
	
	dout(3) << "open success, fh is " << fh << " caps " << f->inode->file_caps << endl;//f->caps << "  fh size " << f->size << endl;
  }

  delete reply;

  trim_cache();
  client_lock.Unlock();

  return result;
}

int Client::close(fh_t fh)
{
  client_lock.Lock();
  dout(3) << "op: client->close(open_files[ " << fh << " ]);" << endl;
  dout(3) << "op: open_files.erase( " << fh << " );" << endl;
  tout << "close" << endl;
  tout << fh << endl;

  // get Fh, Inode
  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;

  // Make sure buffers are all clean!
  //flush_inode_buffers(in);

  // update inode
  if (f->mode & FILE_MODE_R) 	
	in->num_rd--;
  if (f->mode & FILE_MODE_W)
	in->num_wr--;

  // hose fh
  fh_map.erase(fh);
  delete f;

  // note mds auth.. we'll send the close there!  FIXME this is sort of icky
  int mds_auth = in->authority();

  //release_inode_buffers(in);
  put_inode( in );
  int result = 0;

  // release caps right away?
  if (in->num_rd == 0 &&
	  in->num_wr == 0) {
	// synchronously; FIXME this is dumb

	MClientRequest *req = new MClientRequest(MDS_OP_RELEASE, whoami);
	req->set_ino(in->inode.ino);
	
	req->set_iarg( in->file_caps_seq );
	req->set_targ( in->file_wr_mtime );
	req->set_sizearg( in->file_wr_size );
	
	// FIXME where does FUSE maintain user information
	req->set_caller_uid(getuid());
	req->set_caller_gid(getgid());

	MClientReply *reply = make_request(req, true, mds_auth);
	assert(reply);
	int result = reply->get_result();
	assert(result == 0);

	// success?
	if (in->file_caps_seq == reply->get_file_caps_seq()) {
	  // yup.
	  dout(5) << "successfully released caps" << endl;
	  in->file_caps_seq = 0;
	  in->file_caps = 0;
	  in->file_wr_mtime = 0;
	  in->file_wr_size = 0;
	  put_inode(in);
	} else {
	  dout(5) << "failed to release caps; i had " << in->file_caps_seq << " mds had " << reply->get_file_caps_seq() << endl;
	}
  
	delete reply;
  }

  client_lock.Unlock();
  return result;
}



// ------------
// read, write

// ------------------------
// blocking osd interface

class C_Client_Cond : public Context {
public:
  Cond *cond;
  Mutex *mutex;
  int *rvalue;
  bool finished;
  C_Client_Cond(Cond *cond, Mutex *mutex, int *rvalue) {
    this->cond = cond;
    this->mutex = mutex;
    this->rvalue = rvalue;
    this->finished = false;
  }
  void finish(int r) {
    //mutex->Lock();
    *rvalue = r;
    finished = true;
    cond->Signal();
    //mutex->Unlock();
  }
};

class C_Client_MissFinish : public Context {
public:
  Bufferhead *bh;
  Mutex *mutex;
  int *rvalue;
  bool finished;
  C_Client_MissFinish(Bufferhead *bh, Mutex *mutex, int *rvalue) { 
	this->bh = bh; 
	this->mutex = mutex;
	this->rvalue = rvalue;
	this->finished = false;
  }
  void finish(int r) {
	//mutex->Lock();
	*rvalue += r;
	finished = true;
    bh->miss_finish();
	//mutex->Unlock();
  }
};


int Client::read(fh_t fh, char *buf, size_t size, off_t offset) 
{
  client_lock.Lock();

  dout(3) << "op: client->read(" << fh << ", buf, " << size << ", " << offset << ");" << endl;
  tout << "read" << endl;
  tout << fh << endl;
  tout << size << endl;
  tout << offset << endl;

  assert(offset >= 0);
  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;
  
  // do we have read file cap?
  while (in->file_caps & CAP_FILE_RD == 0) {
	dout(7) << " don't have read cap, waiting" << endl;
	Cond cond;
	in->waitfor_read.push_back(&cond);
	cond.Wait(client_lock);
  }
  
  
  // determine whether read range overlaps with file
  // ...ONLY if we're doing async io
  if (in->file_caps & (CAP_FILE_WRBUFFER|CAP_FILE_RDCACHE)) {
	// we're doing buffered i/o.  make sure we're inside the file.
	// we can trust size info bc we get accurate info when buffering/caching caps are issued.
	dout(10) << "file size: " << in->inode.size << endl;
	if (offset > 0 && (size_t)offset >= in->inode.size) {
	  client_lock.Unlock();
	  return 0;
	}
	if (size > in->inode.size) size = in->inode.size;
	
	if (size == 0) {
	  dout(10) << "read is size=0, returning 0" << endl;
	  client_lock.Unlock();
	  return 0;
	}
  } else {
	// unbuffered, synchronous file i/o.  
	// defer to OSDs for file bounds.
  }
  
  int rvalue = 0;

  if (!g_conf.client_bcache) {
	// buffer cache OFF
    Cond cond;
    bufferlist blist;   // data will go here

    C_Client_Cond *onfinish = new C_Client_Cond(&cond, &client_lock, &rvalue);
    filer->read(in->inode.ino, g_OSD_FileLayout, size, offset, &blist, onfinish);
    cond.Wait(client_lock);

    // copy data into caller's buf
    blist.copy(0, blist.length(), buf);
  }
  else {
	// buffer cache ON

	// map buffercache 
	map<off_t, Bufferhead*> hits, rx, tx;
	map<off_t, size_t> holes;
	map<off_t, size_t>::iterator hole;
	
	Filecache *fc = bc.get_fc(in->inode.ino);
	hits.clear(); rx.clear(); tx.clear(); holes.clear();
	fc->map_existing(size, offset, hits, rx, tx, holes);  
	
	if (hits.count(offset)) {
	  // sweet -- we can return stuff immediately: find out how much
	  dout(6) << "read bc hit" << endl;
	  rvalue = (int)bc.touch_continuous(hits, size, offset);
	  assert(rvalue > 0);
	  rvalue = fc->copy_out((size_t)rvalue, offset, buf);
	  assert(rvalue > 0);
	  dout(7) << "read bc hit: immediately returning " << rvalue << " bytes" << endl;
	}
	assert(!(rvalue >= 0 && (size_t)rvalue == size) || holes.empty());
	
	// issue reads for holes
	int hole_rvalue = 0; //FIXME: don't really need to track rvalue in MissFinish context
	for (hole = holes.begin(); hole != holes.end(); hole++) {
	  dout(6) << "read bc miss" << endl;
	  off_t hole_offset = hole->first;
	  size_t hole_size = hole->second;
	  assert(fc->buffer_map.count(hole_offset) == 0);
	  
	  // insert new bufferhead without allocating buffers (Filer::handle_osd_read_reply allocates them)
	  Bufferhead *bh = new Bufferhead(in->inode.ino, hole_offset, &bc);
	  
	  // read into the buffercache: when finished transition state from
	  // rx to clean
	  bh->miss_start(hole_size);
	  C_Client_MissFinish *onfinish = new C_Client_MissFinish(bh, &client_lock, &hole_rvalue);	
	  filer->read(in->inode.ino, g_OSD_FileLayout, hole_size, hole_offset, &(bh->bl), onfinish);
	  dout(6) << "read bc miss: issued osd read len: " << hole_size << " off: " << hole_offset << endl;
	}
	
	if (rvalue == 0) {
	  // we need to wait for the first buffer
	  dout(7) << "read bc miss: waiting for first buffer" << endl;
	  map<off_t, Bufferhead*>::iterator it = fc->buffer_map.lower_bound(offset);
	  if (it == fc->buffer_map.end() || it->first > offset) {
	    assert(it != fc->buffer_map.begin());
	    it--;
	  }
	  Bufferhead *bh = it->second;
	  assert(bh->offset + bh->length() > offset);
#if 0
	  Bufferhead *bh;
	  if (curbuf == fc->buffer_map.end() && fc->buffer_map.count(offset)) {
		dout(10) << "first buffer is currently read in" << endl;
		bh = fc->buffer_map[offset];
	  } else {
		dout(10) << "first buffer is either hit or inflight" << endl;
		bh = curbuf->second;  
	  }
#endif
	  if (bh->state == BUFHD_STATE_RX || bh->state == BUFHD_STATE_TX) {
		dout(10) << "waiting for first buffer" << endl;
		bh->wait_for_read(client_lock);
	  }
	  
	  // buffer is filled -- see how much we can return
	  hits.clear(); rx.clear(); tx.clear(); holes.clear();
	  fc->map_existing(size, offset, hits, rx, tx, holes); // FIXME: overkill
	  //assert(hits.count(offset));
	  rvalue = (int)bc.touch_continuous(hits, size, offset);
	  fc->copy_out(rvalue, offset, buf);
	  dout(7) << "read bc no hit: returned first " << rvalue << " bytes" << endl;
	  
	  trim_bcache();
	}
  }

  // done!
  client_lock.Unlock();
  return rvalue;
}

// hack.. see async write() below
class C_Client_WriteBuffer : public Context {
public:
  Inode *in;
  bufferlist *blist;
  C_Client_WriteBuffer(Inode *in, bufferlist *blist) {
        this->in = in;
        this->blist = blist;
  }
  void finish(int r) {
        in->inflight_buffers.erase(blist);
        delete blist;

        if (in->inflight_buffers.empty()) {
          // wake up flush waiters
          for (list<Cond*>::iterator it = in->waitfor_flushed.begin();
                   it != in->waitfor_flushed.end();
                   it++) {
                (*it)->Signal();
          }
          in->waitfor_flushed.clear();
        }
  }
};


int Client::write(fh_t fh, const char *buf, size_t size, off_t offset) 
{
  client_lock.Lock();

  //dout(7) << "write fh " << fh << " size " << size << " offset " << offset << endl;
  dout(3) << "op: client->write(" << fh << ", buf, " << size << ", " << offset << ");" << endl;
  tout << "write" << endl;
  tout << fh << endl;
  tout << size << endl;
  tout << offset << endl;

  assert(offset >= 0);
  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;

  dout(10) << "cur file size is " << in->inode.size << "    wr size " << in->file_wr_size << endl;


  // do we have write file cap?
  while (in->file_caps & CAP_FILE_WR == 0) {
	dout(7) << " don't have write cap, waiting" << endl;
	Cond cond;
	in->waitfor_write.push_back(&cond);
	cond.Wait(client_lock);
  }


  if (g_conf.client_bcache &&	        // buffer cache ON?
	  in->file_caps & CAP_FILE_WRBUFFER) {   // caps buffered write?
	// buffered write
	dout(7) << "buffered/async write" << endl;
	
	// map buffercache for writing
	map<off_t, Bufferhead*> buffers, rx, tx;
	buffers.clear(); rx.clear(); tx.clear();
	bc.map_or_alloc(in->inode.ino, size, offset, buffers, rx, tx); 
	
	// wait for rx and tx buffers -- FIXME: don't need to wait for tx buffers
	while (!(rx.empty() && tx.empty())) {
	  if (!rx.empty()) {
		rx.begin()->second->wait_for_write(client_lock);
	  } else {
		tx.begin()->second->wait_for_write(client_lock);
	  }
	  buffers.clear(); tx.clear(); rx.clear();
	  bc.map_or_alloc(in->inode.ino, size, offset, buffers, rx, tx); // FIXME: overkill
	} 
	bc.dirty(in->inode.ino, size, offset, buf);
	
	trim_bcache();
	
	/*
	  hack for now.. replace this with a real buffer cache
	  
	  just copy the buffer, send the write off, and return immediately.  
	  flush() will block until all outstanding writes complete.
	*/
	/* this totally sucks, just do synchronous writes!
	   bufferlist *blist = new bufferlist;
	   blist->push_back( new buffer(buf, size, BUFFER_MODE_COPY|BUFFER_MODE_FREE) );
	   
	   in->inflight_buffers.insert(blist);
	   
	   Context *onfinish = new C_Client_WriteBuffer( in, blist );
	   filer->write(in->inode.ino, g_OSD_FileLayout, size, offset, *blist, 0, onfinish);
	*/
	
  } else {
	// synchronous write
    // FIXME: do not bypass buffercache
	//if (g_conf.client_bcache) {
	  // write me
	//} else
	{
	  dout(7) << "synchronous write" << endl;
	  
	  // create a buffer that refers to *buf, but doesn't try to free it when it's done.
	  bufferlist blist;
	  blist.push_back( new buffer(buf, size, BUFFER_MODE_NOCOPY|BUFFER_MODE_NOFREE) );
	  
	  // issue write
	  Cond cond;
	  int rvalue;
	  
	  C_Client_Cond *onfinish = new C_Client_Cond(&cond, &client_lock, &rvalue);
	  filer->write(in->inode.ino, g_OSD_FileLayout, size, offset, blist, 0, onfinish);
	  
	  cond.Wait(client_lock);
	}
  }



  // assume success for now.  FIXME.
  size_t totalwritten = size;
  
  // extend file?
  if (totalwritten + (size_t)offset > in->inode.size) {
	in->inode.size = in->file_wr_size = totalwritten + offset;
	dout(7) << "wrote to " << totalwritten+offset << ", extending file size" << endl;
  } else {
	dout(7) << "wrote to " << totalwritten+offset << ", leaving file size at " << in->inode.size << endl;
  }

  // mtime
  in->file_wr_mtime = in->inode.mtime = g_clock.gettime();

  // ok!
  client_lock.Unlock();
  return totalwritten;  
}


int Client::truncate(const char *file, off_t size) 
{
  client_lock.Lock();
  dout(3) << "op: client->truncate(\"" << file << "\", " << size << ");" << endl;
  tout << "truncate" << endl;
  tout << file << endl;
  tout << size << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_TRUNCATE, whoami);
  req->set_path(file); 
  req->set_sizearg( size );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(getuid());
  req->set_caller_gid(getgid());
  
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  this->insert_trace(reply->get_trace());  
  delete reply;

  dout(10) << " truncate result is " << res << endl;

  client_lock.Unlock();
  return res;
}


int Client::fsync(fh_t fh, bool syncdataonly) 
{
  client_lock.Lock();
  dout(3) << "op: client->fsync(open_files[ " << fh << " ], " << syncdataonly << ");" << endl;
  tout << "fsync" << endl;
  tout << fh << endl;
  tout << syncdataonly << endl;

  int r = 0;

  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;

  dout(3) << "fsync fh " << fh << " ino " << in->inode.ino << " syncdataonly " << syncdataonly << endl;
 
  flush_inode_buffers(in);

  if (syncdataonly &&
	  (in->file_caps & CAP_FILE_WR)) {
	// flush metadata too.. size, mtime
	// ...
  }

  client_lock.Unlock();
  return r;
}


// not written yet, but i want to link!

int Client::statfs(const char *path, struct statfs *stbuf) 
{
  assert(0);  // implement me
  return 0;
}
