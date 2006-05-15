// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "osd/Objecter.h"
#include "osd/ObjectCacher.h"

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

Client::Client(Messenger *m)
{
  // which client am i?
  whoami = MSG_ADDR_NUM(m->get_myaddr());

  mounted = false;
  unmounting = false;

  unsafe_sync_write = 0;

  // 
  root = 0;

  set_cache_size(g_conf.client_cache_size);

  // file handles
  free_fh_set.map_insert(10, 1<<30);

  // set up messengers
  messenger = m;
  messenger->set_dispatcher(this);

  // osd interfaces
  osdmap = new OSDMap();     // initially blank.. see mount()
  objecter = new Objecter(messenger, osdmap);
  objectcacher = new ObjectCacher(objecter, client_lock);
  filer = new Filer(objecter);
}


Client::~Client() 
{
  if (messenger) { delete messenger; messenger = 0; }
  if (filer) { delete filer; filer = 0; }
  if (objectcacher) { delete objectcacher; objectcacher = 0; }
  if (objecter) { delete objecter; objecter = 0; }
  if (osdmap) { delete osdmap; osdmap = 0; }

  tear_down_cache();
}


void Client::tear_down_cache()
{
  // fh's
  for (hash_map<fh_t, Fh*>::iterator it = fh_map.begin();
	   it != fh_map.end();
	   it++) {
	Fh *fh = it->second;
	dout(1) << "tear_down_cache forcing close of fh " << it->first << " ino " << hex << fh->inode->inode.ino << dec << endl;
	put_inode(fh->inode);
	delete fh;
  }
  fh_map.clear();

  // caps!
  // *** FIXME ***

  // empty lru
  lru.lru_set_max(0);
  trim_cache();
  assert(lru.lru_get_size() == 0);

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
  dout(1) << "dump_inode: inode " << hex << in->ino() << dec << " ref " << in->ref << " dir " << in->dir << endl;

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
	
	dout(1) << "dump_cache: inode " << hex << it->first << dec
			<< " ref " << it->second->ref 
			<< " dir " << it->second->dir << endl;
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
  unsigned last = 0;
  while (lru.lru_get_size() != last) {
	last = lru.lru_get_size();

	if (lru.lru_get_size() <= lru.lru_get_max())  break;

	// trim!
	Dentry *dn = (Dentry*)lru.lru_expire();
	if (!dn) break;  // done
	
	//dout(10) << "unlinking dn " << dn->name << " in dir " << hex << dn->dir->inode->inode.ino << dec << endl;
	unlink(dn);
  }

  // hose root?
  if (lru.lru_get_size() == 0 && root && inode_map.size() == 1) {
	delete root;
	root = 0;
	inode_map.clear();
  }
}

// insert inode info into metadata cache

Inode* Client::insert_inode_info(Dir *dir, c_inode_info *in_info)
{
  string dname = in_info->ref_dn;
  Dentry *dn = NULL;
  if (dir->dentries.count(dname))
	dn = dir->dentries[dname];
  dout(12) << "insert_inode_info " << dname << " ino " << hex << in_info->inode.ino << dec << "  size " << in_info->inode.size << "  hashed " << in_info->hashed << endl;
  
  if (dn) {
	if (dn->inode->inode.ino == in_info->inode.ino) {
	  touch_dn(dn);
	  dout(12) << " had dentry " << dname << " with correct ino " << hex << dn->inode->inode.ino << dec << endl;
	} else {
	  dout(12) << " had dentry " << dname << " with WRONG ino " << hex << dn->inode->inode.ino << dec << endl;
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
		dout(12) << " had ino " << hex << in->inode.ino << dec << " linked at wrong position, unlinking" << endl;
		dn = relink(in->dn, dir, dname);
	  } else {
		// link
		dout(12) << " had ino " << hex << in->inode.ino << dec << " unlinked, linking" << endl;
		dn = link(dir, dname, in);
	  }
	}
  }
  
  if (!dn) {
	Inode *in = new Inode;
	inode_map[in_info->inode.ino] = in;
	in->inode = in_info->inode;   // actually update indoe info
	dn = link(dir, dname, in);
	dout(12) << " new dentry+node with ino " << hex << in_info->inode.ino << dec << endl;
  } else {
	// actually update info
	dn->inode->inode = in_info->inode;
  }

  // OK, we found it!
  assert(dn && dn->inode);
  
  // or do we have newer size/mtime from writing?
  if (dn->inode->file_caps() & CAP_FILE_WR) {
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

  // dir info
  dn->inode->dir_auth = in_info->dir_auth;
  dn->inode->dir_hashed = in_info->hashed;  
  dn->inode->dir_replicated = in_info->replicated;  

  // dir replication
  if (in_info->spec_defined) {
	if (in_info->dist.empty() && !dn->inode->dir_contacts.empty())
	  dout(9) << "lost dist spec for " << hex << dn->inode->inode.ino << dec 
			  << " " << in_info->dist << endl;
	if (!in_info->dist.empty() && dn->inode->dir_contacts.empty()) 
	  dout(9) << "got dist spec for " << hex << dn->inode->inode.ino << dec 
			  << " " << in_info->dist << endl;
	dn->inode->dir_contacts = in_info->dist;
  }

  return dn->inode;
}


// insert trace of reply into metadata cache

void Client::insert_trace(const vector<c_inode_info*>& trace)
{
  Inode *cur = root;
  time_t now = time(NULL);

  if (trace.empty()) {
	return;
  }
  
  for (unsigned i=0; i<trace.size(); i++) {
    if (i == 0) {
	  c_inode_info *in_info = trace[0];

      if (!root) {
        cur = root = new Inode();
        root->inode = in_info->inode;
		inode_map[root->inode.ino] = root;
      }
	  
	  if (g_conf.client_cache_stat_ttl)
		root->valid_until = now + g_conf.client_cache_stat_ttl;

	  root->dir_auth = in_info->dir_auth;
	  assert(root->dir_auth == 0);
	  root->dir_hashed = in_info->hashed;  
	  root->dir_replicated = in_info->replicated;  
	  if (in_info->spec_defined) 
		root->dir_contacts = in_info->dist;

      dout(12) << "insert_trace trace " << i << " root .. rep=" << root->dir_replicated << endl;
    } else {
      dout(12) << "insert_trace trace " << i << endl;
      Dir *dir = cur->open_dir();
	  cur = this->insert_inode_info(dir, trace[i]);
	  
	  if (g_conf.client_cache_stat_ttl)
		cur->valid_until = now + g_conf.client_cache_stat_ttl;

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
		dout(14) << " hit dentry " << path[i] << " inode is " << dn->inode << " valid_until " << dn->inode->valid_until << endl;
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
	dout(11) << "lookup '" << path << "' found " << dn->name << " inode " << hex << dn->inode->inode.ino << dec << " valid_until " << dn->inode->valid_until<< endl;
  }

  return dn;
}

// -------

MClientReply *Client::make_request(MClientRequest *req, 
								   bool auth_best, 
								   int use_mds)  // this param is icky, debug weirdness!
{
  // find deepest known prefix
  Inode *diri = root;   // the deepest known containing dir
  Inode *item = 0;      // the actual item... if we know it
  int missing_dn = -1;  // which dn we miss on (if we miss)
  
  unsigned depth = req->get_filepath().depth();
  for (unsigned i=0; i<depth; i++) {
	// dir?
	if (diri && diri->inode.mode & INODE_MODE_DIR && diri->dir) {
	  Dir *dir = diri->dir;

	  // do we have the next dentry?
	  if (dir->dentries.count( req->get_filepath()[i] ) == 0) {
		missing_dn = i;  // no.
		break;
	  }
	  
	  dout(7) << " have path seg " << i << " on " << diri->dir_auth << " ino " << hex << diri->inode.ino << dec << " " << req->get_filepath()[i] << endl;

	  if (i == depth-1) {  // last one!
		item = dir->dentries[ req->get_filepath()[i] ]->inode;
		break;
	  } 

	  // continue..
	  diri = dir->dentries[ req->get_filepath()[i] ]->inode;
	  assert(diri);
	} else {
	  missing_dn = i;
	  break;
	}
  }

  // choose an mds
  int mds = 0;
  if (diri) {
	if (auth_best) {
	  // pick the actual auth (as best we can)
	  if (item) {
		mds = item->authority(mdcluster);
	  } else if (diri->dir_hashed && missing_dn >= 0) {
		mds = diri->dentry_authority(req->get_filepath()[missing_dn].c_str(),
									 mdcluster);
	  } else {
		mds = diri->authority(mdcluster);
	  }
	} else {
	  // balance our traffic!
	  if (diri->dir_hashed && missing_dn >= 0) 
		mds = diri->dentry_authority(req->get_filepath()[missing_dn].c_str(),
									 mdcluster);
	  else 
		mds = diri->pick_replica(mdcluster);
	}
  } else {
	// no root info, pick a random MDS
	mds = rand() % mdcluster->get_num_mds();
  }
  dout(20) << "mds is " << mds << endl;

  // force use of a particular mds?
  if (use_mds >= 0) mds = use_mds;

  // drop mutex for duration of call
  client_lock.Unlock();  
  utime_t start = g_clock.now();

  
  bool nojournal = false;
  if (req->get_op() == MDS_OP_STAT ||
	  req->get_op() == MDS_OP_LSTAT ||
	  req->get_op() == MDS_OP_READDIR ||
	  req->get_op() == MDS_OP_OPEN ||
	  req->get_op() == MDS_OP_RELEASE)
	nojournal = true;

  MClientReply *reply = (MClientReply*)messenger->sendrecv(req,
														   MSG_ADDR_MDS(mds), 
														   MDS_PORT_SERVER);


  if (client_logger) {
	utime_t lat = g_clock.now();
	lat -= start;
	dout(20) << "lat " << lat << endl;
	client_logger->finc("lsum",(double)lat);
	client_logger->inc("lnum");

	if (nojournal) {
	  client_logger->finc("lrsum",(double)lat);
	  client_logger->inc("lrnum");
	} else {
	  client_logger->finc("lwsum",(double)lat);
	  client_logger->inc("lwnum");
	}
	
	if (req->get_op() == MDS_OP_STAT) {
	  client_logger->finc("lstatsum",(double)lat);
	  client_logger->inc("lstatnum");
	}
	else if (req->get_op() == MDS_OP_READDIR) {
	  client_logger->finc("ldirsum",(double)lat);
	  client_logger->inc("ldirnum");
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
	objecter->handle_osd_op_reply((MOSDOpReply*)m);
	break;

  case MSG_OSD_MAP:
	objecter->handle_osd_map((class MOSDMap*)m);
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

  // unmounting?
  if (unmounting) {
	dout(10) << "unmounting: trim pass, size was " << lru.lru_get_size() 
			 << "+" << inode_map.size() << endl;
	trim_cache();
	if (lru.lru_get_size() == 0 && inode_map.empty()) {
	  dout(10) << "unmounting: trim pass, cache now empty, waking unmount()" << endl;
	  unmount_cond.Signal();
	} else {
	  dout(10) << "unmounting: trim pass, size still " << lru.lru_get_size() 
			   << "+" << inode_map.size() << endl;
	}
  }

  client_lock.Unlock();
}





/****
 * caps
 */

class C_Client_Flushed : public Context {
  Client *client;
  Inode *in;
public:
  C_Client_Flushed(Client *c, Inode *i) : client(c), in(i) {}
  void finish(int r) {
	client->finish_flush(in);
  }
};

void Client::handle_file_caps(MClientFileCaps *m)
{
  int mds = MSG_ADDR_NUM(m->get_source());
  Inode *in = 0;
  if (inode_map.count(m->get_ino())) in = inode_map[ m->get_ino() ];

  m->clear_payload();  // for when we send back to MDS

  // reap?
  if (m->get_special() == MClientFileCaps::FILECAP_REAP) {
	int other = m->get_mds();

	if (in && in->stale_caps.count(other)) {
	  dout(5) << "handle_file_caps on ino " << hex << m->get_ino() << dec << " from mds" << mds << " reap on mds" << other << endl;

	  // fresh from new mds?
	  if (!in->caps.count(mds)) {
		if (in->caps.empty()) in->get();
		in->caps[mds].seq = m->get_seq();
		in->caps[mds].caps = m->get_caps();
	  }
	  
	  assert(in->stale_caps.count(other));
	  in->stale_caps.erase(other);
	  if (in->stale_caps.empty()) put_inode(in); // note: this will never delete *in
	  
	  // fall-thru!
	} else {
	  dout(5) << "handle_file_caps on ino " << hex << m->get_ino() << dec << " from mds" << mds << " premature (!!) reap on mds" << other << endl;
	  // delay!
	  cap_reap_queue[in->ino()][other] = m;
	  return;
	}
  }

  assert(in);
  
  // stale?
  if (m->get_special() == MClientFileCaps::FILECAP_STALE) {
	dout(5) << "handle_file_caps on ino " << hex << m->get_ino() << dec << " seq " << m->get_seq() << " from mds" << mds << " now stale" << endl;
	
	// move to stale list
	assert(in->caps.count(mds));
	if (in->stale_caps.empty()) in->get();
	in->stale_caps[mds] = in->caps[mds];

	assert(in->caps.count(mds));
	in->caps.erase(mds);
	if (in->caps.empty()) in->put();

	// delayed reap?
	if (cap_reap_queue.count(in->ino()) &&
		cap_reap_queue[in->ino()].count(mds)) {
	  dout(5) << "handle_file_caps on ino " << hex << m->get_ino() << dec << " from mds" << mds << " delayed reap on mds" << m->get_mds() << endl;
	  
	  // process delayed reap
	  handle_file_caps( cap_reap_queue[in->ino()][mds] );

	  cap_reap_queue[in->ino()].erase(mds);
	  if (cap_reap_queue[in->ino()].empty())
		cap_reap_queue.erase(in->ino());
	}
	return;
  }

  // release?
  if (m->get_special() == MClientFileCaps::FILECAP_RELEASE) {
	dout(5) << "handle_file_caps on ino " << hex << m->get_ino() << dec << " from mds" << mds << " release" << endl;
	assert(in->caps.count(mds));
	in->caps.erase(mds);
	if (in->caps.empty()) {
	  //dout(0) << "did put_inode" << endl;
	  put_inode(in);
	} else {
	  //dout(0) << "didn't put_inode" << endl;
	}
	for (map<int,InodeCap>::iterator p = in->caps.begin();
		 p != in->caps.end();
		 p++)
	  dout(20) << " left cap " << p->first << " " 
			  << cap_string(p->second.caps) << " " 
			  << p->second.seq << endl;
	for (map<int,InodeCap>::iterator p = in->stale_caps.begin();
		 p != in->stale_caps.end();
		 p++)
	  dout(20) << " left stale cap " << p->first << " " 
			  << cap_string(p->second.caps) << " " 
			  << p->second.seq << endl;
	
	return;
  }


  // don't want?
  if (in->file_caps_wanted() == 0) {
	dout(5) << "handle_file_caps on ino " << hex << m->get_ino() << dec << " seq " << m->get_seq() << " " << cap_string(m->get_caps()) << ", which we don't want caps for, releasing." << endl;
	m->set_caps(0);
	m->set_wanted(0);
	messenger->send_message(m, m->get_source(), m->get_source_port());
	return;
  }

  /* no ooo messages yet!
  if (m->get_seq() <= in->file_caps_seq) {
	assert(0); // no ooo support yet
	dout(5) << "handle_file_caps on ino " << hex << m->get_ino() << dec << " old seq " << m->get_seq() << " <= " << in->file_caps_seq << endl;
	delete m;
	return;
  }
  */

  assert(in->caps.count(mds));

  // update caps
  const int old_caps = in->caps[mds].caps;
  const int new_caps = m->get_caps();
  in->caps[mds].caps = new_caps;
  in->caps[mds].seq = m->get_seq();
  dout(5) << "handle_file_caps on in " << hex << m->get_ino() << dec << " seq " << m->get_seq() << " caps now " << cap_string(new_caps) << " was " << cap_string(old_caps) << endl;
  
  // did file size decrease?
  if ((old_caps & new_caps & CAP_FILE_RDCACHE) &&
	  in->inode.size > m->get_inode().size) {
	// must have been a truncate() by someone.
	// trim the buffer cache
	// ***** write me ****
  }

  // update inode
  in->inode = m->get_inode();      // might have updated size... FIXME this is overkill!

  // flush buffers?
  if (g_conf.client_oc &&
	  !(in->file_caps() & CAP_FILE_WRBUFFER)) {

	// **** write me ****

  } 
  
  // release all buffers?
  if ((old_caps & CAP_FILE_RDCACHE) &&
	  !(new_caps & CAP_FILE_RDCACHE))
	release_inode_buffers(in);

  // wake up waiters?
  if (new_caps & CAP_FILE_RD) {
	for (list<Cond*>::iterator it = in->waitfor_read.begin();
		 it != in->waitfor_read.end();
		 it++) {
	  dout(5) << "signaling read waiter " << *it << endl;
	  (*it)->Signal();
	}
	in->waitfor_read.clear();
  }
  if (new_caps & CAP_FILE_WR) {
	for (list<Cond*>::iterator it = in->waitfor_write.begin();
		 it != in->waitfor_write.end();
		 it++) {
	  dout(5) << "signaling write waiter " << *it << endl;
	  (*it)->Signal();
	}
	in->waitfor_write.clear();
  }

  // ack?
  if (old_caps & ~new_caps) {
	// send back to mds
	dout(5) << " we lost caps " << cap_string(old_caps & ~new_caps) << ", acking" << endl;
	messenger->send_message(m, m->get_source(), m->get_source_port());
  } else {
	// discard
	delete m;
  }
}

void Client::finish_flush(Inode *in)
{
  dout(5) << "finish_flush on ino " << in->ino() << endl;

  /*Filecache *fc = bc->get_fc(in);
  assert(!fc->is_dirty() &&
		 !fc->is_inflight());
  */

  // release all buffers?
  if (!(in->file_caps() & CAP_FILE_RDCACHE)) {
	dout(5) << "flush_finish releasing all buffers on ino " << hex << in->ino() << dec << endl;
	//release_inode_buffers(in);
  }
  
  if (in->num_rd == 0 && in->num_wr == 0) {
	dout(5) << "flush_finish ino " << hex << in->ino() << dec << " releasing remaining caps (no readers/writers)" << endl;
	release_caps(in);
  } else {
	dout(5) << "finish_flush ino " << hex << in->ino() << dec << " acking" << endl;
	release_caps(in, in->file_caps() & ~CAP_FILE_WRBUFFER);   // send whatever we have
  }

  put_inode(in);
}


void Client::release_caps(Inode *in,
						  int retain)
{
  dout(5) << "releasing caps on ino " << hex << in->inode.ino << dec
		  << " had " << cap_string(in->file_caps())
		  << " retaining " << cap_string(retain) 
		  << endl;
  
  for (map<int,InodeCap>::iterator it = in->caps.begin();
	   it != in->caps.end();
	   it++) {
	//if (it->second.caps & ~retain) {
	if (1) {
	  // release (some of?) these caps
	  it->second.caps = retain & it->second.caps;
	  // note: tell mds _full_ wanted; it'll filter/behave based on what it is allowed to do
	  MClientFileCaps *m = new MClientFileCaps(in->inode, 
											   it->second.seq,
											   it->second.caps,
											   in->file_caps_wanted()); 
	  messenger->send_message(m, MSG_ADDR_MDS(it->first), MDS_PORT_CACHE);
	}
  }
  
  if ((in->file_caps() & CAP_FILE_WR) == 0) {
	in->file_wr_mtime = 0;
	in->file_wr_size = 0;
  }
}

void Client::update_caps_wanted(Inode *in)
{
  dout(5) << "updating caps wanted on ino " << in->inode.ino 
		  << " to " << cap_string(in->file_caps_wanted())
		  << endl;
  
  // FIXME: pick a single mds and let the others off the hook..
  for (map<int,InodeCap>::iterator it = in->caps.begin();
	   it != in->caps.end();
	   it++) {
	MClientFileCaps *m = new MClientFileCaps(in->inode, 
											 it->second.seq,
											 it->second.caps,
											 in->file_caps_wanted());
	messenger->send_message(m,
							MSG_ADDR_MDS(it->first), MDS_PORT_CACHE);
  }
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

  // mdcluster!
  mdcluster = new MDCluster(g_conf.num_mds, g_conf.num_osd);  // FIXME this is stoopid

  // we got osdmap!
  osdmap->decode(reply->get_osd_map_state());

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
  unmounting = true;

  // make sure all buffers are clean
  dout(3) << "flushing buffers" << endl;

  // *** WRITE ME ****

  // empty lru cache
  lru.lru_set_max(0);
  trim_cache();

  while (lru.lru_get_size() > 0 || 
		 !inode_map.empty() ||
		 unsafe_sync_write > 0) {
	dout(0) << "cache still has " << lru.lru_get_size() 
			<< "+" << inode_map.size() << " items + " 
			<< unsafe_sync_write << " unsafe_sync_writes, waiting (presumably for caps to be released?)" << endl;
	
	dump_cache();
	
	unmount_cond.Wait(client_lock);
  }
  assert(lru.lru_get_size() == 0);
  assert(inode_map.empty());
  
  // send unmount!
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


int Client::unlink(const char *relpath)
{
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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
	if (dn) {
	  release_inode_buffers(dn->inode);
	  unlink(dn);
	}
  }
  this->insert_trace(reply->get_trace());
  delete reply;
  dout(10) << "unlink result is " << res << endl;

  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::rename(const char *relfrom, const char *relto)
{
  client_lock.Lock();

  string absfrom;
  mkabspath(relfrom, absfrom);
  const char *from = absfrom.c_str();
  string absto;
  mkabspath(relto, absto);
  const char *to = absto.c_str();

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

int Client::mkdir(const char *relpath, mode_t mode)
{
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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

int Client::rmdir(const char *relpath)
{
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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
  
int Client::symlink(const char *reltarget, const char *rellink)
{
  client_lock.Lock();

  string abstarget;
  mkabspath(reltarget, abstarget);
  const char *target = abstarget.c_str();
  string abslink;
  mkabspath(rellink, abslink);
  const char *link = abslink.c_str();

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

int Client::readlink(const char *relpath, char *buf, off_t size) 
{ 
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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

int Client::lstat(const char *relpath, struct stat *stbuf)
{
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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
  if (dn && now <= dn->inode->valid_until) {
	inode = dn->inode->inode;
	dout(10) << "lstat cache hit, valid until " << dn->inode->valid_until << endl;
	
	if (g_conf.client_cache_stat_ttl == 0)
	  dn->inode->valid_until = 0;           // only one stat allowed after each readdir

	delete req;  // don't need this
  } else {  
	// FIXME where does FUSE maintain user information
	//struct fuse_context *fc = fuse_get_context();
	//req->set_caller_uid(fc->uid);
	//req->set_caller_gid(fc->gid);
	
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
	stbuf->st_size =  inode.size;
	stbuf->st_blocks = inode.size ? ((inode.size - 1) / 1024 + 1):0;
	stbuf->st_blksize = 1024;
	//stbuf->st_flags =
	//stbuf->st_gen =
	
	dout(10) << "stat sez size = " << inode.size << "   uid = " << inode.uid << " ino = " << hex << stbuf->st_ino << dec << endl;
  }

  trim_cache();
  client_lock.Unlock();
  return res;
}



int Client::chmod(const char *relpath, mode_t mode)
{
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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

int Client::chown(const char *relpath, uid_t uid, gid_t gid)
{
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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

int Client::utime(const char *relpath, struct utimbuf *buf)
{
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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



int Client::mknod(const char *relpath, mode_t mode) 
{ 
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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

int Client::getdir(const char *relpath, map<string,inode_t*>& contents) 
{
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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
		
		if (g_conf.client_cache_stat_ttl)
		  in->valid_until = now + g_conf.client_cache_stat_ttl;
		else if (g_conf.client_cache_readdir_ttl)
		  in->valid_until = now + g_conf.client_cache_readdir_ttl;
		
		// contents to caller too!
		contents[(*it)->ref_dn] = &in->inode;
	  }
	}

	// FIXME: remove items in cache that weren't in my readdir?
	// ***
  }

  delete reply;     //fix thing above first

  client_lock.Unlock();
  return res;
}




/****** file i/o **********/

int Client::open(const char *relpath, int mode) 
{
  client_lock.Lock();

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

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
  
  MClientReply *reply = make_request(req, 
									 mode & (O_RDWR|O_WRONLY)); // try auth if writer
  
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
	f->mode = cmode;

	// inode
	f->inode = inode_map[trace[trace.size()-1]->inode.ino];
	assert(f->inode);
	f->inode->get();

	if (cmode & FILE_MODE_R) f->inode->num_rd++;
	if (cmode & FILE_MODE_W) f->inode->num_wr++;

	// caps included?
	int mds = MSG_ADDR_NUM(reply->get_source());

	if (f->inode->caps.empty()) // first caps?
	  f->inode->get();

	assert(reply->get_file_caps_seq() >= f->inode->caps[mds].seq);
	if (reply->get_file_caps_seq() > f->inode->caps[mds].seq) {   
	  dout(7) << "open got caps " << cap_string(reply->get_file_caps()) << " for " << hex << f->inode->ino() << dec << " seq " << reply->get_file_caps_seq() << " from mds" << mds << endl;

	  int old_caps = f->inode->caps[mds].caps;
	  f->inode->caps[mds].caps = reply->get_file_caps();
	  f->inode->caps[mds].seq = reply->get_file_caps_seq();

	  // we shouldn't ever lose caps at this point.
	  assert((old_caps & ~f->inode->caps[mds].caps) == 0);
	} else {
	  dout(7) << "open got SAME caps " << cap_string(reply->get_file_caps()) << " for " << hex << f->inode->ino() << dec << " seq " << reply->get_file_caps_seq() << " from mds" << mds << endl;
	}
	
	// put in map
	result = fh = get_fh();
	assert(fh_map.count(fh) == 0);
	fh_map[fh] = f;
	
	dout(3) << "open success, fh is " << fh << " combined caps " << cap_string(f->inode->file_caps()) << endl;
  } else {
	dout(0) << "open failure result " << result << endl;
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

  // update inode rd/wr counts
  int before = in->file_caps_wanted();
  if (f->mode & FILE_MODE_R) 	
	in->num_rd--;
  if (f->mode & FILE_MODE_W)
	in->num_wr--;
  int after = in->file_caps_wanted();

  // does this change what caps we want?
  if (before != after && after)
	update_caps_wanted(in);

  // hose fh
  fh_map.erase(fh);
  delete f;

  // release caps right away?
  dout(10) << "num_rd " << in->num_rd << "  num_wr " << in->num_wr << endl;
  if (in->num_rd == 0 && in->num_wr == 0) {
	
	// flush anything?
	// ** WRITE ME **
	dout(10) << "  flushing dirty buffers on " << hex << in->ino() << dec << endl;
	

	/*if (g_conf.client_oc && 
		(fc->is_dirty() || fc->is_inflight())) {
	  // flushing.
	  dout(10) << "  waiting for inflight buffers on " << hex << in->ino() << dec << endl;
	  in->get();
	  fc->add_inflight_waiter(  new C_Client_Flushed(this, in) );
	  } else {*/
	  // all clean!
	dout(10) << "  releasing buffers and caps on " << hex << in->ino() << dec << endl;
	  release_inode_buffers(in);  // free buffers
	  release_caps(in);        	  // release caps now.
	  //}
  }

  put_inode( in );
  int result = 0;

  client_lock.Unlock();
  return result;
}



// ------------
// read, write

// blocking osd interface

int Client::read(fh_t fh, char *buf, off_t size, off_t offset) 
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

  if (offset < 0) 
	offset = f->pos;
  
  // do we have read file cap?
  while ((in->file_caps() & CAP_FILE_RD) == 0) {
	dout(7) << " don't have read cap, waiting" << endl;
	Cond cond;
	in->waitfor_read.push_back(&cond);
	cond.Wait(client_lock);
  }
  
 
  // determine whether read range overlaps with file
  // ...ONLY if we're doing async io
  if (in->file_caps() & (CAP_FILE_WRBUFFER|CAP_FILE_RDCACHE)) {
	// we're doing buffered i/o.  make sure we're inside the file.
	// we can trust size info bc we get accurate info when buffering/caching caps are issued.
	dout(10) << "file size: " << in->inode.size << endl;
	if (offset > 0 && offset >= in->inode.size) {
	  client_lock.Unlock();
	  return 0;
	}
	if (offset + size > (unsigned)in->inode.size) size = (unsigned)in->inode.size - offset;
	
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

  // adjust fd pos
  f->pos = offset+size;

  Cond cond;
  bufferlist blist;   // data will go here
  bool done = false;
  C_Cond *onfinish = new C_Cond(&cond, &done, &rvalue);

  int r = 0;
  if (g_conf.client_oc) {
	// object cache ON
	r = objectcacher->file_read(in->inode, size, offset, &blist, onfinish);
  } else {
	// object cache OFF -- legacy inconsistent way.
    r = filer->read(in->inode, size, offset, &blist, onfinish);
  }

  if (r == 0) {
	// wait!
	while (!done)
	  cond.Wait(client_lock);
  } else {
	// had it cached, apparently!
	assert(r > 0);
	delete onfinish;
  }
  // copy data into caller's buf
  blist.copy(0, blist.length(), buf);

  // done!
  client_lock.Unlock();
  return rvalue;
}



/*
 * hack -- 
 *  until we properly implement synchronous writes wrt buffer cache,
 *  make sure we delay shutdown until they're all safe on disk!
 */
class C_Client_HackUnsafe : public Context {
  Client *cl;
public:
  C_Client_HackUnsafe(Client *c) : cl(c) {}
  void finish(int) {
	cl->hack_sync_write_safe();
  }
};

void Client::hack_sync_write_safe()
{
  client_lock.Lock();
  assert(unsafe_sync_write > 0);
  unsafe_sync_write--;
  if (unsafe_sync_write == 0 && unmounting) {
	dout(10) << "hack_sync_write_safe -- no more unsafe writes, unmount can proceed" << endl;
	unmount_cond.Signal();
  }
  client_lock.Unlock();
}

int Client::write(fh_t fh, const char *buf, off_t size, off_t offset) 
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

  if (offset < 0) 
	offset = f->pos;

  dout(10) << "cur file size is " << in->inode.size << "    wr size " << in->file_wr_size << endl;


  // do we have write file cap?
  while ((in->file_caps() & CAP_FILE_WR) == 0) {
	dout(7) << " don't have write cap, waiting" << endl;
	Cond cond;
	in->waitfor_write.push_back(&cond);
	cond.Wait(client_lock);
  }

  // adjust fd pos
  f->pos = offset+size;

  // time it.
  utime_t start = g_clock.now();
	
  if (g_conf.client_oc) { // buffer cache ON?
	assert(objectcacher);

	bufferlist blist;
	blist.push_back( new buffer(buf, size) );

	// wait?  (this may block!)
	objectcacher->wait_for_write(size, client_lock);
	
	if (in->file_caps() & CAP_FILE_WRBUFFER) {   // caps buffered write?
	  // async, caching, non-blocking.
	  objectcacher->file_write(in->inode, size, offset, blist);
	} else {
	  // atomic, synchronous, blocking.
	  objectcacher->file_atomic_sync_write(in->inode, size, offset, blist, client_lock);	  
	}
  } else {
	// legacy, inconsistent synchronous write.
	dout(7) << "synchronous write" << endl;
	  
	// create a buffer that refers to *buf, but doesn't try to free it when it's done.
	bufferlist blist;
	blist.push_back( new buffer(buf, size, BUFFER_MODE_NOCOPY|BUFFER_MODE_NOFREE) );
	  
	// issue write
	Cond cond;
	int rvalue = 0;
	
	bool done = false;
	C_Cond *onfinish = new C_Cond(&cond, &done, &rvalue);
	C_Client_HackUnsafe *onsafe = new C_Client_HackUnsafe(this);
	unsafe_sync_write++;
	
	dout(20) << " sync write start " << onfinish << endl;
	
	filer->write(in->inode, size, offset, blist, 0, 
				 onfinish, onsafe);
	
	while (!done) {
	  cond.Wait(client_lock);
	  dout(20) << " sync write bump " << onfinish << endl;
	}
	dout(20) << " sync write done " << onfinish << endl;
  }

  // time
  utime_t lat = g_clock.now();
  lat -= start;
  if (client_logger) {
	client_logger->finc("wrlsum",(double)lat);
	client_logger->inc("wrlnum");
  }
	
  // assume success for now.  FIXME.
  off_t totalwritten = size;
  
  // extend file?
  if (totalwritten + offset > in->inode.size) {
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

  dout(3) << "fsync fh " << fh << " ino " << hex << in->inode.ino << dec << " syncdataonly " << syncdataonly << endl;
 
  // blocking flush

  assert(0);  // WRITE ME

  if (syncdataonly &&
	  (in->file_caps() & CAP_FILE_WR)) {
	// flush metadata too.. size, mtime
	// ... WRITE ME ...
  }

  client_lock.Unlock();
  return r;
}


// not written yet, but i want to link!

int Client::chdir(const char *path)
{
  // fake it for now!
  string abs;
  mkabspath(path, abs);
  dout(3) << "chdir " << path << " -> cwd now " << abs << endl;
  cwd = abs;
  return 0;
}

int Client::statfs(const char *path, struct statfs *stbuf) 
{
  assert(0);  // implement me
  return 0;
}
