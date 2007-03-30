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



// unix-ey fs stuff
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <utime.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <sys/statvfs.h>


#include <iostream>
using namespace std;


// ceph stuff
#include "Client.h"


#include "messages/MClientBoot.h"
#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientFileCaps.h"

#include "messages/MClientAuthUser.h"
#include "messages/MClientAuthUserAck.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSGetMap.h"
#include "messages/MMDSMap.h"

#include "osdc/Filer.h"
#include "osdc/Objecter.h"
#include "osdc/ObjectCacher.h"
#include "ClientCapCache.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Logger.h"

#include "crypto/Ticket.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_client) cout << g_clock.now() << " client" << whoami << "." << pthread_self() << " "

#define  tout       if (g_conf.client_trace) cout << "trace: " 


// static logger
LogType client_logtype;
Logger  *client_logger = 0;



class C_Client_CloseRelease : public Context {
  Client *cl;
  Inode *in;
public:
  C_Client_CloseRelease(Client *c, Inode *i) : cl(c), in(i) {}
  void finish(int) {
    cl->close_release(in);
  }
};

class C_Client_CloseSafe : public Context {
  Client *cl;
  Inode *in;
public:
  C_Client_CloseSafe(Client *c, Inode *i) : cl(c), in(i) {}
  void finish(int) {
    cl->close_safe(in);
  }
};






// cons/des

Client::Client(Messenger *m, MonMap *mm)
{
  // which client am i?
  whoami = m->get_myname().num();
  monmap = mm;
  monmap->prepare_mon_key();

  mounted = false;
  unmounting = false;

  last_tid = 0;
  unsafe_sync_write = 0;

  mdsmap = 0;

  // 
  root = 0;

  set_cache_size(g_conf.client_cache_size);

  // file handles
  free_fh_set.insert(10, 1<<30);

  // set up messengers
  messenger = m;
  messenger->set_dispatcher(this);

  // osd interfaces
  osdmap = new OSDMap();     // initially blank.. see mount()
  objecter = new Objecter(messenger, monmap, osdmap);
  objectcacher = new ObjectCacher(objecter, client_lock);
  filer = new Filer(objecter);
  capcache = new ClientCapCache(messenger, this, client_lock);
}


Client::~Client() 
{
  tear_down_cache();

  if (objectcacher) { 
    delete objectcacher; 
    objectcacher = 0; 
  }

  if (filer) { delete filer; filer = 0; }
  if (objecter) { delete objecter; objecter = 0; }
  if (osdmap) { delete osdmap; osdmap = 0; }
  if (mdsmap) { delete mdsmap; mdsmap = 0; }
  if (capcache) { delete capcache; capcache = 0; }

  if (messenger) { delete messenger; messenger = 0; }
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
  dout(1) << "dump_inode: inode " << in->ino() << " ref " << in->ref << " dir " << in->dir << endl;

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
    
    dout(1) << "dump_cache: inode " << it->first
            << " ref " << it->second->ref 
            << " dir " << it->second->dir << endl;
    if (it->second->dir) {
      dout(1) << "  dir size " << it->second->dir->dentries.size() << endl;
    }
  }
 
}

// should send boot message?
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
    
    //dout(10) << "trim_cache unlinking dn " << dn->name << " in dir " << hex << dn->dir->inode->inode.ino << endl;
    unlink(dn);
  }

  // hose root?
  if (lru.lru_get_size() == 0 && root && inode_map.size() == 1) {
    delete root;
    root = 0;
    inode_map.clear();
  }
}

/** insert_inode
 *
 * insert + link a single dentry + inode into the metadata cache.
 */
Inode* Client::insert_inode(Dir *dir, InodeStat *st, const string& dname)
{
  Dentry *dn = NULL;
  if (dir->dentries.count(dname))
    dn = dir->dentries[dname];

  dout(12) << "insert_inode " << dname << " ino " << st->inode.ino 
           << "  size " << st->inode.size
           << "  mtime " << st->inode.mtime
           << "  hashed " << st->hashed
           << endl;
  
  if (dn) {
    if (dn->inode->inode.ino == st->inode.ino) {
      touch_dn(dn);
      dout(12) << " had dentry " << dname
               << " with correct ino " << dn->inode->inode.ino
               << endl;
    } else {
      dout(12) << " had dentry " << dname
               << " with WRONG ino " << dn->inode->inode.ino
               << endl;
      unlink(dn);
      dn = NULL;
    }
  }
  
  if (!dn) {
    // have inode linked elsewhere?  -> unlink and relink!
    if (inode_map.count(st->inode.ino)) {
      Inode *in = inode_map[st->inode.ino];
      assert(in);

      if (in->dn) {
        dout(12) << " had ino " << in->inode.ino
                 << " linked at wrong position, unlinking"
                 << endl;
        dn = relink(in->dn, dir, dname);
      } else {
        // link
        dout(12) << " had ino " << in->inode.ino
                 << " unlinked, linking" << endl;
        dn = link(dir, dname, in);
      }
    }
  }
  
  if (!dn) {
    Inode *in = new Inode(st->inode, objectcacher);
    inode_map[st->inode.ino] = in;
    dn = link(dir, dname, in);
    dout(12) << " new dentry+node with ino " << st->inode.ino << endl;
  } else {
    // actually update info
    dout(12) << " stat inode mask is " << st->inode.mask << endl;
    dn->inode->inode = st->inode;

    // ...but don't clobber our mtime, size!
    if ((dn->inode->inode.mask & INODE_MASK_SIZE) == 0 &&
        dn->inode->file_wr_size > dn->inode->inode.size) 
      dn->inode->inode.size = dn->inode->file_wr_size;
    if ((dn->inode->inode.mask & INODE_MASK_MTIME) == 0 &&
        dn->inode->file_wr_mtime > dn->inode->inode.mtime) 
      dn->inode->inode.mtime = dn->inode->file_wr_mtime;
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
    *(dn->inode->symlink) = st->symlink;
  }

  return dn->inode;
}

/** update_inode_dist
 *
 * update MDS location cache for a single inode
 */
void Client::update_inode_dist(Inode *in, InodeStat *st)
{
  // dir info
  in->dir_auth = st->dir_auth;
  in->dir_hashed = st->hashed;  
  in->dir_replicated = st->replicated;  
  
  // dir replication
  if (st->spec_defined) {
    if (st->dist.empty() && !in->dir_contacts.empty())
      dout(9) << "lost dist spec for " << in->inode.ino 
              << " " << st->dist << endl;
    if (!st->dist.empty() && in->dir_contacts.empty()) 
      dout(9) << "got dist spec for " << in->inode.ino 
              << " " << st->dist << endl;
    in->dir_contacts = st->dist;
  }
}


/** insert_trace
 *
 * insert a trace from a MDS reply into the cache.
 */
Inode* Client::insert_trace(MClientReply *reply)
{
  Inode *cur = root;
  time_t now = time(NULL);

  dout(10) << "insert_trace got " << reply->get_trace_in().size() << " inodes" << endl;

  list<string>::const_iterator pdn = reply->get_trace_dn().begin();

  for (list<InodeStat*>::const_iterator pin = reply->get_trace_in().begin();
       pin != reply->get_trace_in().end();
       ++pin) {
    
    if (pin == reply->get_trace_in().begin()) {
      // root
      dout(10) << "insert_trace root" << endl;
      if (!root) {
        // create
        cur = root = new Inode((*pin)->inode, objectcacher);
        inode_map[root->inode.ino] = root;
      }
    } else {
      // not root.
      dout(10) << "insert_trace dn " << *pdn << " ino " << (*pin)->inode.ino << endl;
      Dir *dir = cur->open_dir();
      cur = this->insert_inode(dir, *pin, *pdn);
      ++pdn;      

      // move to top of lru!
      if (cur->dn) 
        lru.lru_touch(cur->dn);
    }

    // update dist info
    update_inode_dist(cur, *pin);

    // set cache ttl
    if (g_conf.client_cache_stat_ttl)
      cur->valid_until = now + g_conf.client_cache_stat_ttl;
  }

  return cur;
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
    dout(11) << "lookup '" << path << "' found " << dn->name << " inode " << dn->inode->inode.ino << " valid_until " << dn->inode->valid_until<< endl;
  }

  return dn;
}

// -------



MClientReply *Client::make_request(MClientRequest *req, 
                                   bool auth_best, 
                                   int use_mds)  // this param is purely for debug hacking
{
  // assign a unique tid
  req->set_tid(++last_tid);

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
      
      dout(7) << " have path seg " << i << " on " << diri->dir_auth << " ino " << diri->inode.ino << " " << req->get_filepath()[i] << endl;

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
  if (!diri || g_conf.client_use_random_mds) {
    // no root info, pick a random MDS
    mds = rand() % mdsmap->get_num_mds();
  } else {
    if (auth_best) {
      // pick the actual auth (as best we can)
      if (item) {
        mds = item->authority(mdsmap);
      } else if (diri->dir_hashed && missing_dn >= 0) {
        mds = diri->dentry_authority(req->get_filepath()[missing_dn].c_str(),
                                     mdsmap);
      } else {
        mds = diri->authority(mdsmap);
      }
    } else {
      // balance our traffic!
      if (diri->dir_hashed && missing_dn >= 0) 
        mds = diri->dentry_authority(req->get_filepath()[missing_dn].c_str(),
                                     mdsmap);
      else 
        mds = diri->pick_replica(mdsmap);
    }
  }
  dout(20) << "mds is " << mds << endl;

  // force use of a particular mds?
  if (use_mds >= 0) mds = use_mds;


  // time the call
  utime_t start = g_clock.now();
  
  bool nojournal = false;
  int op = req->get_op();
  if (op == MDS_OP_STAT ||
      op == MDS_OP_LSTAT ||
      op == MDS_OP_READDIR ||
      op == MDS_OP_OPEN ||
      op == MDS_OP_RELEASE)
    nojournal = true;

  MClientReply *reply = sendrecv(req, mds);

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
    
    if (op == MDS_OP_STAT) {
      client_logger->finc("lstatsum",(double)lat);
      client_logger->inc("lstatnum");
    }
    else if (op == MDS_OP_READDIR) {
      client_logger->finc("ldirsum",(double)lat);
      client_logger->inc("ldirnum");
    }

  }

  return reply;
}


MClientReply* Client::sendrecv(MClientRequest *req, int mds)
{
  // NEW way.
  Cond cond;
  tid_t tid = req->get_tid();
  mds_rpc_cond[tid] = &cond;
  
  messenger->send_message(req, mdsmap->get_inst(mds), MDS_PORT_SERVER);
  
  // wait
  while (mds_rpc_reply.count(tid) == 0) {
    dout(20) << "sendrecv awaiting reply kick on " << &cond << endl;
    cond.Wait(client_lock);
  }
  
  // got it!
  MClientReply *reply = mds_rpc_reply[tid];
  
  // kick dispatcher (we've got it!)
  assert(mds_rpc_dispatch_cond.count(tid));
  mds_rpc_dispatch_cond[tid]->Signal();
  dout(20) << "sendrecv kickback on tid " << tid << " " << mds_rpc_dispatch_cond[tid] << endl;
  
  // clean up.
  mds_rpc_cond.erase(tid);
  mds_rpc_reply.erase(tid);

  return reply;
}

void Client::handle_client_reply(MClientReply *reply)
{
  tid_t tid = reply->get_tid();

  // store reply
  mds_rpc_reply[tid] = reply;

  // wake up waiter
  assert(mds_rpc_cond.count(tid));
  dout(20) << "handle_client_reply kicking caller on " << mds_rpc_cond[tid] << endl;
  mds_rpc_cond[tid]->Signal();

  // wake for kick back
  assert(mds_rpc_dispatch_cond.count(tid) == 0);
  Cond cond;
  mds_rpc_dispatch_cond[tid] = &cond;
  while (mds_rpc_cond.count(tid)) {
    dout(20) << "handle_client_reply awaiting kickback on tid " << tid << " " << &cond << endl;
    cond.Wait(client_lock);
  }

  // ok, clean up!
  mds_rpc_dispatch_cond.erase(tid);
}


void Client::handle_auth_user_ack(MClientAuthUserAck *m)
{
  uid_t uid = m->get_uid();
  dout(10) << "handle_auth_user_ack for " << uid << endl;

  // put the ticket in the ticket map
  // **
  user_ticket[uid] = m->getTicket();

  // verify the ticket
  //assert(user_ticket[uid]->verif_ticket(monmap->get_key()));

  // wait up the waiter(s)
  // this signals all ticket waiters
  for (list<Cond*>::iterator p = ticket_waiter_cond[uid].begin();
       p != ticket_waiter_cond[uid].end();
       ++p) {
    (*p)->Signal();
  }

  ticket_waiter_cond.erase(uid);

}

Ticket *Client::get_user_ticket(uid_t uid, gid_t gid)
{
  dout(10) << "get_user_ticket for uid: " << uid << ", gid: " << gid << endl;
  // do we already have it?
  if (user_ticket.count(uid) == 0) {
    Cond cond;
    string username;  // i don't know!
    string key;       // get from cache or make it now

    // no key, make one now
    // this should be a function with some
    // security stuff (password) to gen key
    if (user_pub_key.count(uid) == 0) {
      //esignPriv privKey = esignPrivKey("crypto/esig1536.dat");
      esignPriv privKey = esignPrivKey("crypto/esig1023.dat");
      esignPub pubKey = esignPubKey(privKey);
      user_priv_key[uid] = &privKey;
      user_pub_key[uid] = &pubKey;
    }
    key = pubToString(*(user_pub_key[uid]));

    // if no one has already requested the ticket
    if (ticket_waiter_cond.count(uid) == 0) {
      // request from monitor
      int mon = monmap->pick_mon();
      dout(10) << "get_user_ticket requesting ticket for uid " << uid 
	       << " from mon" << mon << endl;
      messenger->send_message(new MClientAuthUser(username, uid, gid, key),
			      monmap->get_inst(mon));
    } else {
      // don't request, someone else already did.  just wait!
      dout(10) << "get_user_ticket waiting for ticket for uid " << uid << endl;
    }
    
    // wait for reply
    ticket_waiter_cond[uid].push_back( &cond );
    
    // naively assume we'll get a ticket FIXME
    while (user_ticket.count(uid) == 0) { 
      cond.Wait(client_lock);
    }

  }

  // inc ref count
  user_ticket_ref[uid]++;
  return user_ticket[uid];
}

void Client::put_user_ticket(Ticket *tk)
{
  // dec ref count
  uid_t uid = tk->get_uid();
  user_ticket_ref[uid]--;
  if (user_ticket_ref[uid] == 0)
    user_ticket_ref.erase(uid);
}

void Client::handle_osd_update(MOSDUpdate *m) {
  
  utime_t update_time_start = g_clock.now();

  hash_t my_hash = m->get_hash();

  // if we dont have it cached, ask mds
  // this will lose execution control
  if (groups.count(my_hash) == 0) {

    MClientUpdate *update = new MClientUpdate(my_hash);

    // is anyone else already waiting for this hash?
    if (update_waiter_osd.count(my_hash) == 0) {
      dout(10) << "mds_group_update for " << my_hash << endl;
      // FIXME choose mds (always choose 0)
      messenger->send_message(update, mdsmap->get_inst(0), MDS_PORT_SERVER);
    }
    else {
      dout(10) << "mds_group_update for " << my_hash << endl;
    }

    update_waiter_osd[my_hash].insert(m->get_source().num());
  }
  // we have the group, hand it back
  else {
    MOSDUpdateReply *reply;
    // its a file group
    if (g_conf.mds_group == 3)
      reply = new MOSDUpdateReply(my_hash, groups[my_hash].get_inode_list(),
				  groups[my_hash].get_sig());
    // its a user group
    else
      reply = new MOSDUpdateReply(my_hash, groups[my_hash].get_list(),
				  groups[my_hash].get_sig());
    
    messenger->send_message(reply, m->get_source_inst());
  }

  // default test option (only for debug)
  //MOSDUpdateReply *reply = new MOSDUpdateReply(my_hash);
  //messenger->send_message(reply, m->get_source_inst());

}

void Client::handle_client_update_reply(MClientUpdateReply *m) {
  
  hash_t my_hash = m->get_user_hash();

  // cache the list
  if (g_conf.mds_group == 3)
    groups[my_hash].set_inode_list(m->get_file_list());
  else
    groups[my_hash].set_list(m->get_user_list());

  groups[my_hash].set_root_hash(my_hash);
  groups[my_hash].set_sig(m->get_sig());

  // wake the waiters and send them all a reply
  for (set<int>::iterator oi = update_waiter_osd[my_hash].begin();
       oi != update_waiter_osd[my_hash].end();
       ++oi) {
    MOSDUpdateReply *reply;
    if (g_conf.mds_group == 3)
      reply = new MOSDUpdateReply(my_hash, groups[my_hash].get_inode_list(),
				  m->get_sig()); 
    else
      reply = new MOSDUpdateReply(my_hash, groups[my_hash].get_list(),
				  m->get_sig());
    messenger->send_message(reply, osdmap->get_inst(*oi));
  }
  
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
  case MSG_OSD_UPDATE:
    handle_osd_update((MOSDUpdate*)m);
    break;
  case MSG_CLIENT_UPDATE_REPLY:
    handle_client_update_reply((MClientUpdateReply*)m);
    break;

    // client
  case MSG_MDS_MAP:
    handle_mds_map((MMDSMap*)m);
    break;

  case MSG_CLIENT_AUTH_USER_ACK:
    handle_auth_user_ack((MClientAuthUserAck*)m);
    break;
    
  case MSG_CLIENT_REPLY:
    handle_client_reply((MClientReply*)m);
    break;

  case MSG_CLIENT_FILECAPS:
    handle_file_caps((MClientFileCaps*)m);
    break;

  case MSG_CLIENT_MOUNTACK:
    handle_mount_ack((MClientMountAck*)m);
    break;
  case MSG_CLIENT_UNMOUNT:
    handle_unmount_ack(m);
    break;
    //  case MSG_OSD_UPDATE:
    //
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

    // hack  
    //mount_cond.Signal();  // don't hold up unmount 

    if (lru.lru_get_size() == 0 && inode_map.empty()) {
      dout(10) << "unmounting: trim pass, cache now empty, waking unmount()" << endl;
      mount_cond.Signal();
    } else {
      dout(10) << "unmounting: trim pass, size still " << lru.lru_get_size() 
               << "+" << inode_map.size() << endl;
      dump_cache();      
    }
  }

  client_lock.Unlock();
}


void Client::handle_mds_map(MMDSMap* m)
{
  if (mdsmap == 0)
    mdsmap = new MDSMap;

  if (whoami < 0) {
    whoami = m->get_dest().num();
    dout(1) << "handle_mds_map i am now " << m->get_dest() << endl;
    messenger->reset_myname(m->get_dest());
  }    

  dout(1) << "handle_mds_map epoch " << m->get_epoch() << endl;
  mdsmap->decode(m->get_encoded());
  
  delete m;

  // note our inc #
  objecter->set_client_incarnation(0);  // fixme

  mount_cond.Signal();  // mount might be waiting for this.
}


/****
 * caps
 */


class C_Client_ImplementedCaps : public Context {
  Client *client;
  MClientFileCaps *msg;
  Inode *in;
public:
  C_Client_ImplementedCaps(Client *c, MClientFileCaps *m, Inode *i) : client(c), msg(m), in(i) {}
  void finish(int r) {
    client->implemented_caps(msg,in);
  }
};

/** handle_file_caps
 * handle caps update from mds.  including mds to mds caps transitions.
 * do not block.
 */
void Client::handle_file_caps(MClientFileCaps *m)
{
  int mds = m->get_source().num();
  Inode *in = 0;
  if (inode_map.count(m->get_ino())) in = inode_map[ m->get_ino() ];

  m->clear_payload();  // for if/when we send back to MDS

  // reap?
  if (m->get_special() == MClientFileCaps::FILECAP_REAP) {
    int other = m->get_mds();

    if (in && in->stale_caps.count(other)) {
      dout(5) << "handle_file_caps on ino " << m->get_ino() << " from mds" << mds << " reap on mds" << other << endl;

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
      dout(5) << "handle_file_caps on ino " << m->get_ino() << " from mds" << mds << " premature (!!) reap on mds" << other << endl;
      // delay!
      cap_reap_queue[in->ino()][other] = m;
      return;
    }
  }

  assert(in);
  
  // stale?
  if (m->get_special() == MClientFileCaps::FILECAP_STALE) {
    dout(5) << "handle_file_caps on ino " << m->get_ino() << " seq " << m->get_seq() << " from mds" << mds << " now stale" << endl;
    
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
      dout(5) << "handle_file_caps on ino " << m->get_ino() << " from mds" << mds << " delayed reap on mds" << m->get_mds() << endl;
      
      // process delayed reap
      handle_file_caps( cap_reap_queue[in->ino()][mds] );

      cap_reap_queue[in->ino()].erase(mds);
      if (cap_reap_queue[in->ino()].empty())
        cap_reap_queue.erase(in->ino());
    }
    delete m;
    return;
  }

  // release?
  if (m->get_special() == MClientFileCaps::FILECAP_RELEASE) {
    dout(5) << "handle_file_caps on ino " << m->get_ino() << " from mds" << mds << " release" << endl;
    assert(in->caps.count(mds));
    in->caps.erase(mds);
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

    if (in->caps.empty()) {
      //dout(0) << "did put_inode" << endl;
      put_inode(in);
    } else {
      //dout(0) << "didn't put_inode" << endl;
    }
    delete m;
    return;
  }


  // don't want?
  if (in->file_caps_wanted() == 0) {
    dout(5) << "handle_file_caps on ino " << m->get_ino() 
            << " seq " << m->get_seq() 
            << " " << cap_string(m->get_caps()) 
            << ", which we don't want caps for, releasing." << endl;
    m->set_caps(0);
    m->set_wanted(0);
    messenger->send_message(m, m->get_source_inst(), m->get_source_port());
    return;
  }

  assert(in->caps.count(mds));

  // update per-mds caps
  const int old_caps = in->caps[mds].caps;
  const int new_caps = m->get_caps();
  in->caps[mds].caps = new_caps;
  in->caps[mds].seq = m->get_seq();
  dout(5) << "handle_file_caps on in " << m->get_ino() 
          << " mds" << mds << " seq " << m->get_seq() 
          << " caps now " << cap_string(new_caps) 
          << " was " << cap_string(old_caps) << endl;
  
  // did file size decrease?
  if ((old_caps & new_caps & CAP_FILE_RDCACHE) &&
      in->inode.size > m->get_inode().size) {
    dout(10) << "**** file size decreased from " << in->inode.size << " to " << m->get_inode().size << " FIXME" << endl;
    // must have been a truncate() by someone.
    // trim the buffer cache
    // ***** fixme write me ****

    in->file_wr_size = m->get_inode().size; //??
  }

  // update inode
  in->inode = m->get_inode();      // might have updated size... FIXME this is overkill!

  // preserve our (possibly newer) file size, mtime
  if (in->file_wr_size > in->inode.size)
    m->get_inode().size = in->inode.size = in->file_wr_size;
  if (in->file_wr_mtime > in->inode.mtime)
    m->get_inode().mtime = in->inode.mtime = in->file_wr_mtime;



  if (g_conf.client_oc) {
    // caching on, use FileCache.
    Context *onimplement = 0;
    if (old_caps & ~new_caps) {     // this mds is revoking caps
      if (in->fc.get_caps() & ~(in->file_caps()))   // net revocation
        onimplement = new C_Client_ImplementedCaps(this, m, in);
      else {
        implemented_caps(m, in);        // ack now.
      }
    }
    in->fc.set_caps(new_caps, onimplement);
  } else {
    // caching off.

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
    if (new_caps & CAP_FILE_LAZYIO) {
      for (list<Cond*>::iterator it = in->waitfor_lazy.begin();
           it != in->waitfor_lazy.end();
           it++) {
        dout(5) << "signaling lazy waiter " << *it << endl;
        (*it)->Signal();
      }
      in->waitfor_lazy.clear();
    }

    // ack?
    if (old_caps & ~new_caps) {
      if (in->sync_writes) {
        // wait for sync writes to finish
        dout(5) << "sync writes in progress, will ack on finish" << endl;
        in->waitfor_no_write.push_back(new C_Client_ImplementedCaps(this, m, in));
      } else {
        // ok now
        implemented_caps(m, in);
      }
    } else {
      // discard
      delete m;
    }
  }
}

void Client::implemented_caps(MClientFileCaps *m, Inode *in)
{
  dout(5) << "implemented_caps " << cap_string(m->get_caps()) 
          << ", acking to " << m->get_source() << endl;

  if (in->file_caps() == 0) {
    in->file_wr_mtime = 0;
    in->file_wr_size = 0;
  }

  messenger->send_message(m, m->get_source_inst(), m->get_source_port());
}


void Client::release_caps(Inode *in,
                          int retain)
{
  dout(5) << "releasing caps on ino " << in->inode.ino << dec
          << " had " << cap_string(in->file_caps())
          << " retaining " << cap_string(retain) 
	  << " want " << cap_string(in->file_caps_wanted())
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
      messenger->send_message(m, mdsmap->get_inst(it->first), MDS_PORT_LOCKER);
    }
  }
  
  if (in->file_caps() == 0) {
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
                            mdsmap->get_inst(it->first), MDS_PORT_LOCKER);
  }
}



// -------------------
// fs ops

int Client::mount()
{
  client_lock.Lock();

  assert(!mounted);  // caller is confused?

  // FIXME mds map update race with mount.

  dout(2) << "sending boot msg to monitor" << endl;
  if (mdsmap) 
    delete mdsmap;
  int mon = monmap->pick_mon();
  messenger->send_message(new MClientBoot(),
			  monmap->get_inst(mon));
  
  while (!mdsmap)
    mount_cond.Wait(client_lock);
  
  dout(2) << "mounting" << endl;
  MClientMount *m = new MClientMount();

  int who = 0; // mdsmap->get_root();  // mount at root, for now
  messenger->send_message(m, 
			  mdsmap->get_inst(who), 
			  MDS_PORT_SERVER);
  
  while (!mounted)
    mount_cond.Wait(client_lock);

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

void Client::handle_mount_ack(MClientMountAck *m)
{
  // mdsmap!
  if (!mdsmap) mdsmap = new MDSMap;
  mdsmap->decode(m->get_mds_map_state());

  // we got osdmap!
  osdmap->decode(m->get_osd_map_state());

  dout(2) << "mounted" << endl;
  mounted = true;
  mount_cond.Signal();

  delete m;
}


int Client::unmount()
{
  client_lock.Lock();

  assert(mounted);  // caller is confused?

  dout(2) << "unmounting" << endl;
  unmounting = true;

  // NOTE: i'm assuming all caches are already flushing (because all files are closed).
  assert(fh_map.empty());
  
  // empty lru cache
  lru.lru_set_max(0);
  trim_cache();

  if (g_conf.client_oc) {
    // release any/all caps
    for (hash_map<inodeno_t, Inode*>::iterator p = inode_map.begin();
         p != inode_map.end();
         p++) {
      Inode *in = p->second;
      if (!in->caps.empty()) {
        in->fc.release_clean();
        if (in->fc.is_dirty()) {
          dout(10) << "unmount residual caps on " << in->ino() << ", flushing" << endl;
          in->fc.empty(new C_Client_CloseRelease(this, in));
        } else {
          dout(10) << "unmount residual caps on " << in->ino()  << ", releasing" << endl;
          release_caps(in);
        }
      }
    }
  }
  
  // FIXME hack dont wait
  //if (0) {
  while (lru.lru_get_size() > 0 || 
	 !inode_map.empty()) {
    dout(2) << "cache still has " << lru.lru_get_size() 
	    << "+" << inode_map.size() << " items" 
	    << ", waiting (presumably for safe or for caps to be released?)"
	    << endl;
    dump_cache();
    mount_cond.Wait(client_lock);
  }
  assert(lru.lru_get_size() == 0);
  assert(inode_map.empty());
  //}
  
  // unsafe writes
  if (!g_conf.client_oc) {
    while (unsafe_sync_write > 0) {
      dout(2) << unsafe_sync_write << " unsafe_sync_writes, waiting" 
              << endl;
      mount_cond.Wait(client_lock);
    }
  }
  
  // send unmount!
  Message *req = new MGenericMessage(MSG_CLIENT_UNMOUNT);
  messenger->send_message(req, mdsmap->get_inst(0), MDS_PORT_SERVER);

  while (mounted)
    mount_cond.Wait(client_lock);

  // stop cleaner/renewal thread
  capcache->kill_cleaner();
  capcache->kill_renewer();

  dout(2) << "unmounted" << endl;

  client_lock.Unlock();
  return 0;
}

void Client::handle_unmount_ack(Message* m)
{
  dout(1) << "got unmount ack" << endl;
  mounted = false;
  mount_cond.Signal();
  delete m;
}



// namespace ops

int Client::link(const char *existing, const char *newname,
		 __int64_t uid, __int64_t gid) 
{
  client_lock.Lock();
  dout(3) << "op: client->link(\"" << existing << "\", \"" << newname << "\");" << endl;
  tout << "link" << endl;
  tout << existing << endl;
  tout << newname << endl;

  // fix uid/gid if not supplied
  // get it from the system
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }
  
  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}


  // main path arg is new link name
  // sarg is target (existing file)


  MClientRequest *req = new MClientRequest(MDS_OP_LINK, whoami);
  req->set_path(newname);
  req->set_sarg(existing);
  
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);
  
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  
  insert_trace(reply);
  delete reply;
  dout(10) << "link result is " << res << endl;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}


int Client::unlink(const char *relpath, __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                                
  // get it from the system                                                     
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 102+whoami;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

  dout(3) << "op: client->unlink\(\"" << path << "\");" << endl;
  tout << "unlink" << endl;
  tout << path << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_UNLINK, whoami);
  req->set_path(path);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  if (res == 0) {
    // remove from local cache
    filepath fp(path);
    Dentry *dn = lookup(fp);
    if (dn) {
      assert(dn->inode);
      unlink(dn);
    }
  }
  insert_trace(reply);
  delete reply;
  dout(10) << "unlink result is " << res << endl;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::rename(const char *relfrom, const char *relto,
		   __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                              
  // get it from the system
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

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
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  insert_trace(reply);
  delete reply;
  dout(10) << "rename result is " << res << endl;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}

// dirs

int Client::mkdir(const char *relpath, mode_t mode, __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied
  // get it from the system
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);
  //Ticket *tk = get_user_ticket(getuid(), getgid());
  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

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
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  insert_trace(reply);
  delete reply;
  dout(10) << "mkdir result is " << res << endl;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::rmdir(const char *relpath, __int64_t uid, __int64_t gid)
{
  client_lock.Lock();
  
  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

  dout(3) << "op: client->rmdir(\"" << path << "\");" << endl;
  tout << "rmdir" << endl;
  tout << path << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_RMDIR, whoami);
  req->set_path(path);
 
  // FIXME where does FUSE maintain user information
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

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
  insert_trace(reply);  
  delete reply;
  dout(10) << "rmdir result is " << res << endl;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}

// symlinks
  
int Client::symlink(const char *reltarget, const char *rellink,
		    __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

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
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  insert_trace(reply);  //FIXME assuming trace of link, not of target
  delete reply;
  dout(10) << "symlink result is " << res << endl;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::readlink(const char *relpath, char *buf, off_t size,
		     __int64_t uid, __int64_t gid) 
{ 
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

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

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;  // return length in bytes (to mimic the system call)
}



// inode stuff

int Client::_lstat(const char *path, int mask, Inode **in,
		   __int64_t uid, __int64_t gid)
{  
  MClientRequest *req = 0;
  filepath fpath(path);
  
  // check whether cache content is fresh enough
  int res = 0;

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }
  // the ticket should be used since it must look at
  // execute permissions in path
  //Ticket *tk = get_user_ticket(uid, gid);

  Dentry *dn = lookup(fpath);
  inode_t inode;
  time_t now = time(NULL);
  if (dn && 
      now <= dn->inode->valid_until &&
      ((dn->inode->inode.mask & INODE_MASK_ALL_STAT) == INODE_MASK_ALL_STAT)) {
    inode = dn->inode->inode;
    dout(10) << "lstat cache hit w/ sufficient inode.mask, valid until " << dn->inode->valid_until << endl;
    
    if (g_conf.client_cache_stat_ttl == 0)
      dn->inode->valid_until = 0;           // only one stat allowed after each readdir

    *in = dn->inode;
  } else {  
    // FIXME where does FUSE maintain user information
    //struct fuse_context *fc = fuse_get_context();
    //req->set_caller_uid(fc->uid);
    //req->set_caller_gid(fc->gid);
    
    req = new MClientRequest(MDS_OP_LSTAT, whoami);
    req->set_iarg(mask);
    req->set_path(fpath);

    MClientReply *reply = make_request(req);
    res = reply->get_result();
    dout(10) << "lstat res is " << res << endl;
    if (res == 0) {
      //Transfer information from reply to stbuf
      inode = reply->get_inode();
      
      //Update metadata cache
      *in = insert_trace(reply);
    }

    delete reply;

    if (res != 0) 
      *in = 0;     // not a success.
  }
     
  return res;
}


void Client::fill_stat(inode_t& inode, struct stat *st) 
{
  memset(st, 0, sizeof(struct stat));
  st->st_ino = inode.ino;
  st->st_mode = inode.mode;
  st->st_nlink = inode.nlink;
  st->st_uid = inode.uid;
  st->st_gid = inode.gid;
  st->st_ctime = inode.ctime;
  st->st_atime = inode.atime;
  st->st_mtime = inode.mtime;
  st->st_size = inode.size;
  st->st_blocks = inode.size ? ((inode.size - 1) / 4096 + 1):0;
  st->st_blksize = 4096;
}

void Client::fill_statlite(inode_t& inode, struct statlite *st) 
{
  memset(st, 0, sizeof(struct stat));
  st->st_ino = inode.ino;
  st->st_mode = inode.mode;
  st->st_nlink = inode.nlink;
  st->st_uid = inode.uid;
  st->st_gid = inode.gid;
#ifndef DARWIN
  // FIXME what's going on here with darwin?
  st->st_ctime = inode.ctime;
  st->st_atime = inode.atime;
  st->st_mtime = inode.mtime;
#endif
  st->st_size = inode.size;
  st->st_blocks = inode.size ? ((inode.size - 1) / 4096 + 1):0;
  st->st_blksize = 4096;
  
  /*
  S_REQUIREBLKSIZE(st->st_litemask);
  if (inode.mask & INODE_MASK_BASE) S_REQUIRECTIME(st->st_litemask);
  if (inode.mask & INODE_MASK_SIZE) {
    S_REQUIRESIZE(st->st_litemask);
    S_REQUIREBLOCKS(st->st_litemask);
  }
  if (inode.mask & INODE_MASK_MTIME) S_REQUIREMTIME(st->st_litemask);
  if (inode.mask & INODE_MASK_ATIME) S_REQUIREATIME(st->st_litemask);
  */
}


int Client::lstat(const char *relpath, struct stat *stbuf,
		  __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

  dout(3) << "op: client->lstat(\"" << path << "\", &st);" << endl;
  tout << "lstat" << endl;
  tout << path << endl;

  Inode *in = 0;

  int res = _lstat(path, INODE_MASK_ALL_STAT, &in);
  if (res == 0) {
    assert(in);
    fill_stat(in->inode,stbuf);
    dout(10) << "stat sez size = " << in->inode.size << " ino = " << stbuf->st_ino << endl;
  }

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}


int Client::lstatlite(const char *relpath, struct statlite *stl,
		      __int64_t uid, __int64_t gid)
{
  client_lock.Lock();
   
  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

  dout(3) << "op: client->lstatlite(\"" << path << "\", &st);" << endl;
  tout << "lstatlite" << endl;
  tout << path << endl;

  // make mask
  int mask = INODE_MASK_BASE | INODE_MASK_PERM;
  if (S_ISVALIDSIZE(stl->st_litemask) || 
      S_ISVALIDBLOCKS(stl->st_litemask)) mask |= INODE_MASK_SIZE;
  if (S_ISVALIDMTIME(stl->st_litemask)) mask |= INODE_MASK_MTIME;
  if (S_ISVALIDATIME(stl->st_litemask)) mask |= INODE_MASK_ATIME;
  
  Inode *in = 0;
  int res = _lstat(path, mask, &in);
  
  if (res == 0) {
    fill_statlite(in->inode,stl);
    dout(10) << "stat sez size = " << in->inode.size << " ino = " << in->inode.ino << endl;
  }

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}



int Client::chmod(const char *relpath, mode_t mode,
		  __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied
  // get it from the system
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

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
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);
  
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  insert_trace(reply);  
  delete reply;
  dout(10) << "chmod result is " << res << endl;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}

//int Client::chown(const char *relpath, uid_t uid, gid_t gid,
//		  __int64_t uid, __int64_t gid)
int Client::chown(const char *relpath, __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

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
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  //FIXME enforce caller uid rights?

  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  insert_trace(reply);  
  delete reply;
  dout(10) << "chown result is " << res << endl;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}

int Client::utime(const char *relpath, struct utimbuf *buf,
		  __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

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
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  insert_trace(reply);  
  delete reply;
  dout(10) << "utime result is " << res << endl;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}




int Client::mknod(const char *relpath, mode_t mode,
		  __int64_t uid, __int64_t gid) 
{ 
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

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
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  insert_trace(reply);  

  dout(10) << "mknod result is " << res << endl;

  delete reply;

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();
  return res;
}



  
//readdir usually include inode info for each entry except of locked entries

//
// getdir

// fyi: typedef int (*dirfillerfunc_t) (void *handle, const char *name, int type, inodeno_t ino);

int Client::getdir(const char *relpath, map<string,inode_t>& contents,
		   __int64_t uid, __int64_t gid) 
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

  dout(3) << "op: client->getdir(\"" << path << "\", dir_contents);" << endl;
  tout << "getdir" << endl;
  tout << path << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_READDIR, whoami);
  req->set_path(path); 

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  //FIXME enforce caller uid rights?
   
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  insert_trace(reply);  

  if (res == 0) {

    // dir contents to cache!
    inodeno_t ino = reply->get_ino();
    Inode *diri = inode_map[ ino ];
    assert(diri);
    assert(diri->inode.mode & INODE_MODE_DIR);

    // add . and ..?
    string dot(".");
    contents[dot] = diri->inode;
    if (diri != root) {
      string dotdot("..");
      contents[dotdot] = diri->dn->dir->parent_inode->inode;
    }

    if (!reply->get_dir_in().empty()) {
      // only open dir if we're actually adding stuff to it!
      Dir *dir = diri->open_dir();
      assert(dir);
      time_t now = time(NULL);
      
      list<string>::const_iterator pdn = reply->get_dir_dn().begin();
      for (list<InodeStat*>::const_iterator pin = reply->get_dir_in().begin();
           pin != reply->get_dir_in().end(); 
           ++pin, ++pdn) {

	if (*pdn == ".") 
	  continue;

	// count entries
        res++;

        // put in cache
        Inode *in = this->insert_inode(dir, *pin, *pdn);
        
        if (g_conf.client_cache_stat_ttl)
          in->valid_until = now + g_conf.client_cache_stat_ttl;
        else if (g_conf.client_cache_readdir_ttl)
          in->valid_until = now + g_conf.client_cache_readdir_ttl;
        
        // contents to caller too!
        contents[*pdn] = in->inode;
      }

      if (dir->is_empty())
	close_dir(dir);
    }
    

    // FIXME: remove items in cache that weren't in my readdir?
    // ***
  }

  delete reply;     //fix thing above first

  //put_user_ticket(tk);
  client_lock.Unlock();
  return res;
}


/** POSIX stubs **/

DIR *Client::opendir(const char *name, __int64_t uid, __int64_t gid) 
{
  DirResult *d = new DirResult;
  d->size = getdir(name, d->contents);
  d->p = d->contents.begin();
  d->off = 0;
  return (DIR*)d;
}

int Client::closedir(DIR *dir, __int64_t uid, __int64_t gid) 
{
  DirResult *d = (DirResult*)dir;
  delete d;
  return 0;
}

//struct dirent {
//  ino_t          d_ino;       /* inode number */
//  off_t          d_off;       /* offset to the next dirent */
//  unsigned short d_reclen;    /* length of this record */
//  unsigned char  d_type;      /* type of file */
//  char           d_name[256]; /* filename */
//};

struct dirent *Client::readdir(DIR *dirp, __int64_t uid, __int64_t gid)
{
  
  DirResult *d = (DirResult*)dirp;

  // end of dir?
  if (d->p == d->contents.end()) 
    return 0;

  // fill the dirent
  d->dp.d_dirent.d_ino = d->p->second.ino;
#ifndef __CYGWIN__
#ifndef DARWIN
  if (d->p->second.is_symlink())
    d->dp.d_dirent.d_type = DT_LNK;
  else if (d->p->second.is_dir())
    d->dp.d_dirent.d_type = DT_DIR;
  else if (d->p->second.is_file())
    d->dp.d_dirent.d_type = DT_REG;
  else
    d->dp.d_dirent.d_type = DT_UNKNOWN;

  d->dp.d_dirent.d_off = d->off;
  d->dp.d_dirent.d_reclen = 1; // all records are length 1 (wrt offset, seekdir, telldir, etc.)
#endif // DARWIN
#endif

  strncpy(d->dp.d_dirent.d_name, d->p->first.c_str(), 256);

  // move up
  ++d->off;
  ++d->p;

  return &d->dp.d_dirent;
}
 
void Client::rewinddir(DIR *dirp, __int64_t uid, __int64_t gid)
{
  DirResult *d = (DirResult*)dirp;
  d->p = d->contents.begin();
  d->off = 0;
}
 
off_t Client::telldir(DIR *dirp, __int64_t uid, __int64_t gid)
{
  DirResult *d = (DirResult*)dirp;
  return d->off;
}

void Client::seekdir(DIR *dirp, off_t offset, __int64_t uid, __int64_t gid)
{
  DirResult *d = (DirResult*)dirp;

  d->p = d->contents.begin();
  d->off = 0;

  if (offset >= d->size) offset = d->size-1;
  while (offset > 0) {
    ++d->p;
    ++d->off;
    --offset;
  }
}

struct dirent_plus *Client::readdirplus(DIR *dirp,
					__int64_t uid, __int64_t gid)
{
  DirResult *d = (DirResult*)dirp;

  // end of dir?
  if (d->p == d->contents.end()) 
    return 0;

  // fill the dirent
  d->dp.d_dirent.d_ino = d->p->second.ino;
#ifndef __CYGWIN__
#ifndef DARWIN
  if (d->p->second.is_symlink())
    d->dp.d_dirent.d_type = DT_LNK;
  else if (d->p->second.is_dir())
    d->dp.d_dirent.d_type = DT_DIR;
  else if (d->p->second.is_file())
    d->dp.d_dirent.d_type = DT_REG;
  else
    d->dp.d_dirent.d_type = DT_UNKNOWN;

  d->dp.d_dirent.d_off = d->off;
  d->dp.d_dirent.d_reclen = 1; // all records are length 1 (wrt offset, seekdir, telldir, etc.)
#endif // DARWIN
#endif

  strncpy(d->dp.d_dirent.d_name, d->p->first.c_str(), 256);

  // plus
  if ((d->p->second.mask & INODE_MASK_ALL_STAT) == INODE_MASK_ALL_STAT) {
    // have it
    fill_stat(d->p->second, &d->dp.d_stat);
    d->dp.d_stat_err = 0;
  } else {
    // don't have it, stat it
    string path = d->path;
    path += "/";
    path += d->p->first;
    d->dp.d_stat_err = lstat(path.c_str(), &d->dp.d_stat);
  }

  // move up
  ++d->off;
  ++d->p;

  return &d->dp;
}

/*
struct dirent_lite *Client::readdirlite(DIR *dirp)
{
  DirResult *d = (DirResult*)dirp;

  // end of dir?
  if (d->p == d->contents.end()) 
    return 0;

  // fill the dirent
  d->dp.d_dirent.d_ino = d->p->second.ino;
  if (d->p->second.is_symlink())
    d->dp.d_dirent.d_type = DT_LNK;
  else if (d->p->second.is_dir())
    d->dp.d_dirent.d_type = DT_DIR;
  else if (d->p->second.is_file())
    d->dp.d_dirent.d_type = DT_REG;
  else
    d->dp.d_dirent.d_type = DT_UNKNOWN;
  strncpy(d->dp.d_dirent.d_name, d->p->first.c_str(), 256);

  d->dp.d_dirent.d_off = d->off;
  d->dp.d_dirent.d_reclen = 1; // all records are length 1 (wrt offset, seekdir, telldir, etc.)

  // plus
  if ((d->p->second.mask & INODE_MASK_ALL_STAT) == INODE_MASK_ALL_STAT) {
    // have it
    fill_statlite(d->p->second,d->dp.d_stat);
    d->dp.d_stat_err = 0;
  } else {
    // don't have it, stat it
    string path = p->path;
    path += "/";
    path += p->first;
    d->dp.d_statlite
    d->dp.d_stat_err = lstatlite(path.c_str(), &d->dp.d_statlite);
  }

  // move up
  ++d->off;
  ++d->p;

  return &d->dp;
}
*/






/****** file i/o **********/

int Client::open(const char *relpath, int flags, __int64_t uid, __int64_t gid) 
{
  client_lock.Lock();


  // fix uid/gid if not supplied                                   
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }
  
  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}


  string abspath;
  mkabspath(relpath, abspath);
  const char *path = abspath.c_str();

  // client access prediction stuff
  /*
  inodeno_t this_inode = path_map[abspath];
  cout << "This inode is " << this_inode << endl;

  if (this_inode != inodeno_t() && successor_inode[uid] != inodeno_t())
    predicter[uid].add_observation(this_inode, successor_inode[uid]);
  
  inodeno_t prediction = predicter[uid].predict_successor(this_inode);
  
  if (prediction == inodeno_t()) {
    cout << "Could not make confident prediction" << endl;
  }
  else {
    cout << "Predicted access of " << prediction << endl;
  }
  */

  dout(3) << "op: fh = client->open(\"" << path << "\", " << flags << ");" << endl;
  tout << "open" << endl;
  tout << path << endl;
  tout << flags << endl;

  int cmode = 0;
  bool tryauth = false;
  if (flags & O_LAZY) 
    cmode = FILE_MODE_LAZY;
  else if (flags & O_WRONLY) {
    cmode = FILE_MODE_W;
    tryauth = true;
  } else if (flags & O_RDWR) {
    cmode = FILE_MODE_RW;
    tryauth = true;
  } else if (flags & O_APPEND) {
    cmode = FILE_MODE_W;
    tryauth = true;
  } else
    cmode = FILE_MODE_R;

  // go
  MClientRequest *req = new MClientRequest(MDS_OP_OPEN, whoami);
  req->set_path(path);
  req->set_iarg(flags);
  req->set_iarg2(cmode);

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  // add prediction info
  //string pred_string;
  //if (prediction != inodeno_t()) {
  //char convertInode[sizeof(prediction)];
  //memcpy(convertInode, &prediction, sizeof(convertInode));
  //pred_string = string(convertInode, 8);
  //}

  MClientReply *reply = make_request(req, tryauth); // try auth if writer

  assert(reply);
  dout(3) << "op: open_files[" << reply->get_result() << "] = fh;  // fh = " << reply->get_result() << endl;
  tout << reply->get_result() << endl;

  insert_trace(reply);  
  int result = reply->get_result();

  // success?
  fh_t fh = 0;
  if (result >= 0) {
    // yay
    Fh *f = new Fh;
    f->mode = cmode;

    // inode
    f->inode = inode_map[reply->get_ino()];
    assert(f->inode);
    f->inode->get();

    // note successor relationships
    //if (this_inode == inodeno_t() && successor_inode[uid] != inodeno_t())
    //predicter[uid].add_observation(reply->get_ino(), successor_inode[uid]);
    
    // update successor
    //path_map[abspath] = reply->get_ino();
    //successor_inode[uid] = reply->get_ino();

    if (cmode & FILE_MODE_R) f->inode->num_open_rd++;
    if (cmode & FILE_MODE_W) f->inode->num_open_wr++;
    if (cmode & FILE_MODE_LAZY) f->inode->num_open_lazy++;

    // caps included?
    int mds = reply->get_source().num();

    if (f->inode->caps.empty()) {// first caps?
      dout(7) << " first caps on " << f->inode->inode.ino << endl;
      f->inode->get();
    }

    int new_caps = reply->get_file_caps();

    // need security caps? check if I even asked for one
    ExtCap ext_cap = reply->get_ext_cap();
    
    dout(3) << "Received a " << ext_cap.mode() << " capability for uid: "
	 << ext_cap.get_uid() << " for inode: " << ext_cap.get_ino() << endl;

    // cache it
    f->inode->set_ext_cap(uid, &ext_cap);
    capcache->cache_cap(f->inode->ino(), uid, ext_cap);
    // presumably i want to renew it later
    //caps_in_use[uid].insert(ext_cap.get_id());
    capcache->open_cap(uid, ext_cap.get_id());

    assert(reply->get_file_caps_seq() >= f->inode->caps[mds].seq);
    if (reply->get_file_caps_seq() > f->inode->caps[mds].seq) {   
      dout(7) << "open got caps " << cap_string(new_caps)
              << " for " << f->inode->ino() 
              << " seq " << reply->get_file_caps_seq() 
              << " from mds" << mds 
	      << endl;


      int old_caps = f->inode->caps[mds].caps;
      f->inode->caps[mds].caps = new_caps;
      f->inode->caps[mds].seq = reply->get_file_caps_seq();

      // we shouldn't ever lose caps at this point.
      // actually, we might...?
      assert((old_caps & ~f->inode->caps[mds].caps) == 0);

      if (g_conf.client_oc)
        f->inode->fc.set_caps(new_caps);

    } else {
      dout(7) << "open got SAME caps " << cap_string(new_caps) 
              << " for " << f->inode->ino() 
              << " seq " << reply->get_file_caps_seq() 
              << " from mds" << mds 
	      << endl;
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

  //put_user_ticket(tk);
  trim_cache();
  client_lock.Unlock();

  return result;
}





void Client::close_release(Inode *in)
{
  dout(10) << "close_release on " << in->ino() << endl;
  dout(10) << " wr " << in->num_open_wr << " rd " << in->num_open_rd
	   << " dirty " << in->fc.is_dirty() << " cached " << in->fc.is_cached() << endl;

  if (!in->num_open_rd) 
    in->fc.release_clean();

  int retain = 0;
  if (in->num_open_wr || in->fc.is_dirty()) retain |= CAP_FILE_WR | CAP_FILE_WRBUFFER | CAP_FILE_WREXTEND;
  if (in->num_open_rd || in->fc.is_cached()) retain |= CAP_FILE_RD | CAP_FILE_RDCACHE;

  release_caps(in, retain);              // release caps now.
}

void Client::close_safe(Inode *in)
{
  dout(10) << "close_safe on " << in->ino() << endl;
  put_inode(in);
  if (unmounting) 
    mount_cond.Signal();
}

int Client::close(fh_t fh, __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                            
  // get it from the system                                                
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

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
    in->num_open_rd--;
  if (f->mode & FILE_MODE_W)
    in->num_open_wr--;
  int after = in->file_caps_wanted();

  // does this change what caps we want?
  if (before != after && after)
    update_caps_wanted(in);

  // stop using capability
  //caps_in_use[uid].erase(in->get_ext_cap(uid)->get_id());

  //capcache->close_cap(uid, capcache->get_cache_cap(in->ino(), uid)->get_id());

  // hose fh
  fh_map.erase(fh);
  delete f;

  // release caps right away?
  dout(10) << "num_open_rd " << in->num_open_rd << "  num_open_wr " << in->num_open_wr << endl;

  if (g_conf.client_oc) {
    // caching on.
    if (in->num_open_rd == 0 && in->num_open_wr == 0) {
      in->fc.empty(new C_Client_CloseRelease(this, in));
    } 
    else if (in->num_open_rd == 0) {
      in->fc.release_clean();
      close_release(in);
    } 
    else if (in->num_open_wr == 0) {
      in->fc.flush_dirty(new C_Client_CloseRelease(this,in));
    }

    // pin until safe?
    if (in->num_open_wr == 0 && !in->fc.all_safe()) {
      dout(10) << "pinning ino " << in->ino() << " until safe" << endl;
      in->get();
      in->fc.add_safe_waiter(new C_Client_CloseSafe(this, in));
    }
  } else {
    // caching off.
    if (in->num_open_rd == 0 && in->num_open_wr == 0) {
      dout(10) << "  releasing caps on " << in->ino() << endl;
      release_caps(in);              // release caps now.
    }
  }
  
  put_inode( in );
  int result = 0;

  //put_user_ticket(tk);
  client_lock.Unlock();
  return result;
}



// ------------
// read, write


off_t Client::lseek(fh_t fh, off_t offset, int whence)
{
  client_lock.Lock();
  dout(3) << "op: client->lseek(" << fh << ", " << offset << ", " << whence << ");" << endl;

  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;

  switch (whence) {
  case SEEK_SET: 
    f->pos = offset;
    break;

  case SEEK_CUR:
    f->pos += offset;
    break;

  case SEEK_END:
    f->pos = in->inode.size + offset;
    break;

  default:
    assert(0);
  }
  
  off_t pos = f->pos;
  client_lock.Unlock();

  return pos;
}


// blocking osd interface

int Client::read(fh_t fh, char *buf, off_t size, off_t offset,
		 __int64_t uid, __int64_t gid) 
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  dout(3) << "op: client->read(" << fh << ", buf, " << size << ", " << offset << ");   // that's " << offset << "~" << size << endl;
  tout << "read" << endl;
  tout << fh << endl;
  tout << size << endl;
  tout << offset << endl;

  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;

  if (offset < 0) 
    offset = f->pos;

  bool lazy = f->mode == FILE_MODE_LAZY;

  // grab security cap for file (mode should always be correct)
  // add that assertion
  //ExtCap *read_ext_cap = in->get_ext_cap(uid);
  ExtCap *read_ext_cap = capcache->get_cache_cap(in->ino(), uid);
  assert(read_ext_cap);
  
  // determine whether read range overlaps with file
  // ...ONLY if we're doing async io
  if (!lazy && (in->file_caps() & (CAP_FILE_WRBUFFER|CAP_FILE_RDCACHE))) {
    // we're doing buffered i/o.  make sure we're inside the file.
    // we can trust size info bc we get accurate info when buffering/caching caps are issued.
    //dout(-10) << "file size: " << in->inode.size << endl;
    if (offset > 0 && offset >= in->inode.size) {
      client_lock.Unlock();
      return 0;
    }
    if (offset + size > (off_t)in->inode.size) 
      size = (off_t)in->inode.size - offset;
    
    if (size == 0) {
      dout(-10) << "read is size=0, returning 0" << endl;
      client_lock.Unlock();
      return 0;
    }
  } else {
    // unbuffered, synchronous file i/o.  
    // or lazy.
    // defer to OSDs for file bounds.
  }
  
  bufferlist blist;   // data will go here
  int r = 0;
  int rvalue = 0;

  if (g_conf.client_oc) {
    // object cache ON
    rvalue = r = in->fc.read(offset, size, blist, client_lock, read_ext_cap);  // may block.
  } else {
    // object cache OFF -- legacy inconsistent way.

    // do we have read file cap?
    while (!lazy && (in->file_caps() & CAP_FILE_RD) == 0) {
      dout(7) << " don't have read cap, waiting" << endl;
      Cond cond;
      in->waitfor_read.push_back(&cond);
      cond.Wait(client_lock);
    }  
    // lazy cap?
    while (lazy && (in->file_caps() & CAP_FILE_LAZYIO) == 0) {
      dout(7) << " don't have lazy cap, waiting" << endl;
      Cond cond;
      in->waitfor_lazy.push_back(&cond);
      cond.Wait(client_lock);
    }
    
    // do sync read
    Cond cond;
    bool done = false;
    C_Cond *onfinish = new C_Cond(&cond, &done, &rvalue);

    r = filer->read(in->inode, offset, size, &blist, onfinish, read_ext_cap);

    assert(r >= 0);

    // wait!
    while (!done)
      cond.Wait(client_lock);
  }

  // adjust fd pos
  f->pos = offset+blist.length();

  // copy data into caller's char* buf
  blist.copy(0, blist.length(), buf);

  //dout(10) << "i read '" << blist.c_str() << "'" << endl;
  dout(10) << "read rvalue " << rvalue << ", r " << r << endl;

  // done!
  //put_user_ticket(tk);
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
    mount_cond.Signal();
  }
  client_lock.Unlock();
}

int Client::write(fh_t fh, const char *buf, off_t size, off_t offset,
		  __int64_t uid, __int64_t gid) 
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                             
  // get it from the system                                               
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  //dout(7) << "write fh " << fh << " size " << size << " offset " << offset << endl;
  dout(3) << "op: client->write(" << fh << ", buf, " << size << ", " << offset << ");" << endl;
  tout << "write" << endl;
  tout << fh << endl;
  tout << size << endl;
  tout << offset << endl;

  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;

  if (offset < 0) 
    offset = f->pos;

  bool lazy = f->mode == FILE_MODE_LAZY;

  dout(10) << "cur file size is " << in->inode.size << "    wr size " << in->file_wr_size << endl;
  
  ExtCap *write_ext_cap = capcache->get_cache_cap(in->ino(), uid);
  assert(write_ext_cap);
  
  // time it.
  utime_t start = g_clock.now();
    
  // copy into fresh buffer (since our write may be resub, async)
  bufferptr bp = buffer::copy(buf, size);
  bufferlist blist;
  blist.push_back( bp );
  
  if (g_conf.client_oc) { // buffer cache ON?
    assert(objectcacher);

    // write (this may block!)
    in->fc.write(offset, size, blist, client_lock, write_ext_cap);
    
    // adjust fd pos
    f->pos = offset+size;

  } else {
    // legacy, inconsistent synchronous write.
    dout(7) << "synchronous write" << endl;

    // do we have write file cap?
    while (!lazy && (in->file_caps() & CAP_FILE_WR) == 0) {
      dout(7) << " don't have write cap, waiting" << endl;
      Cond cond;
      in->waitfor_write.push_back(&cond);
      cond.Wait(client_lock);
    }
    while (lazy && (in->file_caps() & CAP_FILE_LAZYIO) == 0) {
      dout(7) << " don't have lazy cap, waiting" << endl;
      Cond cond;
      in->waitfor_lazy.push_back(&cond);
      cond.Wait(client_lock);
    }

    // prepare write
    Cond cond;
    bool done = false;
    C_Cond *onfinish = new C_Cond(&cond, &done);
    C_Client_HackUnsafe *onsafe = new C_Client_HackUnsafe(this);
    unsafe_sync_write++;
    in->sync_writes++;
    
    dout(20) << " sync write start " << onfinish << endl;
    
    filer->write(in->inode, offset, size, blist, 0, 
                 onfinish, onsafe, write_ext_cap
		 //, 1+((int)g_clock.now()) / 10 //f->pos // hack hack test osd revision snapshots
		 ); 
    
    // adjust fd pos
    f->pos = offset+size;

    while (!done) {
      cond.Wait(client_lock);
      dout(20) << " sync write bump " << onfinish << endl;
    }

    in->sync_writes--;
    if (in->sync_writes == 0 &&
        !in->waitfor_no_write.empty()) {
      for (list<Context*>::iterator i = in->waitfor_no_write.begin();
           i != in->waitfor_no_write.end();
           i++)
        (*i)->finish(0);
      in->waitfor_no_write.clear();
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
  //put_user_ticket(tk);
  client_lock.Unlock();
  return totalwritten;  
}


int Client::truncate(const char *file, off_t size, __int64_t uid, __int64_t gid) 
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  dout(3) << "op: client->truncate(\"" << file << "\", " << size << ");" << endl;
  tout << "truncate" << endl;
  tout << file << endl;
  tout << size << endl;


  MClientRequest *req = new MClientRequest(MDS_OP_TRUNCATE, whoami);
  req->set_path(file); 
  req->set_sizearg( size );

  // FIXME where does FUSE maintain user information
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);
  
  MClientReply *reply = make_request(req, true);
  int res = reply->get_result();
  insert_trace(reply);  
  delete reply;

  dout(10) << " truncate result is " << res << endl;

  //put_user_ticket(tk);
  client_lock.Unlock();
  return res;
}


int Client::fsync(fh_t fh, bool syncdataonly, __int64_t uid, __int64_t gid) 
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  dout(3) << "op: client->fsync(open_files[ " << fh << " ], " << syncdataonly << ");" << endl;
  tout << "fsync" << endl;
  tout << fh << endl;
  tout << syncdataonly << endl;

  int r = 0;

  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;

  dout(3) << "fsync fh " << fh << " ino " << in->inode.ino << " syncdataonly " << syncdataonly << endl;

  // metadata?
  if (!syncdataonly) {
    dout(0) << "fsync - not syncing metadata yet.. implement me" << endl;
  }

  // data?
  Cond cond;
  bool done = false;
  if (!objectcacher->commit_set(in->ino(),
                                new C_Cond(&cond, &done))) {
    // wait for callback
    while (!done) cond.Wait(client_lock);
  }

  //put_user_ticket(tk);
  client_lock.Unlock();
  return r;
}


// not written yet, but i want to link!

int Client::chdir(const char *path, __int64_t uid, __int64_t gid)
{
  // fake it for now!
  string abs;
  mkabspath(path, abs);
  dout(3) << "chdir " << path << " -> cwd now " << abs << endl;
  cwd = abs;
  return 0;
}

int Client::statfs(const char *path, struct statvfs *stbuf,
		   __int64_t uid, __int64_t gid)
{
  bzero (stbuf, sizeof (struct statvfs));
  // FIXME
  stbuf->f_bsize   = 1024;
  stbuf->f_frsize  = 1024;
  stbuf->f_blocks  = 1024 * 1024;
  stbuf->f_bfree   = 1024 * 1024;
  stbuf->f_bavail  = 1024 * 1024;
  stbuf->f_files   = 1024 * 1024;
  stbuf->f_ffree   = 1024 * 1024;
  stbuf->f_favail  = 1024 * 1024;
  stbuf->f_namemax = 1024;

  return 0;
}


int Client::lazyio_propogate(int fd, off_t offset, size_t count,
			     __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  dout(3) << "op: client->lazyio_propogate(" << fd
          << ", " << offset << ", " << count << ")" << endl;
  
  assert(fh_map.count(fd));
  Fh *f = fh_map[fd];
  Inode *in = f->inode;

  if (f->mode & FILE_MODE_LAZY) {
    // wait for lazy cap
    while ((in->file_caps() & CAP_FILE_LAZYIO) == 0) {
      dout(7) << " don't have lazy cap, waiting" << endl;
      Cond cond;
      in->waitfor_lazy.push_back(&cond);
      cond.Wait(client_lock);
    }

    if (g_conf.client_oc) {
      Cond cond;
      bool done = false;
      in->fc.flush_dirty(new C_SafeCond(&client_lock, &cond, &done));
      
      while (!done)
        cond.Wait(client_lock);
      
    } else {
      // mmm, nothin to do.
    }
  }

  //put_user_ticket(tk);
  client_lock.Unlock();
  return 0;
}

int Client::lazyio_synchronize(int fd, off_t offset, size_t count,
			       __int64_t uid, __int64_t gid)
{
  client_lock.Lock();

  // fix uid/gid if not supplied                                               
  // get it from the system                                                    
  if (g_conf.fix_client_id == 1) {
    uid = 340+whoami;
    gid = 1020;
  }
  else if (uid == -1 || gid == -1) {
    uid = getuid();
    gid = getgid();
  }

  //Ticket *tk = get_user_ticket(uid, gid);

  //if (!tk) {
  //  client_lock.Unlock();
  //  return -EPERM;
  //}

  dout(3) << "op: client->lazyio_synchronize(" << fd
          << ", " << offset << ", " << count << ")" << endl;
  
  assert(fh_map.count(fd));
  Fh *f = fh_map[fd];
  Inode *in = f->inode;
  
  if (f->mode & FILE_MODE_LAZY) {
    // wait for lazy cap
    while ((in->file_caps() & CAP_FILE_LAZYIO) == 0) {
      dout(7) << " don't have lazy cap, waiting" << endl;
      Cond cond;
      in->waitfor_lazy.push_back(&cond);
      cond.Wait(client_lock);
    }
    
    if (g_conf.client_oc) {
      in->fc.flush_dirty(0);       // flush to invalidate.
      in->fc.release_clean();
    } else {
      // mm, nothin to do.
    }
  }
  
  //put_user_ticket(tk);
  client_lock.Unlock();
  return 0;
}


// =========================================
// layout


int Client::describe_layout(int fh, FileLayout *lp)
{
  client_lock.Lock();
  dout(3) << "op: client->describe_layout(" << fh << ");" << endl;

  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;

  *lp = in->inode.layout;

  client_lock.Unlock();
  return 0;
}

int Client::get_stripe_unit(int fd)
{
  FileLayout layout;
  describe_layout(fd, &layout);
  return layout.stripe_size;
}

int Client::get_stripe_width(int fd)
{
  FileLayout layout;
  describe_layout(fd, &layout);
  return layout.stripe_size*layout.stripe_count;
}

int Client::get_stripe_period(int fd)
{
  FileLayout layout;
  describe_layout(fd, &layout);
  return layout.period();
}

int Client::enumerate_layout(int fh, list<ObjectExtent>& result,
			     off_t length, off_t offset)
{
  client_lock.Lock();
  dout(3) << "op: client->enumerate_layout(" << fh << ", " << length << ", " << offset << ");" << endl;

  assert(fh_map.count(fh));
  Fh *f = fh_map[fh];
  Inode *in = f->inode;

  // map to a list of extents
  filer->file_to_extents(in->inode, offset, length, result);

  client_lock.Unlock();
  return 0;
}



void Client::ms_handle_failure(Message *m, const entity_inst_t& inst)
{
  entity_name_t dest = inst.name;

  if (dest.is_mon()) {
    // resend to a different monitor.
    int mon = monmap->pick_mon(true);
    dout(0) << "ms_handle_failure " << dest << " inst " << inst 
            << ", resending to mon" << mon 
            << endl;
    messenger->send_message(m, monmap->get_inst(mon));
  }
  else if (dest.is_osd()) {
    objecter->ms_handle_failure(m, dest, inst);
  } 
  else if (dest.is_mds()) {
    dout(0) << "ms_handle_failure " << dest << " inst " << inst << endl;
    // help!
    assert(0);
  }
  else {
    // client?
    dout(0) << "ms_handle_failure " << dest << " inst " << inst 
            << ", dropping" << endl;
    delete m;
  }
}

