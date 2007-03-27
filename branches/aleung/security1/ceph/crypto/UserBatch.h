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

#ifndef __USERBATCH_H
#define __USERBATCH_H

#include "include/types.h"
#include "common/Clock.h"
#include "common/Mutex.h"
#include "common/Cond.h"

#include "messages/MClientRequest.h"
//#include "mds/Server.h"

class Server;
//class MDS;

class UserBatch {

  Server *server;
  MDS *mds;

  // is the thread initiated
  bool thread_init;
  // is the thread running
  bool batch_stop;
  // are we batching requests
  bool batching;

  Mutex batch_lock;
  Cond batch_cond;

public:
  utime_t one_req_ago;
  utime_t two_req_ago;
  uid_t user;
  gid_t user_group;

  set<MClientRequest*> batched_requests;
  bool batch_id_set;
  cap_id_t batch_id;

  class BatchThread : public Thread {
    UserBatch *batch;
  public:
    BatchThread() {}
    BatchThread  (UserBatch *ub) : batch (ub) {}
    void *entry() {
      batch->batch_entry();
      return 0;
    }
  } batch_thread;

  UserBatch(Server *serve, MDS* metads) : server(serve), mds(metads) {
    thread_init = false;
    batching = false;
    batch_stop = false;
    batch_thread = BatchThread(this);
    batch_thread.create();
  }
  ~UserBatch () {
    batch_lock.Lock();
    batch_stop = true;
    batch_cond.Signal();
    batch_lock.Unlock();
    batch_thread.join();
  }

  bool is_batching() { return batching; }
  bool should_batch(utime_t new_request_time) {
    //if (new_request_time - two_req_ago < utime_t(0,10000)) // 10ms between
    if (new_request_time > utime_t()) // always batch
      return true;
    return false;
  }
  void update_batch_time(utime_t new_request_time) {
    two_req_ago = one_req_ago;
    one_req_ago = new_request_time;
  }

  void add_to_batch(MClientRequest *req) {
    dout(1) << "Batching the request for uid:"
	    << req->get_caller_uid() << " on client:"
	    << req->get_client() << " for file:"
	    << req->get_ino() << " with client inst:"
	    << req->get_client_inst() << endl;
    
    batch_lock.Lock();
    
    // wait until the thread has initialized
    while (! thread_init)
      batch_cond.Wait(batch_lock);
    
    // was batching thread already on?
    if (batching) {
      batched_requests.insert(req);
    }
    else {
      // set the user were batching for
      user = req->get_caller_uid();
      user_group = req->get_caller_gid();

      // set the future capid
      batch_id.cid = mds->cap_id_count;
      batch_id.mds_id = mds->get_nodeid();
      mds->cap_id_count++;
      
      batching = true;
      batch_id_set = true;
      
      batched_requests.insert(req);
      
      // start the buffering now
      batch_cond.Signal();
    }
    
    batch_lock.Unlock();
    return;
  }

  void batch_entry()
  {
    cout << "batch thread start------>" << endl;
    batch_lock.Lock();
    
    // init myself and signal anyone waiting for me to init
    thread_init = true;
    batch_cond.Signal();
    
    while(!batch_stop) {
      
      // ifwe're not buffering, then,
      // were gonna get signaled when we start buffering
      // plus i need to release the lock for anyone
      // waiting for me to init
      while (!batching)
	batch_cond.Wait(batch_lock);
      
      // the sleep releases the lock and allows the dispatch
      // to insert requests into the buffer
      // sleep first, then serve cap
      batch_cond.WaitInterval(catch_lock, utime_t(5,0));
      
      // now i've slept, make cap for users
      list<inodeno_t> inode_list;
      CapGroup inode_hash;
      for (set<MClientRequest *>::iterator si = buffered_reqs.begin();
	   si != buffered_reqs.end();
	   si++) {
	inode_list.push_back((*ii)->get_ino());
	inode_hash.add_inode((*ii)->get_ino());
      }
      inode_hash.sign_list(mds->getPrvKey());
      mds->unix_groups_byhash[inode_hash.get_root_hash()]= inode_hash;
      
      ExtCap *ext_cap = new ExtCap(FILE_MODE_RW,
				   user,
				   user_group,
				   inode_hash.get_root_hash());
      ext_cap->set_type(USER_BATCH);
      ext_cap->set_id(batch_id);
      ext_cap->sign_extcap(mds->getPrvKey());
      
      // put the cap every inodes cache
      CInode *inode_cache;
      for (list<inodeno_t>::iterator ili = inode_list.begin();
	   ili != inode_list.end();
	   ili++) {
	inode_cache= mds->mdcache->inode_map[(*ili)];
	inode_cache->add_user_extcap(user, ext_cap);
      }
      
      // let requests loose
      for (set<MClientRequest *>::iterator ri = batched_requests.begin();
	   ri != batched_requests.end();
	   ri++) {
	server->handle_client_open(*ri, this);
      }
      
      batched_requests.clear();
      
      //turn batching off
      batching = false;
    }
    
    batch_lock.Unlock();
    cout << "<------batcher thread finish" << endl;
  }
  
};

#endif
