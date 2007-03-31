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
#ifndef __CLIENTCAPCACHE_H
#define __CLIENTCAPCACHE_H

#include "common/Cond.h"
#include "common/Thread.h"
#include "mds/MDSMap.h"

class ClientCapCache {
 public:
  void cache_cap(inodeno_t ino, uid_t user, ExtCap& cap) {
    inode_user_cap_cache[ino][user] = cap;
  }
  void clear_cap(inodeno_t ino, uid_t user) {
    inode_user_cap_cache[ino].erase(user);
  }
  void open_cap(uid_t user, cap_id_t cid) {
    caps_in_use[user].insert(cid);
  }
  void close_cap(uid_t user, cap_id_t cid) {
    caps_in_use[user].erase(cid);
  }
  bool is_open(cap_id_t cid) {
    for (map<uid_t, set<cap_id_t> >::iterator mi = caps_in_use.begin();
	 mi != caps_in_use.end();
	 mi++) {
      if (mi->second.count(cid) != 0)
	return true;
    }
    return false;
  }
  ExtCap *get_cache_cap(inodeno_t ino, uid_t user) {
    if (inode_user_cap_cache[ino].count(user) == 0)
      return 0;
    return &(inode_user_cap_cache[ino][user]);
  }
  void kill_cleaner() {
    cleaner_stop = true;
    //cleaner_cond.Signal();
    //cleaner_thread.join();
  }
  void kill_renewer() {
    renewer_stop = true;
    //renewer_cond.Signal();
    //renewer_thread.join();
  }

  ClientCapCache (Messenger *m, Client *cli, Mutex& l) : messenger(m),
							 client(cli), lock(l),
							 cleaner_stop(true),
							 cleaner_thread(this),
							 renewer_stop(false),
							 renewer_thread(this)
  {
    if (g_conf.renewal) {
      //cleaner_thread.create();
      renewer_thread.create();
    }
  }
  ~ClientCapCache () {
    cleaner_stop = true;
    renewer_stop = true;

    if (g_conf.renewal) {
      //cleaner_cond.Signal();
      //cleaner_thread.join();
      renewer_cond.Signal();
      renewer_thread.join();
    }
  }

 private:
  Messenger *messenger;
  //MDSMap *mdsmap;
  Client *client;
  Mutex& lock;
  map<inodeno_t, map<uid_t, ExtCap> > inode_user_cap_cache;
  map<uid_t, set<cap_id_t> > caps_in_use;
  
  bool cleaner_stop;
  Cond cleaner_cond;
  // expunge caps that are not in use, are expired and have no extension
  void cleaner_entry() {
    cout << "Cleaner start" << endl;
    //lock.Lock();
    /*
    while (!cleaner_stop) {
      cout << "Cleaner running" << endl;
      while (!cleaner_stop) {
	cout << "Cleaner cleaning" << endl;
	utime_t cutoff = g_clock.now();
	// clean all inodes
	for (map<inodeno_t, map<uid_t, ExtCap> >::iterator mi = inode_user_cap_cache.begin();
	     mi != inode_user_cap_cache.end();
	     mi++)
	  // clean all users
	  for (map<uid_t, ExtCap>::iterator ui = mi->second.begin();
	       ui != mi->second.begin();
	       ui++) {
	    cap_id_t cid = ui->second.get_id();
	    // if not open
	    if (!(cc->is_open(cid))) {
	      // if past cutoff
	      if (ui->second.get_te() < cutoff) {
		// if no extension
		//if () {
		//}
		cout << "Found a cap to expunge" << endl;
	      }
	    }
	  }
      }
      if (cleaner_stop) break;
      // clean every 8 minutes
      cleaner_cond.WaitInterval(lock, utime_t(480,0));
    }
    */
    //lock.Unlock();
    cout << "Cleaner finish" << endl;
  }

  class CleanerThread : public Thread {
    ClientCapCache *cc;
  public:
    CleanerThread(ClientCapCache *c) : cc(c) {}
    void *entry() {
      cc->cleaner_entry();
      return 0;
    }
  } cleaner_thread;
  
  bool renewer_stop;
  Cond renewer_cond;
  // renew caps that are in use (leaves a re-use grace period)
  void renewer_entry() {
    cout << "Renewer start" << endl;
    lock.Lock();

    while (!renewer_stop) {

      cout << "Renewer running, requesting ";
      // renewal all open caps
      MClientRenewal *renewal = new MClientRenewal();
      for (map<uid_t, set<cap_id_t> >::iterator mi = caps_in_use.begin();
	   mi != caps_in_use.end();
	   mi++) {
	renewal->add_cap_set(mi->second);
	cout << mi->second << ", ";
      }
      cout << endl;
      
      // make asynchronous renewal request
      // FIXME always send to mds 0
      if (client->mdsmap && !caps_in_use.empty()) {
	cout << "Sending renewal request" << endl;
	messenger->send_message(renewal, client->mdsmap->get_inst(0), MDS_PORT_SERVER);
      }
      
      if (renewer_stop) break;
      // clean every 4 minutes
      cout << "Renewer sleeping" << endl;
      renewer_cond.WaitInterval(lock, utime_t(g_conf.renewal_period,0));
    }

    lock.Unlock();
    cout << "Renewer finish" << endl;
  }

  class RenewerThread : public Thread {
    ClientCapCache *cc;
  public:
    RenewerThread(ClientCapCache *c) : cc(c) {}
    void *entry() {
      cc->renewer_entry();
      return 0;
    }
  } renewer_thread;
  
};

#endif
