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
  ExtCap *get_cache_cap(inodeno_t ino, uid_t user) {
    if (inode_user_cap_cache[ino].count(user) == 0)
      return 0;
    return &(inode_user_cap_cache[ino][user]);
  }

  ClientCapCache (Mutex& l) : lock(l), cleaner_stop(false),
  cleaner_thread(this) { cleaner_thread.create(); }
  ~ClientCapCache () {
    cleaner_stop = true;
    cleaner_cond.Signal();
    cleaner_thread.join();
  }

 private:
  Mutex& lock;
  map<inodeno_t, map<uid_t, ExtCap> > inode_user_cap_cache;
  map<uid_t, set<cap_id_t> > caps_in_use;
  
  bool cleaner_stop;
  Cond cleaner_cond;
  void cleaner_entry() {
    cout << "Cleaner start" << endl;
    lock.Lock();
    lock.Unlock();
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
};

#endif
