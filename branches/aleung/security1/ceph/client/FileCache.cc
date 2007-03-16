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

#include "config.h"
#include "include/types.h"

#include "FileCache.h"
#include "osdc/ObjectCacher.h"

#include "msg/Messenger.h"

#include "crypto/ExtCap.h"

#undef dout
#define dout(x)  if (x <= g_conf.debug_client) cout << g_clock.now() << " " << oc->objecter->messenger->get_myname() << ".filecache "
#define derr(x)  if (x <= g_conf.debug_client) cout << g_clock.now() << " " << oc->objecter->messenger->get_myname() << ".filecache "


// flush/release/clean

void FileCache::flush_dirty(Context *onflush)
{
  if (oc->flush_set(inode.ino, onflush)) {
    onflush->finish(0);
    delete onflush;
  }
}

off_t FileCache::release_clean()
{
  return oc->release_set(inode.ino);
}

bool FileCache::is_cached()
{
  return oc->set_is_cached(inode.ino);
}

bool FileCache::is_dirty() 
{
  return oc->set_is_dirty_or_committing(inode.ino);
}

void FileCache::empty(Context *onempty)
{
  off_t unclean = release_clean();
  bool clean = oc->flush_set(inode.ino, onempty);
  assert(!unclean == clean);

  if (clean) {
    onempty->finish(0);
    delete onempty;
  }
}


void FileCache::tear_down()
{
  off_t unclean = release_clean();
  if (unclean) {
    dout(0) << "tear_down " << unclean << " unclean bytes, purging" << endl;
    oc->purge_set(inode.ino);
  }
}

// caps

class C_FC_CheckCaps : public Context {
  FileCache *fc;
public:
  C_FC_CheckCaps(FileCache *f) : fc(f) {}
  void finish(int r) {
	fc->check_caps();
  }
};

void FileCache::set_caps(int caps, Context *onimplement) 
{
  if (onimplement) {
    dout(10) << "set_caps setting onimplement context for " << cap_string(caps) << endl;
    assert(latest_caps & ~caps);  // we should be losing caps.
    caps_callbacks[caps].push_back(onimplement);
  }
  
  latest_caps = caps;
  check_caps();  

  // kick waiters?  (did we gain caps?)
  if (can_read() && !waitfor_read.empty()) 
    for (set<Cond*>::iterator p = waitfor_read.begin();
	 p != waitfor_read.end();
	 ++p)
      (*p)->Signal();
  if (can_write() && !waitfor_write.empty()) 
    for (set<Cond*>::iterator p = waitfor_write.begin();
	 p != waitfor_write.end();
	 ++p)
      (*p)->Signal();
  
}


void FileCache::check_caps()
{
  // calc used
  int used = 0;
  if (num_reading) used |= CAP_FILE_RD;
  if (oc->set_is_cached(inode.ino)) used |= CAP_FILE_RDCACHE;
  if (num_writing) used |= CAP_FILE_WR;
  if (oc->set_is_dirty_or_committing(inode.ino)) used |= CAP_FILE_WRBUFFER;
  dout(10) << "check_caps used " << cap_string(used) << endl;

  // try to implement caps?
  // BUG? latest_caps, not least caps i've seen?
  if ((latest_caps & CAP_FILE_RDCACHE) == 0 &&
      (used & CAP_FILE_RDCACHE))
    release_clean();
  if ((latest_caps & CAP_FILE_WRBUFFER) == 0 &&
      (used & CAP_FILE_WRBUFFER))
    flush_dirty(new C_FC_CheckCaps(this));
  //if (latest_caps == 0 &&
  //  used != 0)
  //empty(new C_FC_CheckCaps(this));
  
  // check callbacks
  map<int, list<Context*> >::iterator p = caps_callbacks.begin();
  while (p != caps_callbacks.end()) {
    if (used == 0 || (~(p->first) & used) == 0) {
      // implemented.
      dout(10) << "used is " << cap_string(used) 
               << ", caps " << cap_string(p->first) << " implemented, doing callback(s)" << endl;
      finish_contexts(p->second);
      map<int, list<Context*> >::iterator o = p;
      p++;
      caps_callbacks.erase(o);
    } else {
      dout(10) << "used is " << cap_string(used) 
               << ", caps " << cap_string(p->first) << " not yet implemented" << endl;
      p++;
    }
  }
}



// read/write

int FileCache::read(off_t offset, size_t size, bufferlist& blist, Mutex& client_lock, ExtCap* read_ext_cap)
{
  int r = 0;

  // can i read?
  while ((latest_caps & CAP_FILE_RD) == 0) {
    dout(10) << "read doesn't have RD cap, blocking" << endl;
    Cond c;
    waitfor_read.insert(&c);
    c.Wait(client_lock);
    waitfor_read.erase(&c);
  }

  // inc reading counter
  num_reading++;
  
  if (latest_caps & CAP_FILE_RDCACHE) {
    // read (and block)
    Cond cond;
    bool done = false;
    int rvalue = 0;
    C_Cond *onfinish = new C_Cond(&cond, &done, &rvalue);
    
    r = oc->file_read(inode, offset, size, &blist, onfinish, read_ext_cap);
    
    if (r == 0) {
      // block
      while (!done) 
        cond.Wait(client_lock);
      r = rvalue;
    } else {
      // it was cached.
      delete onfinish;
    }
  } else { // cache is on but I can't rdcache
    r = oc->file_atomic_sync_read(inode, offset, size, &blist, client_lock, read_ext_cap);
  }

  // dec reading counter
  num_reading--;

  if (num_reading == 0 && !caps_callbacks.empty()) 
    check_caps();

  return r;
}

void FileCache::write(off_t offset, size_t size, bufferlist& blist, Mutex& client_lock, ExtCap *write_ext_cap)
{
  // can i write
  while ((latest_caps & CAP_FILE_WR) == 0) {
    dout(10) << "write doesn't have WR cap, blocking" << endl;
    Cond c;
    waitfor_write.insert(&c);
    c.Wait(client_lock);
    waitfor_write.erase(&c);
  }

  // inc writing counter
  num_writing++;

  if (latest_caps & CAP_FILE_WRBUFFER) {   // caps buffered write?
    // wait? (this may block!)
    oc->wait_for_write(size, client_lock);

    // async, caching, non-blocking.
    oc->file_write(inode, offset, size, blist, write_ext_cap);
  } else {
    // atomic, synchronous, blocking.
    oc->file_atomic_sync_write(inode, offset, size, blist, client_lock, write_ext_cap);
  }    
    
  // dec writing counter
  num_writing--;
  if (num_writing == 0 && !caps_callbacks.empty())
    check_caps();
}

bool FileCache::all_safe()
{
  return !oc->set_is_dirty_or_committing(inode.ino);
}

void FileCache::add_safe_waiter(Context *c) 
{
  bool safe = oc->commit_set(inode.ino, c);
  if (safe) {
    c->finish(0);
    delete c;
  }
}
