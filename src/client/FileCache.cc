// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "include/types.h"

#include "FileCache.h"
#include "osdc/ObjectCacher.h"

#include "msg/Messenger.h"

#include "config.h"
#define dout(x)  if (x <= g_conf.debug_client) *_dout << dbeginl << g_clock.now() << " " << oc->objecter->messenger->get_myname() << ".filecache "
#define derr(x)  if (x <= g_conf.debug_client) *_derr << dbeginl << g_clock.now() << " " << oc->objecter->messenger->get_myname() << ".filecache "



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
    dout(0) << "tear_down " << unclean << " unclean bytes, purging" << dendl;
    oc->purge_set(inode.ino);
  }
}

// truncate

void FileCache::truncate(off_t olds, off_t news)
{
  dout(5) << "truncate " << olds << " -> " << news << dendl;

  // map range to objects
  list<ObjectExtent> ls;
  oc->filer.file_to_extents(inode, news, olds-news, ls);
  oc->truncate_set(inode.ino, ls);
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
    dout(10) << "set_caps setting onimplement context for " << cap_string(caps) << dendl;
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

int FileCache::get_used_caps()
{
  int used = 0;
  if (num_reading) used |= CEPH_CAP_RD;
  if (oc->set_is_cached(inode.ino)) used |= CEPH_CAP_RDCACHE;
  if (num_writing) used |= CEPH_CAP_WR;
  if (oc->set_is_dirty_or_committing(inode.ino)) used |= CEPH_CAP_WRBUFFER;
  return used;
}

void FileCache::check_caps()
{
  // calc used
  int used = get_used_caps();
  dout(10) << "check_caps used was " << cap_string(used) << dendl;

  // try to implement caps?
  // BUG? latest_caps, not least caps i've seen?
  if ((latest_caps & CEPH_CAP_RDCACHE) == 0 &&
      (used & CEPH_CAP_RDCACHE))
    release_clean();
  if ((latest_caps & CEPH_CAP_WRBUFFER) == 0 &&
      (used & CEPH_CAP_WRBUFFER))
    flush_dirty(new C_FC_CheckCaps(this));
  
  used = get_used_caps();
  dout(10) << "check_caps used now " << cap_string(used) << dendl;

  // check callbacks
  map<int, list<Context*> >::iterator p = caps_callbacks.begin();
  while (p != caps_callbacks.end()) {
    if (used == 0 || (~(p->first) & used) == 0) {
      // implemented.
      dout(10) << "used is " << cap_string(used) 
               << ", caps " << cap_string(p->first) << " implemented, doing callback(s)" << dendl;
      finish_contexts(p->second);
      map<int, list<Context*> >::iterator o = p;
      p++;
      caps_callbacks.erase(o);
    } else {
      dout(10) << "used is " << cap_string(used) 
               << ", caps " << cap_string(p->first) << " not yet implemented" << dendl;
      p++;
    }
  }
}



// read/write

int FileCache::read(off_t offset, size_t size, bufferlist& blist, Mutex& client_lock)
{
  int r = 0;

  // can i read?
  while ((latest_caps & CEPH_CAP_RD) == 0) {
    dout(10) << "read doesn't have RD cap, blocking" << dendl;
    Cond c;
    waitfor_read.insert(&c);
    c.Wait(client_lock);
    waitfor_read.erase(&c);
  }

  // inc reading counter
  num_reading++;
  
  if (latest_caps & CEPH_CAP_RDCACHE) {
    // read (and block)
    Cond cond;
    bool done = false;
    int rvalue = 0;
    C_Cond *onfinish = new C_Cond(&cond, &done, &rvalue);
    
    r = oc->file_read(inode, offset, size, &blist, onfinish);
    
    if (r == 0) {
      // block
      while (!done) 
        cond.Wait(client_lock);
      r = rvalue;
    } else {
      // it was cached.
      delete onfinish;
    }
  } else {
    r = oc->file_atomic_sync_read(inode, offset, size, &blist, client_lock);
  }

  // dec reading counter
  num_reading--;

  if (num_reading == 0 && !caps_callbacks.empty()) 
    check_caps();

  return r;
}

void FileCache::write(off_t offset, size_t size, bufferlist& blist, Mutex& client_lock)
{
  // can i write
  while ((latest_caps & CEPH_CAP_WR) == 0) {
    dout(10) << "write doesn't have WR cap, blocking" << dendl;
    Cond c;
    waitfor_write.insert(&c);
    c.Wait(client_lock);
    waitfor_write.erase(&c);
  }

  // inc writing counter
  num_writing++;

  if (size > 0) {
    if (latest_caps & CEPH_CAP_WRBUFFER) {   // caps buffered write?
      // wait? (this may block!)
      oc->wait_for_write(size, client_lock);
      
      // async, caching, non-blocking.
      oc->file_write(inode, offset, size, blist);
    } else {
      // atomic, synchronous, blocking.
      oc->file_atomic_sync_write(inode, offset, size, blist, client_lock);
    }    
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
