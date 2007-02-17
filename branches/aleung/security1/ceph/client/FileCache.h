#ifndef __FILECACHE_H
#define __FILECACHE_H

#include <iostream>
using namespace std;

#include "common/Cond.h"
#include "mds/Capability.h"

#include "crypto/ExtCap.h"

class ObjectCacher;

class FileCache {
  ObjectCacher *oc;
  inode_t inode;
  
  // caps
  int latest_caps;
  map<int, list<Context*> > caps_callbacks;

  int num_reading;
  int num_writing;
  //int num_unsafe;

  // waiters
  list<Cond*> waitfor_read;
  list<Cond*> waitfor_write;
  //list<Context*> waitfor_safe;
  bool waitfor_release;

 public:
  FileCache(ObjectCacher *_oc, inode_t _inode) : 
    oc(_oc), 
    inode(_inode),
    latest_caps(0),
    num_reading(0), num_writing(0),// num_unsafe(0),
    waitfor_release(false) {}

  // waiters/waiting
  bool can_read() { return latest_caps & CAP_FILE_RD; }
  bool can_write() { return latest_caps & CAP_FILE_WR; }
  bool all_safe();// { return num_unsafe == 0; }

  void add_read_waiter(Cond *c) { waitfor_read.push_back(c); }
  void add_write_waiter(Cond *c) { waitfor_write.push_back(c); }
  void add_safe_waiter(Context *c);// { waitfor_safe.push_back(c); }

  // ...
  void flush_dirty(Context *onflush=0);
  off_t release_clean();
  void empty(Context *onempty=0);
  bool is_empty() { return !(is_cached() || is_dirty()); }
  bool is_cached();
  bool is_dirty();  

  int get_caps() { return latest_caps; }
  void set_caps(int caps, Context *onimplement=0);
  void check_caps();

  int read(off_t offset, size_t size, bufferlist& blist, Mutex& client_lock);  // may block.
  void write(off_t offset, size_t size, bufferlist& blist, Mutex& client_lock);  // may block.

};


#endif
