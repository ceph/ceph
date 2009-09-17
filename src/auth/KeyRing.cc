// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <errno.h>
#include <map>

#include "config.h"

#include "Crypto.h"

using namespace std;

#define NUM_CONCURRENT_KEYS 3

/*
  KeyRing is being used at the service side, for holding the temporary rotating
  key of that service
*/

class KeyRing {
  CryptoKey master;
  map<uint32_t, CryptoKey> keys;
  deque<uint32_t> keys_fifo;
  Mutex lock;
public:
  KeyRing() : lock("KeyRing") {}

  bool load_master(const char *filename);
  bool set_next_key(uint64_t id, CryptoKey& key);

  void get_master(CryptoKey& dest);
};


bool KeyRing::load_master(const char *filename)
{
  int fd = open(filename, O_RDONLY);
  if (fd < 0) {
    dout(0) << "can't open key ring file " << filename << dendl;
    return false;
  }

  // get size
  struct stat st;
  int rc = fstat(fd, &st);
  if (rc != 0) {
    dout(0) << "error stating key ring file " << filename << dendl;
    return false;
  }
  __int32_t len = st.st_size;
 
  bufferlist bl;

  bufferptr bp(len);
  int off = 0;
  while (off < len) {
    int r = read(fd, bp.c_str()+off, len-off);
    if (r < 0) {
      derr(0) << "errno on read " << strerror(errno) << dendl;
      return false;
    }
    off += r;
  }
  bl.append(bp);
  close(fd);
  
  return true;
}

bool KeyRing::set_next_key(uint64_t id, CryptoKey& key)
{
  Mutex::Locker l(lock);

  keys[id] = key;
  keys_fifo.push_back(id);

  while (keys_fifo.size() > NUM_CONCURRENT_KEYS) {
    uint32_t old_id = keys_fifo[0];
    keys_fifo.pop_front();
    map<uint32_t, CryptoKey>::iterator iter = keys.find(old_id);
    assert(iter != keys.end());
    keys.erase(iter);
  }

  return true;
}

void KeyRing::get_master(CryptoKey& dest)
{
  Mutex::Locker l(lock);

  dest = master;
}

