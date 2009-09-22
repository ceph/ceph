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

#ifndef __KEYRING_H
#define __KEYRING_H

#include "config.h"

#include "auth/Crypto.h"

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



#endif
