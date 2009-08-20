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

#ifndef __CRYPTOTOOLS_H
#define __CRYPTOTOOLS_H

#include "AuthTypes.h"

class CryptoHandler {
public:
  virtual bool encrypt(CryptoKey& secret, const bufferlist& in, bufferlist& out) = 0;
  virtual bool decrypt(CryptoKey& secret, const bufferlist& in, bufferlist& out) = 0;
};

class CryptoManager {
public:
  CryptoHandler *get_crypto(int type);
};

extern CryptoManager ceph_crypto_mgr;


#endif
