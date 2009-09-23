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

#ifndef __CRYPTO_H
#define __CRYPTO_H

#include "include/types.h"

/*
 * match encoding of struct ceph_secret
 */
class CryptoKey {
protected:
  __u16 type;
  utime_t created;
  bufferptr secret;

public:
  CryptoKey() : type(0) { }
  CryptoKey(int t, utime_t c, bufferptr& s) : type(t), created(c), secret(s) { }

  void encode(bufferlist& bl) const {
    ::encode(type, bl);
    ::encode(created, bl);
    __u16 len = secret.length();
    ::encode(len, bl);
    bl.append(secret);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(type, bl);
    ::decode(created, bl);
    __u16 len;
    ::decode(len, bl);
    bl.copy(len, secret);
    secret.c_str();   // make sure it's a single buffer!
  }

  int set_secret(int type, bufferptr& s);
  bufferptr& get_secret() { return secret; }

  // --
  int create(int type);
  int encrypt(const bufferlist& in, bufferlist& out);
  int decrypt(const bufferlist& in, bufferlist& out);
};
WRITE_CLASS_ENCODER(CryptoKey);


/*
 * Driver for a particular algorithm
 */
class CryptoHandler {
public:
  virtual int create(bufferptr& secret) = 0;
  virtual int validate_secret(bufferptr& secret) = 0;
  virtual int encrypt(bufferptr& secret, const bufferlist& in, bufferlist& out) = 0;
  virtual int decrypt(bufferptr& secret, const bufferlist& in, bufferlist& out) = 0;
};


class CryptoManager {
public:
  CryptoHandler *get_crypto(int type);
};

extern CryptoManager ceph_crypto_mgr;

extern void generate_random_string(string& s, int len);


#endif
