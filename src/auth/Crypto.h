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

#ifndef CEPH_AUTH_CRYPTO_H
#define CEPH_AUTH_CRYPTO_H

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

  int get_type() const { return type; }
  utime_t get_created() const { return created; }
  void print(std::ostream& out) const;

  int set_secret(int type, bufferptr& s);
  bufferptr& get_secret() { return secret; }
  const bufferptr& get_secret() const { return secret; }

  void encode_base64(string& s) const {
    bufferlist bl;
    encode(bl);
    bufferlist e;
    bl.encode_base64(e);
    e.append('\0');
    s = e.c_str();
  }
  void decode_base64(const string& s) {
    bufferlist e;
    e.append(s);
    bufferlist bl;
    bl.decode_base64(e);
    bufferlist::iterator p = bl.begin();
    decode(p);
  }

  // --
  int create(int type);
  int encrypt(const bufferlist& in, bufferlist& out) const;
  int decrypt(const bufferlist& in, bufferlist& out) const;

  void to_str(std::string& s) const;
};
WRITE_CLASS_ENCODER(CryptoKey);

static inline ostream& operator<<(ostream& out, const CryptoKey& k)
{
  k.print(out);
  return out;
}


/*
 * Driver for a particular algorithm
 *
 * To use these functions, you need to call ceph::crypto::init(), see
 * common/ceph_crypto.h. common_init_finish does this for you.
 */
class CryptoHandler {
public:
  virtual ~CryptoHandler() {}
  virtual int create(bufferptr& secret) = 0;
  virtual int validate_secret(bufferptr& secret) = 0;
  virtual int encrypt(const bufferptr& secret, const bufferlist& in,
		      bufferlist& out) const = 0;
  virtual int decrypt(const bufferptr& secret, const bufferlist& in,
		      bufferlist& out) const = 0;
};


class CryptoManager {
public:
  CryptoHandler *get_crypto(int type);
};

extern CryptoManager ceph_crypto_mgr;

extern int get_random_bytes(char *buf, int len);


#endif
