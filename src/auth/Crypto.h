// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
#include "include/utime.h"

#include "common/Formatter.h"
#include "include/buffer.h"

#include <string>

class CephContext;
class CryptoHandler;

/*
 * match encoding of struct ceph_secret
 */
class CryptoKey {
protected:
  __u16 type;
  utime_t created;
  bufferptr secret;

  // cache a pointer to the handler, so we don't have to look it up
  // for each crypto operation
  mutable CryptoHandler *ch;

public:
  CryptoKey() : type(0), ch(NULL) { }
  CryptoKey(int t, utime_t c, bufferptr& s) : type(t), created(c), secret(s), ch(NULL) { }

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

  int set_secret(CephContext *cct, int type, bufferptr& s);
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
  string encode_base64() const {
    string s;
    encode_base64(s);
    return s;
  }
  void decode_base64(const string& s) {
    bufferlist e;
    e.append(s);
    bufferlist bl;
    bl.decode_base64(e);
    bufferlist::iterator p = bl.begin();
    decode(p);
  }

  void encode_formatted(string label, Formatter *f, bufferlist &bl);
  void encode_plaintext(bufferlist &bl);

  // --
  int create(CephContext *cct, int type);
  void encrypt(CephContext *cct, const bufferlist& in, bufferlist& out, std::string &error) const;
  void decrypt(CephContext *cct, const bufferlist& in, bufferlist& out, std::string &error) const;

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
  virtual int get_type() const = 0;
  virtual int create(bufferptr& secret) = 0;
  virtual int validate_secret(bufferptr& secret) = 0;
  virtual void encrypt(const bufferptr& secret, const bufferlist& in,
		      bufferlist& out, std::string &error) const = 0;
  virtual void decrypt(const bufferptr& secret, const bufferlist& in,
		      bufferlist& out, std::string &error) const = 0;
};

extern int get_random_bytes(char *buf, int len);
extern uint64_t get_random(uint64_t min_val, uint64_t max_val);

class CryptoNone : public CryptoHandler {
public:
  CryptoNone() { }
  ~CryptoNone() {}
  int get_type() const {
    return CEPH_CRYPTO_NONE;
  }
  int create(bufferptr& secret);
  int validate_secret(bufferptr& secret);
  void encrypt(const bufferptr& secret, const bufferlist& in,
	      bufferlist& out, std::string &error) const;
  void decrypt(const bufferptr& secret, const bufferlist& in,
	      bufferlist& out, std::string &error) const;
};

class CryptoAES : public CryptoHandler {
public:
  CryptoAES() { }
  ~CryptoAES() {}
  int get_type() const {
    return CEPH_CRYPTO_AES;
  }
  int create(bufferptr& secret);
  int validate_secret(bufferptr& secret);
  void encrypt(const bufferptr& secret, const bufferlist& in,
	       bufferlist& out, std::string &error) const;
  void decrypt(const bufferptr& secret, const bufferlist& in, 
	      bufferlist& out, std::string &error) const;
};

#endif
