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


#ifndef __MCLIENTAUTHUSER_H
#define __MCLIENTAUTHUSER_H

#include "msg/Message.h"

class MClientAuthUser : public Message {
  string username;
  uid_t uid;
  gid_t gid;
  string pubKey;
  //char userPublicKey[ESIGNKEYSIZE];
  
 public:
  MClientAuthUser() : Message(MSG_CLIENT_AUTH_USER) {}
  MClientAuthUser(string un, uid_t u, gid_t g, string k) : 
    Message(MSG_CLIENT_AUTH_USER), username(un), uid(u), gid(g), pubKey(k) { }
  //MClientAuthUser(string un, uid_t u, gid_t g, byte* k) : 
  //  Message(MSG_CLIENT_AUTH_USER), username(un), uid(u), gid(g) {
  //  memset(userPublicKey, k, ESIGNKEYSIZE);
  //}

  char *get_type_name() { return "client_auth_user"; }
  const string& get_str_key() { return pubKey; }
  esignPub get_key() { return _fromStr_esignPubKey(pubKey); }
  string get_username() { return username; }
  uid_t get_uid() { return uid; }
  gid_t get_gid() { return gid; }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(uid), (char*)&uid);
    off += sizeof(uid);
    payload.copy(off, sizeof(gid), (char*)&gid);
    off += sizeof(gid);
    _decode(username, payload, off);
    _decode(pubKey, payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&uid, sizeof(uid));
    payload.append((char*)&gid, sizeof(gid));
    _encode(username, payload);
    _encode(pubKey, payload);
  }

};

#endif
