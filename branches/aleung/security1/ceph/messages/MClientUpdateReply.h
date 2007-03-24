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

#ifndef __MCLIENTUPDATEREPLY_H
#define __MCLIENTUPDATEREPLY_H

#include "msg/Message.h"
#include "osd/osd_types.h"
//#include "crypto/CryptoLib.h"
//using namespace CryptoLib;
//#include "crypto/MerkleTree.h"

class MClientUpdateReply : public Message {
 private:
  hash_t user_hash;
  byte signature[ESIGNSIGSIZE];
  list<uid_t> updated_users;
  list<inodeno_t> updated_files;

 public:
  MClientUpdateReply() : Message(MSG_CLIENT_UPDATE_REPLY) { }
  MClientUpdateReply(hash_t uhash, list<uid_t>& ulist, byte *sig) :
    Message(MSG_CLIENT_UPDATE_REPLY),
    user_hash(uhash), updated_users(ulist) {
    memcpy(signature, sig, ESIGNSIGSIZE);
  }
  MClientUpdateReply(hash_t uhash, list<inodeno_t>& flist) :
    Message(MSG_CLIENT_UPDATE_REPLY),
    user_hash(uhash), updated_files(flist) { }

  hash_t get_user_hash() { return user_hash; }
  list<uid_t>& get_user_list() { return updated_users; }
  list<inodeno_t>& get_file_list() { return updated_files; }

  void set_sig(byte *sig) { memcpy(signature, sig, ESIGNSIGSIZE); }
  byte *get_sig() { return signature; }

  void sign_list(esignPriv privKey) {
    SigBuf sig;
    sig = esignSig((byte*)&user_hash, sizeof(user_hash), privKey);
    memcpy(signature, sig.data(), sig.size());
  }

  bool verify_list(esignPub pubKey) {
    SigBuf sig;
    sig.Assign(signature, sizeof(signature));
    return esignVer((byte*)&user_hash, sizeof(user_hash), sig, pubKey);
  }

  virtual void encode_payload() {
    payload.append((char*)&user_hash, sizeof(user_hash));
    payload.append((char*)signature, sizeof(signature));

    _encode(updated_users, payload);
    _encode(updated_files, payload);
  }
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(user_hash), (char*)&user_hash);
    off += sizeof(user_hash);
    payload.copy(off, sizeof(signature), (char*)signature);
    off += sizeof(signature);

    _decode(updated_users, payload, off);
    _decode(updated_files, payload, off);
  }
  virtual char *get_type_name() { return "client_update_reply"; }
  void print(ostream& out) {
    out << "client_update_reply(" << user_hash
	<< ")";
  }
};

#endif
