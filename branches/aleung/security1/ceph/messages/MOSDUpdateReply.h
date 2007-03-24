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

#ifndef __MOSDUPDATEREPLY_H
#define __MOSDUPDATEREPLY_H

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDUpdateReply : public Message {
private:
  hash_t user_hash;
  byte signature[ESIGNSIGSIZE];
  list<uid_t> updated_users;
  list<inodeno_t> updated_files;

public:
  MOSDUpdateReply () : Message(MSG_OSD_UPDATE_REPLY) { }
  MOSDUpdateReply(hash_t uhash) : Message(MSG_OSD_UPDATE_REPLY),
				  user_hash(uhash) { }
  MOSDUpdateReply(hash_t uhash, list<uid_t>& users) :
    Message(MSG_OSD_UPDATE_REPLY), user_hash(uhash), updated_users(users) { }
  MOSDUpdateReply(hash_t uhash, list<uid_t>& users, byte *sig) :
    Message(MSG_OSD_UPDATE_REPLY), user_hash(uhash), updated_users(users) {
    memcpy(signature, sig, ESIGNSIGSIZE);
  }
  MOSDUpdateReply(hash_t fhash, list<inodeno_t>& files, byte *sig) :
    Message(MSG_OSD_UPDATE_REPLY), user_hash(fhash), updated_files(files) {
    memcpy(signature, sig, ESIGNSIGSIZE);
  }

  hash_t get_user_hash() { return user_hash; }
  list<uid_t>& get_list() { return updated_users; }
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

    payload.append((char*)signature, sizeof(signature));
    payload.append((char*)&user_hash, sizeof(user_hash));
    _encode(updated_users, payload);
    _encode(updated_files, payload);
  }
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(signature), (char*)signature);
    off += sizeof(signature);
    payload.copy(off, sizeof(user_hash), (char*)&user_hash);
    off += sizeof(user_hash);
    _decode(updated_users, payload, off);
    _decode(updated_files, payload, off);
  }
  virtual char *get_type_name() { return "oop_update_reply"; }
  void print(ostream& out) {
    out << "osd_update_reply("
	<< user_hash
	<< ")";
  }
};

#endif
