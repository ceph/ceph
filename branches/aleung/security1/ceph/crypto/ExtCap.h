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

#ifndef __EXTCAP_H
#define __EXTCAP_H

#include "include/buffer.h"
#include "mds/Capability.h"

using namespace std;

#include "crypto/CryptoLib.h"
using namespace CryptoLib;

class ExtCap {
  struct cap_data_t {
    int id;
    utime_t t_s;
    utime_t t_e;
    int mode;
    __uint8_t comp;
    
    uid_t uid;
    gid_t gid;
    inodeno_t ino; // inode number
    string user_rhash;
    string file_rhash;
  };

  cap_data_t data;
  FixedSigBuf allocSig;
  SigBuf signature;
  bool sigConverted;

public:
  ExtCap() {}
  ExtCap(utime_t s, utime_t e, int m, __uint8_t c, string user, string file)
  {
    data.id = 0;
    data.t_s = s;
    data.t_e = e;
    data.mode = m;
    data.comp = c;
    data.user_rhash = user;
    data.file_rhash = file;
    sigConverted = false;
  }
  // capability for single user/single file
  /**********
   * This function will create the time on the spot
   * @param m is the mode
   * @param is the user id
   * @param n is the file inode number
   **********/
  ExtCap(int m, uid_t u, inodeno_t n)
  {
    data.id = 0;
    data.t_s = g_clock.now();
    data.t_e = data.t_s;
    data.t_e += 3600;
    data.mode = m;
    data.uid = u;
    sigConverted = false;
  }

  ~ExtCap() { }
  
  int get_id() const { return data.id; }
  utime_t get_ts() const { return data.t_s; }
  utime_t get_te() const { return data.t_e; }
  int mode() const { return data.mode; }
  __int8_t comp() const { return data.comp; }
  string get_user_rhash() const { return data.user_rhash; }
  string get_file_rhash() const { return data.file_rhash; }

  void set_mode(int new_mode) { data.mode = new_mode; }

  SigBuf get_sig() {
    if (sigConverted)
      return signature;
    signature.Assign(allocSig.data(), allocSig.size());
    sigConverted = true;
    return signature;
  }

  void sign_extcap(esignPriv privKey) {
    byte capArray[sizeof(data)];
    memcpy(capArray, &data, sizeof(data));
    signature = esignSig(capArray, sizeof(data), privKey);
    allocSig.Assign(signature,signature.size());
  }

  bool verif_extcap (esignPub pubKey) {
    byte capArray[sizeof(data)];
    memcpy(capArray, &data, sizeof(data));
    signature.Assign(allocSig, allocSig.size());
    return esignVer(capArray, sizeof(data), signature, pubKey);
  }

  void _encode(bufferlist& bl) {
    bl.append((char*)&(data.id), sizeof(data.id));
    bl.append((char*)&(data.t_s), sizeof(data.t_s));
    bl.append((char*)&(data.t_e), sizeof(data.t_e));
    bl.append((char*)&(data.mode), sizeof(data.mode));
    bl.append((char*)&(data.comp), sizeof(data.comp));
    bl.append((char*)&allocSig, sizeof(allocSig));

    ::_encode(data.user_rhash, bl);
    ::_encode(data.file_rhash, bl);
    
  }
  void _decode(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(data.id), (char*)&(data.id));
    off += sizeof(data.id);
    bl.copy(off, sizeof(data.t_s), (char*)&(data.t_s));
    off += sizeof(data.t_s);
    bl.copy(off, sizeof(data.t_e), (char*)&(data.t_e));
    off += sizeof(data.t_e);
    bl.copy(off, sizeof(data.mode), (char*)&(data.mode));
    off += sizeof(data.mode);
    bl.copy(off, sizeof(data.comp), (char*)&(data.comp));
    off += sizeof(data.comp);
    bl.copy(off, sizeof(allocSig), (char*)&allocSig);
    off += sizeof(allocSig);

    ::_decode(data.user_rhash, bl, off);
    ::_decode(data.file_rhash, bl, off);
  }
};

#endif
