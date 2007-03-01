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

#include "include/types.h"
#include "include/buffer.h"
#include "mds/Capability.h"

using namespace std;

#include "crypto/CryptoLib.h"
using namespace CryptoLib;

#include "common/Clock.h"

class ExtCap {
private:
  struct cap_data_t {
    int id; // capability id
    utime_t t_s; // creation time
    utime_t t_e; // expiration time
    int mode; // I/O mode
    __uint8_t comp; // specify users/pubkey (for delegation)
    
    uid_t uid; // user id
    gid_t gid; // group id
    inodeno_t ino; // inode number
  };
  
  cap_data_t data;
  byte sigArray[ESIGNSIGSIZE];
  //SigBuf signature;

public:
  friend class Client;
  friend class OSD;
  // default constructor, should really not be used
  ExtCap() {}

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
    data.ino = n;
  }

  // capability for single user, many named files

  // capability for single user, too many files

  // capability for many named users, single file

  // capability for many named user, many named files

  // capability for many named users, too many files

  // capability for too many users, single file

  // capability for too many user, many named files

  // capability for too many user, too many files

  ~ExtCap() { }
  
  int get_id() const { return data.id; }
  utime_t get_ts() const { return data.t_s; }
  utime_t get_te() const { return data.t_e; }
  uid_t get_uid() const { return data.uid; }
  gid_t get_gid() const { return data.gid; }
  inodeno_t get_ino() const { return data.ino; }
  int mode() const { return data.mode; }
  __int8_t comp() const { return data.comp; }

  // in case the mode needs to be changed
  // FYI, you should resign the cap after this
  void set_mode(int new_mode) { data.mode = new_mode; }

  const cap_data_t* get_data() const {
    return (&data);
  }
  
  int get_data_size() const {
    return sizeof(data);
  }

  void sign_extcap(esignPriv privKey) {
    byte capArray[sizeof(data)];
    memcpy(capArray, &data, sizeof(data));
    SigBuf signature;
    signature = esignSig(capArray, sizeof(data), privKey);
    // store the signature into permanent buffer
    memcpy(sigArray, signature.data(), signature.size());
    
    //byte hexArray[sizeof(capArray)];
    //memset(hexArray, 0x00, sizeof(hexArray));
    //toHex(capArray, hexArray, sizeof(capArray), sizeof(capArray));
    //cout << "Signed content capArray hex: " << endl << string((const char*)hexArray,sizeof(hexArray)) << endl;

    //byte hexTest[sizeof(sigArray)];
    //memset(hexTest, 0x00, sizeof(sigArray));
    //toHex(sigArray, hexTest, sizeof(sigArray), sizeof(sigArray));
    //cout << "COPIED DATA BUFFER HEX: " << endl << string((const char*)hexTest,sizeof(hexTest)) << endl;
  }

  bool verif_extcap (esignPub pubKey) {
    byte capArray[sizeof(data)];
    memcpy(capArray, &data, sizeof(data));

    //byte hexArray[sizeof(sigArray)];
    //memset(hexArray, 0x00, sizeof(hexArray));
    //toHex(sigArray, hexArray, sizeof(sigArray), sizeof(sigArray));
    //cout << "Verified signature hex: " << endl << string((const char*)hexArray,sizeof(hexArray)) << endl;
    SigBuf signature;
    signature.Assign(sigArray, sizeof(sigArray));
    
    return esignVer(capArray, sizeof(data), signature, pubKey);
  }

  void _encode(bufferlist& bl) {
    bl.append((char*)&(data), sizeof(data));
    bl.append((char*)sigArray, sizeof(sigArray));
    
  }
  void _decode(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(data), (char*)&(data));
    off += sizeof(data);
    bl.copy(off, sizeof(sigArray), (char*)sigArray);
    off += sizeof(sigArray);
  }
};

#endif
