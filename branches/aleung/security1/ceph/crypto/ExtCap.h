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
  SigBuf signature;

public:
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

  /*
  SigBuf get_sig() {
    if (sigConverted)
      return signature;
    signature.Assign(allocSig.data(), allocSig.size());
    sigConverted = true;
    return signature;
  }

  FixedSigBuf get_fixed_sig() {
    return allocSig;
  }

  FixedSigBuf *get_fixed_sig_ptr( ){
    return &allocSig;
  }
  */

  const cap_data_t* get_data() const {
    return (&data);
  }
  
  int get_data_size() const {
    return sizeof(data);
  }

  void sign_extcap(esignPriv privKey) {
    byte capArray[sizeof(data)];
    memcpy(capArray, &data, sizeof(data));
    signature = esignSig(capArray, sizeof(data), privKey);
    // store the signature into permanent buffer
    memcpy(sigArray, signature.data(), signature.size());
    
    //byte hexArray[sizeof(capArray)];
    //memset(hexArray, 0x00, sizeof(hexArray));
    //toHex(capArray, hexArray, sizeof(capArray), sizeof(capArray));
    //cout << "Signed content capArray hex: " << endl << string((const char*)hexArray,sizeof(hexArray)) << endl;

    //cout << "SIGNATURE SIZE: " << signature.size() << endl;
    //allocSig.Assign(signature,signature.size());
    
    //byte hexTest[sizeof(sigArray)];
    //memset(hexTest, 0x00, sizeof(sigArray));
    //toHex(sigArray, hexTest, sizeof(sigArray), sizeof(sigArray));
    //cout << "COPIED DATA BUFFER HEX: " << endl << string((const char*)hexTest,sizeof(hexTest)) << endl;
  }

  bool verif_extcap (esignPub pubKey) {
    byte capArray[sizeof(data)];
    memcpy(capArray, &data, sizeof(data));

    //byte hexArray[sizeof(capArray)];
    //memset(hexArray, 0x00, sizeof(hexArray));
    //toHex(capArray, hexArray, sizeof(capArray), sizeof(capArray));
    //cout << "Verified content capArray hex: " << endl << string((const char*)hexArray,sizeof(hexArray)) << endl;

    signature.Assign(sigArray, sizeof(sigArray));
    
    return esignVer(capArray, sizeof(data), signature, pubKey);
  }

  void _encode(bufferlist& bl) {
    /*
    bl.append((char*)&(data.id), sizeof(data.id));
    bl.append((char*)&(data.t_s), sizeof(data.t_s));
    bl.append((char*)&(data.t_e), sizeof(data.t_e));
    bl.append((char*)&(data.mode), sizeof(data.mode));
    bl.append((char*)&(data.comp), sizeof(data.comp));
    bl.append((char*)&(data.uid), sizeof(data.uid));
    bl.append((char*)&(data.gid), sizeof(data.gid));
    bl.append((char*)&(data.ino), sizeof(data.ino));
    */
    bl.append((char*)&(data), sizeof(data));
    //bl.append((char*)((void*)allocSig), sizeof(allocSig));
    bl.append((char*)sigArray, sizeof(sigArray));

    //::_encode(user_rhash, bl);
    //::_encode(file_rhash, bl);
    
  }
  void _decode(bufferlist& bl, int& off) {
    /*
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
    bl.copy(off, sizeof(data.uid), (char*)&(data.uid));
    off += sizeof(data.uid);
    bl.copy(off, sizeof(data.gid), (char*)&(data.gid));
    off += sizeof(data.gid);
    bl.copy(off, sizeof(data.ino ), (char*)&(data.ino ));
    off += sizeof(data.ino);
    */
    bl.copy(off, sizeof(data), (char*)&(data));
    off += sizeof(data);
    //bl.copy(off, sizeof(allocSig), (char*)((void*)allocSig));
    //off += sizeof(allocSig);
    bl.copy(off, sizeof(sigArray), (char*)sigArray);
    off += sizeof(sigArray);

    //::_decode(user_rhash, bl, off);
    //::_decode(file_rhash, bl, off);
  }
};

#endif
