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
#include "common/Clock.h"

using namespace std;

#include "crypto/CryptoLib.h"
using namespace CryptoLib;
#include "crypto/MerkleTree.h"
#include "crypto/CapGroup.h"

#define NO_GROUP 0
#define UNIX_GROUP 1

struct cap_id_t {
  int cid;
  int mds_id;
};
// comparison operators
inline bool operator>(const cap_id_t& a, const cap_id_t& b)
{
  if (a.mds_id > b.mds_id)
    return true;
  else if (a.mds_id == b.mds_id ) {
    if (a.cid > b.cid)
      return true;
    else
      return false;
  }
  else
    return false;
}
inline bool operator<(const cap_id_t& a, const cap_id_t& b)
{
  if (a.mds_id < b.mds_id)
    return true;
  else if (a.mds_id == b.mds_id ) {
    if (a.cid < b.cid)
      return true;
    else
      return false;
  }
  else
    return false;
}

// ostream
inline std::ostream& operator<<(std::ostream& out, const cap_id_t& c)
{
  out << c.mds_id << ".";
  out.setf(std::ios::right);
  out << c.cid;
  out.unsetf(std::ios::right);
  return out;
}

class ExtCap {
private:
  struct cap_data_t {
    cap_id_t id; // capability id
    utime_t t_s; // creation time
    utime_t t_e; // expiration time
    int mode; // I/O mode
    __uint8_t type; // specify mds policy

    // single user ident
    uid_t uid; // user id
    //unix group idents
    gid_t gid; // group id
    bool world;

    // hash based users and files
    hash_t user_group;
    hash_t file_group;

    inodeno_t ino; // inode number
  };
  
  cap_data_t data;
  byte sigArray[ESIGNSIGSIZE];

public:
  friend class Client;
  friend class OSD;
  friend class CapCache;
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
    data.id.cid = 0;
    data.id.mds_id = 0;
    data.t_s = g_clock.now();
    data.t_e = data.t_s;
    data.t_e += 3600;
    data.mode = m;
    data.uid = u;
    data.gid = 0;
    data.ino = n;
  }

  // capability for single user, many named files

  // capability for single user, too many files

  // capability for many named users, single file

  // capability for many named user, many named files

  // capability for many named users, too many files

  // capability for too many users, single file
  // --> unix grouping, issue cap to must general mode
  ExtCap(int m, uid_t u, gid_t g, inodeno_t n)
  {
    data.id.cid = 0;
    data.id.mds_id = 0;
    data.t_s = g_clock.now();
    data.t_e = data.t_s;
    data.t_e += 3600;
    data.mode = m;
    data.uid = u;
    data.gid = g;
    data.ino = n;
  }

  ExtCap(int m, uid_t u, CapGroup& cg, inodeno_t n)
  {
    data.id.cid = 0;
    data.id.mds_id = 0;
    data.t_s = g_clock.now();
    data.t_e = data.t_s;
    data.t_e += 3600;
    data.mode = m;
    data.uid = u;
    data.gid = cg.get_gid();
    data.user_group = cg.get_root_hash();
    data.ino = n;
  }

  // capability for too many user, many named files

  // capability for too many user, too many files

  ~ExtCap() { }
  
  cap_id_t get_id() const { return data.id; }
  utime_t get_ts() const { return data.t_s; }
  utime_t get_te() const { return data.t_e; }
  uid_t get_uid() const { return data.uid; }
  gid_t get_gid() const { return data.gid; }
  inodeno_t get_ino() const { return data.ino; }
  int mode() const { return data.mode; }
  __int8_t get_type() const { return data.type; }

  // in case the mode needs to be changed
  // FYI, you should resign the cap after this
  void set_mode(int new_mode) { data.mode = new_mode; }
  void set_id(int new_id, int new_mds_id) {
    data.id.cid = new_id;
    data.id.mds_id = new_mds_id;
  }
  void set_type(__int8_t new_type) { data.type = new_type;}

  void set_user_hash(hash_t nhash) { data.user_group = nhash; }
  void set_file_hash(hash_t nhash) { data.file_group = nhash; }

  hash_t get_user_hash() { return data.user_group; }
  hash_t get_file_hash() { return data.file_group; }

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
