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

#ifndef __MONMAP_H
#define __MONMAP_H

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "msg/Message.h"
#include "include/types.h"
#include "crypto/CryptoLib.h"
using namespace CryptoLib;

class MonMap {
 public:
  epoch_t   epoch;       // what epoch of the osd cluster descriptor is this
  int       num_mon;
  vector<entity_inst_t> mon_inst;
  int       last_mon;    // last mon i talked to
  //string pub_str_key;
  char pub_str_key[ESIGNKEYSIZE];
  esignPub pub_key;
  bool keyConvert;

  MonMap(int s=0) : epoch(0), num_mon(s), mon_inst(s), last_mon(-1) {}
  
  //void generate_key_pair(string& private_key) {
  // private_key is assumed to already be allocated to right size
  void generate_key_pair(char *private_key) {
    //esignPriv tempKey = esignPrivKey("crypto/esig1536.dat");
    esignPriv tempKey = esignPrivKey("crypto/esig1023.dat");
    //private_key = privToString(tempKey);
    memcpy(private_key, privToString(tempKey).c_str(), ESIGNPRIVSIZE);
    pub_key = esignPubKey(tempKey);
    //pub_str_key = pubToString(pub_key);
    memcpy(pub_str_key, pubToString(pub_key).c_str(), sizeof(pub_str_key));
    
    // now throw away the private key
    keyConvert = false;
  }


  void add_mon(entity_inst_t inst) {
    mon_inst.push_back(inst);
    num_mon++;
  }

  // pick a mon.  
  // choice should be stable, unless we explicitly ask for a new one.
  int pick_mon(bool newmon=false) { 
    if (newmon || (last_mon < 0)) {
      last_mon = 0;  //last_mon = rand() % num_mon;
    }
    return last_mon;    
  }

  const entity_inst_t &get_inst(int m) {
    assert(m < num_mon);
    return mon_inst[m];
  }

  // key mutator
  void set_str_key(char *key) {
    memcpy(pub_str_key, key, sizeof(pub_str_key));
  }
  //void set_str_key(string key) {
  //pub_str_key = key;
  //}
  
  // key access
  const char* get_str_key() {
    return pub_str_key;
  }
  //const string get_str_key() {
  //return pub_str_key;
  //}
  const esignPub& get_key() {
    if (!keyConvert) {
      pub_key = _fromStr_esignPubKey(string(pub_str_key, sizeof(pub_str_key)));
      keyConvert = true;
    }
    return pub_key;
  }

  void prepare_mon_key() {
    pub_key = _fromStr_esignPubKey(string(pub_str_key, sizeof(pub_str_key)));
    keyConvert = true;
  }

  void encode(bufferlist& blist) {
    blist.append((char*)&epoch, sizeof(epoch));
    blist.append((char*)&num_mon, sizeof(num_mon));
    
    _encode(mon_inst, blist);
    //_encode(pub_str_key, blist);
    blist.append(pub_str_key, sizeof(pub_str_key));
  }
  
  void decode(bufferlist& blist) {
    int off = 0;
    blist.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    blist.copy(off, sizeof(num_mon), (char*)&num_mon);
    off += sizeof(num_mon);

    _decode(mon_inst, blist, off);
    //_decode(pub_str_key, blist, off);
    blist.copy(off, sizeof(pub_str_key), pub_str_key);
    off += sizeof(pub_str_key);
  }

  int write(char *fn) {
    // encode
    bufferlist bl;
    encode(bl);

    // write
    int fd = ::open(fn, O_RDWR|O_CREAT);
    if (fd < 0) return fd;
    ::fchmod(fd, 0644);
    ::write(fd, (void*)bl.c_str(), bl.length());
    ::close(fd);
    return 0;
  }

  int read(char *fn) {
    // read
    bufferlist bl;
    int fd = ::open(fn, O_RDONLY);
    if (fd < 0) return fd;
    struct stat st;
    ::fstat(fd, &st);
    bufferptr bp(st.st_size);
    bl.append(bp);
    ::read(fd, (void*)bl.c_str(), bl.length());
    ::close(fd);
  
    // decode
    decode(bl);
    return 0;
  }

};

#endif
