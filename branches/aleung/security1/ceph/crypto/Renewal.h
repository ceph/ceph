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
#ifndef __RENEWAL_H
#define __RENEWAL_H

#include "common/Clock.h"
#include "crypto/CryptoLib.h"
using namespace CryptoLib;
using namespace std;

class Renewal {
  set<cap_id_t> renewed_caps;
  utime_t t_s;
  utime_t t_e;
  byte signature[ESIGNSIGSIZE];

public:
  Renewal() {}
  Renewal(set<cap_id_t>& capset, utime_t s, utime_t e) :
    renewed_caps(capset), t_s(s), t_e(e) { }
  Renewal(set<cap_id_t>& capset) : renewed_caps(capset) {
    t_s = g_clock.now();
    t_e = t_s;
    t_e += 3600;
  }
  bool contains(cap_id_t cid) { return renewed_caps.count(cid); }
  utime_t get_ts() { return t_s; }
  utime_t get_te() { return t_e; }
  set<cap_id_t>& get_renewed_caps() { return renewed_caps; }
  int num_renewed_caps() { return renewed_caps.size(); }
  void add_cap(cap_id_t cid) { renewed_caps.insert(cid); }
  void add_set(set<cap_id_t>& newcaps) {
    for (set<cap_id_t>::iterator si = renewed_caps.begin();
	 si != renewed_caps.end();
	 si++) {
      renewed_caps.insert(*si);;
    }
  }

  void sign_renewal(esignPriv privKey) {
    cap_id_t capids[renewed_caps.size()];
    int count = 0;
    for (set<cap_id_t>::iterator si = renewed_caps.begin();
	 si != renewed_caps.end();
	 si++) {
      capids[count] = *si;
      count++;
    }
    SigBuf sig;
    sig = esignSig((byte*)capids, sizeof(capids), privKey);
    memcpy(signature, sig.data(), sig.size());
  }
  bool verif_renewal(esignPub pubKey) {
    cap_id_t capids[renewed_caps.size()];
    int count = 0;
    for (set<cap_id_t>::iterator si = renewed_caps.begin();
	 si != renewed_caps.end();
	 si++) {
      capids[count] = *si;
      count++;
    }
    SigBuf sig;
    sig.Assign(signature, sizeof(signature));
    return esignVer((byte*)capids, sizeof(capids), sig, pubKey);
  }

  void _encode(bufferlist& bl) {
    ::_encode(renewed_caps, bl);
    bl.append((char*)&t_s, sizeof(t_s));
    bl.append((char*)&t_e, sizeof(t_e));
    bl.append((char*)signature, sizeof(signature));
  }
  void _decode(bufferlist& bl, int& off) {
    ::_decode(renewed_caps, bl, off);
    bl.copy(off, sizeof(t_s), (char*)&t_s);
    off += sizeof(t_s);
    bl.copy(off, sizeof(t_e), (char*)&t_e);
    off += sizeof(t_e);
  }
  
};

#endif
