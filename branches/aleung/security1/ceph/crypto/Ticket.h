// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/**********
 * This class is a ticket which serves to
 * authorize the client's public key.
 * It also provides the seed key for future
 * symmetric key establishment.
 **********/

#ifndef __TICKET_H
#define __TICKET_H

#include"common/Clock.h"

using namespace std;

#include"crypto/CryptoLib.h"
using namespace CryptoLib;

class Ticket {
 private:
  // identification
  uid_t uid;
  gid_t gid;
  epoch_t t_s;
  epoch_t t_e;
  // shared key IV
  // needs to be converted back to a byte arry to be used
  string iv;
  string username;
  string pubKey;
  esignPub realKey;
  SigBuf signature;
  bool keyConverted;

public:
  friend class Monitor;
  friend class MDS;

 public:
  Ticket(uid_t u, gid_t g, string user, epoch_t s, epoch_t e,
	 string initVec, string key) : uid(u), gid(g), username(user),
				       t_s(s), t_e(e), iv(initVec),
				       pubKey(key), keyConverted(false) {}
  epoch_t get_ts() const { return t_s; }
  epoch_t get_te() const { return t_e; }
  
  uid_t get_uid() const { return uid; }
  uid_t get_gid() const { return gid; }

  string get_iv() { return iv; }

  string get_str_key() { return pubKey; }
  esignPub get_key() {
    if (keyConverted)
      return realKey;
    realKey = _fromStr_esignPubKey(pubKey);
    keyConverted = true;
    return realKey;
  }

  SigBuf get_sig() {
    return signature;
  }

  void decode(bufferlist& blist) {
    int off = 0;
    blist.copy(off, sizeof(uid), (char*)&uid);
    off += sizeof(uid);
    blist.copy(off, sizeof(gid), (char*)&gid);
    off += sizeof(gid);
    blist.copy(off, sizeof(t_s), (char*)&t_s);
    off += sizeof(t_s);
    blist.copy(off, sizeof(t_e), (char*)&t_e);
    off += sizeof(t_e);
    
    _decode(iv, blist, off);
    _decode(username, blist, off);
    _decode(pubKey, blist, off);
    //-decode(signature, blist, off);
    // waiting on conversion
  }
  void encode(bufferlist& blist) {
    blist.append((char*)&uid, sizeof(uid));
    blist.append((char*)&gid, sizeof(gid));
    blist.append((char*)&t_s, sizeof(t_s));
    blist.append((char*)&t_e, sizeof(t_e));

    _encode(iv, blist);
    _encode(username, blist);
    _encode(pubKey, blist);
    //_encode(signature, blist);
  }
  //bl.append((char*)&uid,sizeof(uid));
  //bl.copy(off,sizeof(uid),(char*)&uid);
  //off += sizeof(uid);
}

#endif
