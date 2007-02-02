// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/**********
 * This class is a ticket which serves to
 * authorize the user's public key.
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
  FixedSigBuf allocSig;
  SigBuf signature;
  bool keyConverted;
  bool sigConverted;

public:
  friend class Monitor;
  friend class MDS;

 public:
  // constructor when a fixed buffer is passed
  Ticket(uid_t u, gid_t g, string user, epoch_t s,
	 epoch_t e, string initVec, string key,
	 FixedSigBuf sig) : uid(u), gid(g), username(user),
			    t_s(s), t_e(e), iv(initVec),
			    pubKey(key), allocSig(sig),
			    keyConverted(false) {}
  // constructor when a non-fixed buffer is passed
  Ticket(uid_t u, gid_t g, string user, epoch_t s,
	 epoch_t e, string initVec, string key,
	 SigBuf sig) : uid(u), gid(g), username(user),
			    t_s(s), t_e(e), iv(initVec),
			    pubKey(key), keyConverted(false) {
    allocSig.Assign(sig, sig.size());
  }

  epoch_t get_ts() const { return t_s; }
  epoch_t get_te() const { return t_e; }
  
  uid_t get_uid() const { return uid; }
  uid_t get_gid() const { return gid; }

  const string& get_iv() { return iv; }

  const string& get_str_key() { return pubKey; }
  esignPub get_key() {
    if (keyConverted)
      return realKey;
    realKey = _fromStr_esignPubKey(pubKey);
    keyConverted = true;
    return realKey;
  }

  SigBuf get_sig() {
    if (sigConverted)
      return signature;
    signature.Assign(allocSig.data(), allocSig.size());
    sigConverted = true;
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
    blist.copy(off, sizeof(allocSig), (char*)&allocSig);
    off += sizeof(allocSig);
    
    _decode(iv, blist, off);
    _decode(username, blist, off);
    _decode(pubKey, blist, off);
  }
  void encode(bufferlist& blist) {
    blist.append((char*)&uid, sizeof(uid));
    blist.append((char*)&gid, sizeof(gid));
    blist.append((char*)&t_s, sizeof(t_s));
    blist.append((char*)&t_e, sizeof(t_e));
    blist.append((char*)&allocSig, sizeof(allocSig));

    _encode(iv, blist);
    _encode(username, blist);
    _encode(pubKey, blist);
  }
  //bl.append((char*)&uid,sizeof(uid));
  //bl.copy(off,sizeof(uid),(char*)&uid);
  //off += sizeof(uid);
};

#endif
