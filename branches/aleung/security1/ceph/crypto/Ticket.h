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
  struct ident_t {
    uid_t uid;
    gid_t gid;
    utime_t t_s;
    utime_t t_e;
    // shared key IV
    byte iv[RJBLOCKSIZE];
    char pubKey[ESIGNKEYSIZE];
    //string iv;
    //string username;
    //string pubKey;
  };
  ident_t identity;
  byte sigArray[ESIGNSIGSIZE];
  esignPub realPubKey;
  //FixedSigBuf allocSig;
  //SigBuf signature;
  bool keyConverted;
  //bool sigConverted;

public:
  friend class Monitor;
  friend class MDS;

 public:
  Ticket () : keyConverted(false) { }
  /*
  Ticket(uid_t u, gid_t g, utime_t s, utime_t e,
	 string initVec, string user, string key) {
    identity.uid = u;
    identity.gid = g;
    identity.t_s = s;
    identity.t_e = e;
    identity.iv = initVec;
    identity.username = user,
    identity.pubKey = key;
    keyConverted = false;
    sigConverted = false;
  }
  */
  // take the key as a string and convert it automatically
  // assume the IV is the correct size
  Ticket(uid_t u, gid_t g, byte *initVec, esignPub key) {
    identity.uid = u;
    identity.gid = g;
    identity.t_s = g_clock.now();
    identity.t_e = identity.t_s;
    identity.t_e += 3600;
    //identity.t_s = s;
    //identity.t_e = e;
    memcpy(identity.iv, initVec, sizeof(identity.iv));
    memcpy(identity.pubKey, pubToString(key).c_str(), sizeof(identity.pubKey));
  }

  ~Ticket() { }

  utime_t get_ts() const { return identity.t_s; }
  utime_t get_te() const { return identity.t_e; }
  
  uid_t get_uid() const { return identity.uid; }
  uid_t get_gid() const { return identity.gid; }

  //const string& get_iv() { return identity.iv; }
  void get_iv(byte* arg) {
    memcpy(arg, identity.iv, sizeof(identity.iv));
  }

  esignPub get_key() {
    if (keyConverted)
      return realPubKey;
    realPubKey = _fromStr_esignPubKey(string(identity.pubKey,
					      sizeof(identity.pubKey)));
    keyConverted = true;
    return realPubKey;
  }
  /*
  SigBuf get_sig() {
    if (sigConverted)
      return signature;
    signature.Assign(allocSig.data(), allocSig.size());
    sigConverted = true;
    return signature;
  }
  */
  void sign_ticket(esignPriv privKey) {
    byte ticketArray[sizeof(identity)];
    memcpy(ticketArray, &identity, sizeof(identity));
    SigBuf signature;
    signature = esignSig(ticketArray, sizeof(identity), privKey);
    //allocSig.Assign(signature,signature.size());
    assert(signature.size() == ESIGNSIGSIZE);
    memcpy(sigArray, signature.data(), signature.size());
  }

  bool verif_ticket (esignPub pubKey) {
    byte ticketArray[sizeof(identity)];
    memcpy(ticketArray, &identity, sizeof(identity));
    SigBuf signature;
    signature.Assign(sigArray, sizeof(sigArray));
    return esignVer(ticketArray, sizeof(identity), signature, pubKey);
  }
  

  void decode(bufferlist& blist, int& off) {
    /*
    blist.copy(off, sizeof(identity.uid), (char*)&(identity.uid));
    off += sizeof(identity.uid);
    blist.copy(off, sizeof(identity.gid), (char*)&(identity.gid));
    off += sizeof(identity.gid);
    blist.copy(off, sizeof(identity.t_s), (char*)&(identity.t_s));
    off += sizeof(identity.t_s);
    blist.copy(off, sizeof(identity.t_e), (char*)&(identity.t_e));
    off += sizeof(identity.t_e);
    */
    blist.copy(off, sizeof(identity), (char*)&identity);
    off += sizeof(identity);
    blist.copy(off, sizeof(sigArray), (char*)sigArray);
    off += sizeof(sigArray);
    //blist.copy(off, sizeof(allocSig), (char*)&allocSig);
    //off += sizeof(allocSig);
    
    //_decode(identity.iv, blist, off);
    //_decode(identity.username, blist, off);
    //_decode(identity.pubKey, blist, off);

  }
  void encode(bufferlist& blist) {
    /*
    blist.append((char*)&(identity.uid), sizeof(identity.uid));
    blist.append((char*)&(identity.gid), sizeof(identity.gid));
    blist.append((char*)&(identity.t_s), sizeof(identity.t_s));
    blist.append((char*)&(identity.t_e), sizeof(identity.t_e));
    blist.append((char*)&allocSig, sizeof(allocSig));
    */
    blist.append((char*)&identity, sizeof(identity));
    blist.append((char*)sigArray, sizeof(sigArray));

    //_encode(identity.iv, blist);
    //_encode(identity.username, blist);
    //_encode(identity.pubKey, blist);
  }
};

#endif
