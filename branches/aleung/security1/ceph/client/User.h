// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/**********
 * This class constructs a user instance.
 * Clients will act a proxies for user behavior.
 * We should assume the client has authenticated alreadt
 * perhaps to the kernel.
 **********/
#ifndef __USER_H
#define __USER_H

#include<iostream>
using namespace std;

#include"crypto/CryptoLib.h"
using namespace CryptoLib;

class User {
  // identification
  uid_t uid;
  gid_t gid;
  char *username;
  esignPub myPubKey;
  // a kerberos like certification ticket
  Ticket *ticket;

 public:
  // the pub/prv key pair must exist before hand. The user
  // presumably logs in with a passwd which relates
  // to the key gen
  User(uid_t u, gid g, char *uname, esignPub key) :
    uid(u),
    gid(g),
    username(uname),
    myPubKey(key)
    { }
    
}

#endif
