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
#ifndef __MERKLETREE_H
#define __MERKLETREE_H

#include<iostream>
#include<list>
#include<set>
using namespace std;

#include "CryptoLib.h"
using namespace CryptoLib;

struct hash_t{
  byte val[SHA1DIGESTSIZE];
};

// comparison operators
inline bool operator==(const hash_t& a, const hash_t& b)
{
  return (memcmp((void*)&a, (void*)&b, sizeof(hash_t)) == 0);
}
inline bool operator>(const hash_t& a, const hash_t& b)
{
  return (memcmp((void*)&a, (void*)&b, sizeof(hash_t)) > 0);
}
inline bool operator<(const hash_t& a, const hash_t& b)
{
  return (memcmp((void*)&a, (void*)&b, sizeof(hash_t)) < 0);
}
// ostream
inline std::ostream& operator<<(std::ostream& out, const hash_t& c)
{
  byte hexArray[2*SHA1DIGESTSIZE];
  memset(hexArray, 0x00, sizeof(hexArray));
  toHex(c.val, hexArray, SHA1DIGESTSIZE, 2*SHA1DIGESTSIZE);
  out << string((const char*)hexArray, sizeof(hexArray));
  return out;
}

class MerkleTree {
 private:
  // the root hash of the tree
  hash_t root_hash;

 public:
  // default constructor
  MerkleTree () { memset(&root_hash, 0x00, sizeof(root_hash)); }

  // constructor from an initial list of users
  MerkleTree (list< uid_t >& input) {
    memset(&root_hash, 0x00, sizeof(root_hash));
    uid_t uidArray[input.size()];
    int counter = 0;
    
    // FIXME just do a linear hash first for root hash
    // copy list into buffer
    for (list<uid_t>::iterator li = input.begin();
	 li != input.end();
	 li++) {
      uidArray[counter] = *li;
      counter++;
    }
    // zero the array
    sha1((byte*)uidArray, (byte*)&root_hash, sizeof(uidArray));
  }
  
  // constructor from an initial list of files
  MerkleTree (list< inodeno_t >& input) {
    memset(&root_hash, 0x00, sizeof(root_hash));
    inodeno_t inoArray[input.size()];
    int counter = 0;
    
    // FIXME just do a linear hash first for root hash
    // copy list into buffer
    for (list<inodeno_t>::iterator li = input.begin();
	 li != input.end();
	 li++) {
      inoArray[counter] = *li;
      counter++;
    }
    // zero the array
    sha1((byte*)inoArray, (byte*)&root_hash, sizeof(inoArray));
  }

  // constructor from an initial set of users
  MerkleTree (set< uid_t >& input) {
    memset(&root_hash, 0x00, sizeof(root_hash));
    uid_t uidArray[input.size()];
    int counter = 0;
    
    // FIXME just do a linear hash first for root hash
    // copy list into buffer
    for (set<uid_t>::iterator si = input.begin();
	 si != input.end();
	 si++) {
      uidArray[counter] = *si;
      counter++;
    }
    // zero the array
    sha1((byte*)uidArray, (byte*)&root_hash, sizeof(uidArray));
  }

  void add_user(uid_t user) {
    // hash the user
    hash_t user_hash;
    sha1((byte*)&user, (byte*)&user_hash, sizeof(user));
    // join the user and root_hash
    hash_t conjunction[2];
    conjunction[0] = root_hash;
    conjunction[1] = user_hash;
    // hash em both
    sha1((byte*)&conjunction, (byte*)&root_hash, sizeof(conjunction));
  }

  void add_inode(inodeno_t ino) {
    // hash the user
    hash_t ino_hash;
    sha1((byte*)&ino, (byte*)&ino_hash, sizeof(ino));
    // join the user and root_hash
    hash_t conjunction[2];
    conjunction[0] = root_hash;
    conjunction[1] = ino_hash;
    // hash em both
    sha1((byte*)&conjunction, (byte*)&root_hash, sizeof(conjunction));
  }
  
  hash_t& get_root_hash() { return root_hash; }
};

// ostream
inline std::ostream& operator<<(std::ostream& out, MerkleTree& c)
{
  return out << c.get_root_hash();
}

#endif
