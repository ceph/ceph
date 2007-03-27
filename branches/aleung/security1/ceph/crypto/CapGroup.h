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
#ifndef __CAPGROUP_H
#define __CAPGROUP_H

#include<iostream>
using namespace std;

#include "crypto/MerkleTree.h"

class CapGroup {
 private:
  //gid_t group_id;
  hash_t root_hash;

  MerkleTree mtree;
  list<uid_t> users;

  MerkleTree file_tree;
  list<inodeno_t> inodes;

  byte signature[ESIGNSIGSIZE];

 public:
  friend class OSD;
  friend class Locker;
  CapGroup () { }
  //CapGroup (gid_t id) { group_id = id; }
  CapGroup (hash_t rhash, list<uid_t>& ulist) :
    root_hash(rhash), users(ulist) { }
  CapGroup (uid_t user) {
    users.push_back(user);
    mtree.add_user(user);
    root_hash = mtree.get_root_hash();
  }
  CapGroup (hash_t rhash, list<inodeno_t>& inodelist) :
    root_hash(rhash), inodes(inodelist) { }
  CapGroup (inodeno_t ino) {
    inodes.push_back(ino);
    file_tree.add_inode(ino);
    root_hash = file_tree.get_root_hash();
  }
  //CapGroup (gid_t id, list<uid_t>& ulist) : group_id(id), users(ulist) {
    // add users to MerkleTree
  //  mtree = MerkleTree(users);
  //  root_hash = mtree.get_root_hash();
  //}
  
  //gid_t get_gid() { return group_id; }
  //void set_gid(gid_t id) { group_id = id; }

  byte *get_sig() { return signature; }
  void set_sig(byte *sig) { memcpy(signature, sig, ESIGNSIGSIZE); }

  hash_t get_root_hash() { return root_hash; }
  void set_root_hash(hash_t nhash) { root_hash = nhash; }

  void sign_list(esignPriv privKey) {
    SigBuf sig;
    sig = esignSig((byte*)&root_hash, sizeof(root_hash), privKey);
    memcpy(signature, sig.data(), sig.size());
  }
  bool verify_list(esignPub pubKey) {
    SigBuf sig;
    sig.Assign(signature, sizeof(signature));
    return esignVer((byte*)&root_hash, sizeof(root_hash), sig, pubKey);
  }

  void add_user(uid_t user) {
    users.push_back(user);
    // re-compute root-hash
    mtree.add_user(user);
    root_hash = mtree.get_root_hash();
  }
  void remove_user(uid_t user) {
    users.remove(user);
    //FIXME need to re-compute hash
  }

  void add_inode(inodeno_t ino) {
    inodes.push_back(ino);
    // re-compute root-hash
    file_tree.add_inode(ino);
    root_hash = file_tree.get_root_hash();
  }
  void remove_user(inodeno_t ino) {
    inodes.remove(ino);
    //FIXME need to re-compute hash
  }

  int num_inodes() { return inodes.size(); }

  bool contains(uid_t user) {
    for (list<uid_t>::iterator ui = users.begin();
	 ui != users.end();
	 ui++) {
      if (*ui == user)
	return true;
    }
    return false;
  }

  bool contains_inode(inodeno_t ino) {
    for (list<inodeno_t>::iterator ii = inodes.begin();
	 ii != inodes.end();
	 ii++) {
      if (*ii == ino)
	return true;
    }
    return false;
  }

  void set_list(list<uid_t>& nlist) {
    users = nlist;
    mtree = MerkleTree(users);
  }
  list<uid_t>& get_list() { return users; }

  void set_inode_list(list<inodeno_t>& ilist) {
    inodes = ilist;
    file_tree = MerkleTree(inodes);
  }
  list<inodeno_t>& get_inode_list() { return inodes; }
};

#endif
