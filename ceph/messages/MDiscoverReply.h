#ifndef __MDISCOVERREPLY_H
#define __MDISCOVERREPLY_H

#include "include/Message.h"
#include "mds/CDir.h"
#include "mds/CInode.h"
#include "include/filepath.h"

#include <vector>
#include <string>
using namespace std;


class MDiscoverReply : public Message {
  inodeno_t    base_ino;
  bool         no_base_dir;
  bool         no_base_dentry;
  
  // ... + dir + dentry + inode
  // inode [ + ... ], base_ino = 0 : discover base_ino=0, start w/ root ino
  // dentry + inode [ + ... ]      : discover want_base_dir=false
  // (dir + dentry + inode) +      : discover want_base_dir=true
  vector<CDirDiscover>   dirs;      // not inode-aligned if no_base_dir = true.
  filepath               path;      // not inode-aligned in no_base_dentry = true
  vector<CInodeDiscover> inodes;

 public:
  // accessors
  inodeno_t get_base_ino() { return base_ino; }
  int       get_num_inodes() { return inodes.size(); }

  bool      has_base_dir() { return !no_base_dir; }
  bool      has_base_dentry() { return !no_base_dentry; }
  bool has_root() {
    if (base_ino == 0) {
      assert(no_base_dir && no_base_dentry);
      return true;
    }
	return false;
  }

  // these index _arguments_ are aligned to the inodes.
  CDirDiscover* get_dir(int n) { return dirs[n + no_base_dir]; }
  string& get_dentry(int n) { return path[n + no_base_dentry]; }
  CInodeDiscover* get_inode(int n) { return inodes[n]; }

  // cons
  MDiscoverReply() {}
  MDiscoverReply(inodeno_t base_ino) :
	Message(MSG_MDS_DISCOVERREPLY) {
	this->base_ino = base_ino;
	no_base_dir = no_base_dentry = false;
  }
  virtual char *get_type_name() { return "DisR"; }
  
  // builders
  bool is_empty() {
    return dirs.empty() && inodes.empty();
  }
  void set_path(filepath& dp) { path = dp; }
  void add_dentry(string& dn) { 
	if (dirs.empty()) no_base_dir = true;
    path.add_dentry(dn);
  }

  void add_inode(CInodeDiscover& din) {
	if (inodes.empty() && dirs.empty()) no_base_dir = true;
	if (inodes.empty() && dentries.empty()) no_base_dentry = true;
    inodes.add( din );
  }

  void add_dir(CDirDiscover& dir) {
    dirs.add( dir );
  }


  // ...
  virtual int decode_payload(crope r, int off = 0) {
	r.copy(off, sizeof(base_ino), (char*)&base_ino);
    off += sizeof(base_ino);
    r.copy(off, sizeof(bool), (char*)&no_base_dir);
    off += sizeof(bool);
    r.copy(off, sizeof(bool), (char*)&no_base_dentry);
    off += sizeof(bool);
    
    // dirs
    int n;
    r.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      dirs[i] = CDirDiscover();
      off = dirs[i]._unrope(r, off);
    }

    // filepath
    off = path._unrope(r, off);

    // inodes
    r.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      inodes[i] = CInodeDiscover();
      off = inodes[i]._unrope(r, off);
    }
    return off;
  }
  virtual crope get_payload() {
	crope r;
	r.append((char*)&base_ino, sizeof(base_ino));
	r.append((char*)&no_base_dir, sizeof(bool));
	r.append((char*)&no_base_dentry, sizeof(bool));

    int n = dirs.size();
    r.append((char*)&n, sizeof(int));
    for (vector<CDirDiscover>::iterator it = dirs.begin();
         it != dirs.end();
         it++) 
      r.append((*it)._rope());
    
    r.append(path._rope());

    n = inodes.size();
    for (vector<CInodeDiscover>::iterator it = dirs.begin();
         it != dirs.end();
         it++) 
      r.append((*it)._rope());
    
    return r;
  }

};

#endif
