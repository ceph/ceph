#ifndef __MDISCOVERREPLY_H
#define __MDISCOVERREPLY_H

#include "include/Message.h"
#include "mds/CDir.h"
#include "mds/CInode.h"
#include "include/filepath.h"

#include <vector>
#include <string>
using namespace std;

// normal: d+inode + (dir + d+inode)*
// root:   inode + dir
// dir dis: dir



class MDiscoverReply : public Message {
  inodeno_t    base_ino;
  bool         no_base_dir;
  bool         no_base_dentry;
  bool        flag_forward;
  bool        flag_error;

  // ... + dir + dentry + inode
  // inode [ + ... ], base_ino = 0 : discover base_ino=0, start w/ root ino
  // dentry + inode [ + ... ]      : discover want_base_dir=false
  // (dir + dentry + inode) +      : discover want_base_dir=true
  vector<CDirDiscover*>   dirs;      // not inode-aligned if no_base_dir = true.
  filepath                path;      // not inode-aligned in no_base_dentry = true
  vector<CInodeDiscover*> inodes;

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
  string& get_path() { return path.get_path(); }

  bool is_flag_forward() { return flag_forward; }
  bool is_flag_error() { return flag_error; }

  // these index _arguments_ are aligned to the inodes.
  CDirDiscover& get_dir(int n) { return *(dirs[n - no_base_dir]); }
  string& get_dentry(int n) { return path[n - no_base_dentry]; }
  CInodeDiscover& get_inode(int n) { return *(inodes[n]); }
  inodeno_t get_ino(int n) { return inodes[n]->get_ino(); }

  // cons
  MDiscoverReply() {}
  MDiscoverReply(inodeno_t base_ino) :
	Message(MSG_MDS_DISCOVERREPLY) {
	this->base_ino = base_ino;
	flag_forward = flag_error = no_base_dir = no_base_dentry = false;
  }
  ~MDiscoverReply() {
	for (vector<CDirDiscover*>::iterator it = dirs.begin();
         it != dirs.end();
         it++) 
	  delete *it;
    for (vector<CInodeDiscover*>::iterator it = inodes.begin();
         it != inodes.end();
         it++) 
	  delete *it;
  }
  virtual char *get_type_name() { return "DisR"; }
  
  // builders
  bool is_empty() {
    return dirs.empty() && inodes.empty() && !flag_forward && !flag_error;
  }
  void set_path(filepath& dp) { path = dp; }
  void add_dentry(string& dn) { 
	if (dirs.empty()) no_base_dir = true;
    path.add_dentry(dn);
  }

  void add_inode(CInodeDiscover* din) {
	if (inodes.empty() && dirs.empty()) no_base_dir = true;
	if (inodes.empty() && path.depth() == 0) no_base_dentry = true;
    inodes.push_back( din );
  }

  void add_dir(CDirDiscover* dir) {
    dirs.push_back( dir );
  }

  void set_flag_forward() { flag_forward = true; }
  void set_flag_error() { flag_error = true; }


  // ...
  virtual int decode_payload(crope r) {
	int off = 0;
	r.copy(off, sizeof(base_ino), (char*)&base_ino);
    off += sizeof(base_ino);
    r.copy(off, sizeof(bool), (char*)&no_base_dir);
    off += sizeof(bool);
    r.copy(off, sizeof(bool), (char*)&no_base_dentry);
    off += sizeof(bool);
    r.copy(off, sizeof(bool), (char*)&flag_forward);
    off += sizeof(bool);
    r.copy(off, sizeof(bool), (char*)&flag_error);
    off += sizeof(bool);
    
    // dirs
    int n;
    r.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      dirs.push_back( new CDirDiscover() );
      off = dirs[i]->_unrope(r, off);
    }
	dout(12) << n << " dirs out" << endl;

    // inodes
    r.copy(off, sizeof(int), (char*)&n);
    off += sizeof(int);
    for (int i=0; i<n; i++) {
      inodes.push_back( new CInodeDiscover() );
      off = inodes[i]->_unrope(r, off);
    }
	dout(12) << n << " inodes out" << endl;

    // filepath
    off = path._unrope(r, off);
	dout(12) << path.depth() << " dentries out" << endl;

    return off;
  }
  virtual crope get_payload() {
	crope r;
	r.append((char*)&base_ino, sizeof(base_ino));
	r.append((char*)&no_base_dir, sizeof(bool));
	r.append((char*)&no_base_dentry, sizeof(bool));
	r.append((char*)&flag_forward, sizeof(bool));
	r.append((char*)&flag_forward, sizeof(bool));

	// dirs
    int n = dirs.size();
    r.append((char*)&n, sizeof(int));
    for (vector<CDirDiscover*>::iterator it = dirs.begin();
         it != dirs.end();
         it++) 
      r.append((*it)->_rope());
	dout(12) << n << " dirs in" << endl;
    
	// inodes
    n = inodes.size();
    r.append((char*)&n, sizeof(int));
    for (vector<CInodeDiscover*>::iterator it = inodes.begin();
         it != inodes.end();
         it++) 
      r.append((*it)->_rope());
	dout(12) << n << " inodes in" << endl;

	// path
    r.append(path._rope());
	dout(12) << path.depth() << " dentries in" << endl;
    
    return r;
  }

};

#endif
