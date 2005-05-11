#ifndef __MCLIENTREPLY_H
#define __MCLIENTREPLY_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "mds/CDentry.h"

#include <vector>
using namespace std;

class CInode;

/***
 *
 * MClientReply - container message for MDS reply to a client's MClientRequest
 *
 * key fields:
 *  long tid - transaction id, so the client can match up with pending request
 *  int result - error code, or fh if it was open
 *
 * for most requests:
 *  trace is a vector of c_inoe_info's tracing from root to the file/dir/whatever
 *  the operation referred to, so that the client can update it's info about what
 *  metadata lives on what MDS.
 *
 * for readdir replies:
 *  dir_contents is a vector c_inode_info*'s.  
 * 
 * that's mostly it, i think!
 *
 */

class c_inode_info {
 public:
  inode_t inode;
  string ref_dn;    // referring dentry (blank if root)
  string symlink;   // symlink content (if symlink)

  bool inode_soft_valid;  // true if inode info is valid (ie was readable on mds at the time)
  bool inode_hard_valid;  // true if inode info is valid (ie was readable on mds at the time)

  set<int> dist;    // where am i replicated?

 public:
  c_inode_info() {}
  c_inode_info(CInode *in, int whoami, string ref_dn) {
	// inode
	this->inode = in->inode;
	this->inode_soft_valid = in->softlock.can_read(in->is_auth());
	this->inode_hard_valid = in->hardlock.can_read(in->is_auth());
	
	// symlink content?
	if (in->is_symlink()) this->symlink = in->symlink;
	  
	// referring dentry?
	this->ref_dn = ref_dn;
	
	// replicated where?
	in->get_dist_spec(this->dist, whoami);
  }
  
  void _rope(crope &s) {
	s.append((char*)&inode, sizeof(inode));
	s.append((char*)&inode_soft_valid, sizeof(inode_soft_valid));
	s.append((char*)&inode_hard_valid, sizeof(inode_hard_valid));

	if (ref_dn.length()) s.append(ref_dn.c_str());
	s.append((char)0);
	if (symlink.length()) s.append(symlink.c_str());
	s.append((char)0);

	// distn
	int n = dist.size();
	s.append((char*)&n, sizeof(int));
	for (set<int>::iterator it = dist.begin();
		 it != dist.end();
		 it++) {
	  int j = *it;
	  s.append((char*)&j,sizeof(int));
	}
  }
  
  void _unrope(crope &s, int& off) {
	s.copy(off, sizeof(inode), (char*)&inode);
	off += sizeof(inode);
	s.copy(off, sizeof(inode_soft_valid), (char*)&inode_soft_valid);
	off += sizeof(inode_soft_valid);
	s.copy(off, sizeof(inode_hard_valid), (char*)&inode_hard_valid);
	off += sizeof(inode_hard_valid);

	ref_dn = s.c_str() + off;
	off += ref_dn.length() + 1;

	symlink = s.c_str() + off;
	off += symlink.length() + 1;

	int l;
	s.copy(off, sizeof(int), (char*)&l);
	off += sizeof(int);
	for (int i=0; i<l; i++) {
	  int j;
	  s.copy(off, sizeof(int), (char*)&j);
	  off += sizeof(int);
	  dist.insert(j);
	}
  }
};


typedef struct {
  long pcid;
  long tid;
  int op;
  int result;  // error code
  int trace_depth;
  int dir_size;
} MClientReply_st;

class MClientReply : public Message {
  // reply data
  MClientReply_st st;
 
  string path;
  vector<c_inode_info*> trace;
  vector<c_inode_info*> dir_contents;

 public:
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  long get_tid() { return st.tid; }
  int get_op() { return st.op; }
  inodeno_t get_ino() { return trace[trace.size()-1]->inode.ino; }
  int get_result() { return st.result; }
  const string& get_path() { return path; }
  const vector<c_inode_info*>& get_trace() { return trace; }
  vector<c_inode_info*>& get_dir_contents() { return dir_contents; }
  
  void set_result(int r) { st.result = r; }
  
  MClientReply() {};
  MClientReply(MClientRequest *req, int result = 0) : 
	Message(MSG_CLIENT_REPLY) {
	this->st.pcid = req->get_pcid();    // match up procedure call id!!!
	this->st.tid = req->get_tid();
	this->st.op = req->get_op();
	this->path = req->get_path();

	this->st.result = result;
	st.trace_depth = 0;
	st.dir_size = 0;
  }
  virtual ~MClientReply() {
	vector<c_inode_info*>::iterator it;
	
	for (it = trace.begin(); it != trace.end(); it++) 
	  delete *it;
	
	for (it = dir_contents.begin(); it != dir_contents.end(); it++) 
	  delete *it;
  }
  virtual char *get_type_name() { return "creply"; }


  // serialization
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(st), (char*)&st);
	off += sizeof(st);

	path = s.c_str() + off;
	off += path.length() + 1;

	for (int i=0; i<st.trace_depth; i++) {
	  c_inode_info *ci = new c_inode_info;
	  ci->_unrope(s, off);
	  trace.push_back(ci);
	}

	if (st.dir_size) {
	  for (int i=0; i<st.dir_size; i++) {
		c_inode_info *ci = new c_inode_info;
		ci->_unrope(s, off);
		dir_contents.push_back(ci);
	  }
	}
  }
  virtual void encode_payload(crope& r) {
	st.dir_size = dir_contents.size();
	st.trace_depth = trace.size();
	
	r.append((char*)&st, sizeof(st));
	r.append(path.c_str(), path.length()+1);

	vector<c_inode_info*>::iterator it;
	for (it = trace.begin(); it != trace.end(); it++) 
	  (*it)->_rope(r);

	for (it = dir_contents.begin(); it != dir_contents.end(); it++) 
	  (*it)->_rope(r);
  }

  // builders
  void add_dir_item(c_inode_info *c) {
	dir_contents.push_back(c);
  }

  void set_trace_dist(CInode *in, int whoami) {
	while (in) {
	  // add this inode to trace, along with referring dentry name
	  string ref_dn;
	  CDentry *dn = in->get_parent_dn();
	  if (dn) ref_dn = dn->get_name();

	  trace.insert(trace.begin(), new c_inode_info(in, whoami, ref_dn));
	  
	  in = in->get_parent_inode();
	}
  }

};

#endif
