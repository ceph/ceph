#ifndef __MCLIENTREPLY_H
#define __MCLIENTREPLY_H

#include "include/types.h"

#include "msg/Message.h"
#include "mds/CInode.h"
#include "mds/CDir.h"
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

  bool     spec_defined;
  int      dir_auth;
  set<int> dist;    // where am i replicated?


 public:
  c_inode_info() {}
  c_inode_info(CInode *in, int whoami, string ref_dn, timepair_t& now) {
	// inode
	this->inode = in->inode;
	this->inode_soft_valid = in->softlock.can_read(in->is_auth());
	this->inode_hard_valid = in->hardlock.can_read(in->is_auth());
	
	// symlink content?
	if (in->is_symlink()) this->symlink = in->symlink;
	  
	// referring dentry?
	this->ref_dn = ref_dn;
	
	// replicated where?
	spec_defined = in->dir && in->dir->is_auth();
	if (spec_defined) {
	  dir_auth = in->dir->get_dir_auth();
	  in->dir->get_dist_spec(this->dist, whoami, now);
	} 
  }
  
  void _encode(bufferlist &bl) {
	bl.append((char*)&inode, sizeof(inode));
	bl.append((char*)&inode_soft_valid, sizeof(inode_soft_valid));
	bl.append((char*)&inode_hard_valid, sizeof(inode_hard_valid));
	bl.append((char*)&spec_defined, sizeof(spec_defined));
	bl.append((char*)&dir_auth, sizeof(dir_auth));

	::_encode(ref_dn, bl);
	::_encode(symlink, bl);
	::_encode(dist, bl);	// distn
  }
  
  void _decode(bufferlist &bl, int& off) {
	bl.copy(off, sizeof(inode), (char*)&inode);
	off += sizeof(inode);
	bl.copy(off, sizeof(inode_soft_valid), (char*)&inode_soft_valid);
	off += sizeof(inode_soft_valid);
	bl.copy(off, sizeof(inode_hard_valid), (char*)&inode_hard_valid);
	off += sizeof(inode_hard_valid);
	bl.copy(off, sizeof(spec_defined), (char*)&spec_defined);
	off += sizeof(spec_defined);
	bl.copy(off, sizeof(dir_auth), (char*)&dir_auth);
	off += sizeof(dir_auth);

	::_decode(ref_dn, bl, off);
	::_decode(symlink, bl, off);
	::_decode(dist, bl, off);
  }
};


typedef struct {
  long pcid;
  long tid;
  int op;
  int result;  // error code
  int trace_depth;
  int dir_size;
  unsigned char file_caps;  // for open
  __uint64_t file_data_version;  // for client buffercache consistency
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
  unsigned char get_file_caps() { return st.file_caps; }
  __uint64_t get_file_data_version() { return st.file_data_version; }
  
  void set_result(int r) { st.result = r; }
  void set_file_caps(unsigned char c) { st.file_caps = c; }
  void set_file_data_version(__uint64_t v) { st.file_data_version = v; }

  MClientReply() {};
  MClientReply(MClientRequest *req, int result = 0) : 
	Message(MSG_CLIENT_REPLY) {
	memset(&st, 0, sizeof(st));
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
  virtual void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(st), (char*)&st);
	off += sizeof(st);

	_decode(path, payload, off);

	for (int i=0; i<st.trace_depth; i++) {
	  c_inode_info *ci = new c_inode_info;
	  ci->_decode(payload, off);
	  trace.push_back(ci);
	}

	if (st.dir_size) {
	  for (int i=0; i<st.dir_size; i++) {
		c_inode_info *ci = new c_inode_info;
		ci->_decode(payload, off);
		dir_contents.push_back(ci);
	  }
	}
  }
  virtual void encode_payload() {
	st.dir_size = dir_contents.size();
	st.trace_depth = trace.size();
	
	payload.append((char*)&st, sizeof(st));
	_encode(path, payload);

	vector<c_inode_info*>::iterator it;
	for (it = trace.begin(); it != trace.end(); it++) 
	  (*it)->_encode(payload);

	for (it = dir_contents.begin(); it != dir_contents.end(); it++) 
	  (*it)->_encode(payload);
  }

  // builders
  void add_dir_item(c_inode_info *c) {
	dir_contents.push_back(c);
  }

  void set_trace_dist(CInode *in, int whoami, timepair_t& now) {
	while (in) {
	  // add this inode to trace, along with referring dentry name
	  string ref_dn;
	  CDentry *dn = in->get_parent_dn();
	  if (dn) ref_dn = dn->get_name();

	  trace.insert(trace.begin(), new c_inode_info(in, whoami, ref_dn, now));
	  
	  in = in->get_parent_inode();
	}
  }

};

#endif
