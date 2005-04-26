#ifndef __MCLIENTREPLY_H
#define __MCLIENTREPLY_H

#include "msg/Message.h"
#include "mds/CInode.h"

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

typedef struct {
  inode_t inode;
  set<int> dist;
  string ref_dn;    // referring dentry (blank if root)
  bool is_sync;     
} c_inode_info;

typedef struct {
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

  
  crope rope_info(c_inode_info *ci) {
	crope s;
	s.append((char*)&ci->inode, sizeof(inode_t));
	s.append((char*)&ci->is_sync, sizeof(bool));

	int n = ci->dist.size();
	s.append((char*)&n, sizeof(int));
	for (set<int>::iterator it = ci->dist.begin();
		 it != ci->dist.end();
		 it++) {
	  int j = *it;
	  s.append((char*)&j,sizeof(int));
	}

	s.append(ci->ref_dn.c_str());
	s.append((char)0);
	return s;
  }
  int unrope_info(c_inode_info *ci, crope s) {
	s.copy(0, sizeof(inode_t), (char*)(&ci->inode));
	int off = sizeof(inode_t);
	s.copy(off, sizeof(bool), (char*)(&ci->is_sync));
	off += sizeof(bool);

	int l;
	s.copy(off, sizeof(int), (char*)&l);
	off += sizeof(int);
	for (int i=0; i<l; i++) {
	  int j;
	  s.copy(off, sizeof(int), (char*)&j);
	  off += sizeof(int);
	  ci->dist.insert(j);
	}

	ci->ref_dn = s.c_str() + off;
	return off + ci->ref_dn.length() + 1;
  }

  // serialization
  virtual void decode_payload(crope& s) {
	crope::iterator sp = s.mutable_begin();
	s.copy(0, sizeof(st), (char*)&st);
	path = s.c_str() + sizeof(st);
	sp += sizeof(st) + path.length() + 1;
	
	for (int i=0; i<st.trace_depth; i++) {
	  c_inode_info *ci = new c_inode_info;
	  sp += unrope_info(ci, s.substr(sp, s.end()));
	  trace.push_back(ci);
	}

	if (st.dir_size) {
	  for (int i=0; i<st.dir_size; i++) {
		c_inode_info *ci = new c_inode_info;
		sp += unrope_info(ci, s.substr(sp, s.end()));
		dir_contents.push_back(ci);
	  }
	}
  }
  virtual void encode_payload(crope& r) {
	st.dir_size = dir_contents.size();
	st.trace_depth = trace.size();
	
	r.append((char*)&st, sizeof(st));
	if (path.length()) r.append(path.c_str());
	r.append((char)0);
	
	vector<c_inode_info*>::iterator it;
	for (it = trace.begin(); it != trace.end(); it++) 
	  r.append(rope_info(*it));

	for (it = dir_contents.begin(); it != dir_contents.end(); it++) 
	  r.append(rope_info(*it));
  }

  // builders
  void add_dir_item(c_inode_info *c) {
	dir_contents.push_back(c);
  }

  void set_trace_dist(CInode *in, int whoami) {
	while (in) {
	  c_inode_info *info = new c_inode_info;
	  info->inode = in->inode;
	  //info->is_sync = in->is_sync() || in->is_presync();
	  in->get_dist_spec(info->dist, whoami);
	  trace.insert(trace.begin(), info);

	  in = in->get_parent_inode();
	}
  }

};

#endif
