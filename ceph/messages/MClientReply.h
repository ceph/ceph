#ifndef __MCLIENTREPLY_H
#define __MCLIENTREPLY_H

#include "include/Message.h"
#include "mds/CInode.h"

#include <vector>
using namespace std;

class CInode;

typedef struct {
  inode_t inode;
  set<int> dist;
  string ref_dn;    // referring dentry (blank if root)
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
  ~MClientReply() {
	vector<c_inode_info*>::iterator it;
	
	for (it = trace.begin(); it != trace.end(); it++) 
	  delete *it;
	
	for (it = dir_contents.begin(); it != dir_contents.end(); it++) 
	  delete *it;
  }
  virtual char *get_type_name() { return "creply"; }

  // serialization
  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(st), (char*)&st);
	path = s.c_str() + sizeof(st);
	size_t off = sizeof(st) + path.length() + 1;
	
	for (int i=0; i<st.trace_depth; i++) {
	  c_inode_info *ci = new c_inode_info;
	  s.copy(off, sizeof(c_inode_info), (char*)ci);
	  trace.push_back(ci);
	  off += sizeof(c_inode_info);
	}

	if (st.dir_size) {
	  for (int i=0; i<st.dir_size; i++) {
		c_inode_info *ci = new c_inode_info;
		s.copy(off, sizeof(c_inode_info), (char*)ci);
		dir_contents.push_back(ci);
		off += sizeof(c_inode_info);
	  }
	}
  }
  virtual crope get_payload() {
	st.dir_size = dir_contents.size();
	st.trace_depth = trace.size();
	
	crope r;
	r.append((char*)&st, sizeof(st));
	r.append(path.c_str());
	
	vector<c_inode_info*>::iterator it;
	for (it = trace.begin(); it != trace.end(); it++) 
	  r.append((char*)*it, sizeof(c_inode_info));

	for (it = dir_contents.begin(); it != dir_contents.end(); it++) 
	  r.append((char*)*it, sizeof(c_inode_info));

	return r;
  }

  // builders
  void add_dir_item(c_inode_info *c) {
	dir_contents.push_back(c);
  }

  void set_trace_dist(CInode *in, int whoami) {
	while (in) {
	  c_inode_info *info = new c_inode_info;
	  info->inode = in->inode;
	  in->get_dist_spec(info->dist, whoami);
	  trace.insert(trace.begin(), info);

	  in = in->get_parent_inode();
	}
  }

};

#endif
