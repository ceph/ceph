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

class MClientReply : public Message {
 public:
  long tid;
  int op;
  int result;  // error code

  // reply data
  string path;
  vector<c_inode_info*> trace;
  vector<c_inode_info*> *dir_contents;

  MClientReply(MClientRequest *req, int result = 0) : 
	Message(MSG_CLIENT_REPLY) {
	this->tid = req->tid;
	this->op = req->op;
	this->path = req->path;
	this->result = result;
	dir_contents = 0;
  }
  ~MClientReply() {
	if (dir_contents) {
	  vector<c_inode_info*>::iterator it;
	  
	  for (it = trace.begin(); it != trace.end(); it++) 
		delete *it;
	  
	  for (it = dir_contents->begin(); it != dir_contents->end(); it++) 
		delete *it;
	  delete dir_contents;
	  dir_contents = 0;
	}
  }

  void add_dir_item(c_inode_info *c) {
	if (!dir_contents)
	  dir_contents = new vector<c_inode_info*>;
	dir_contents->push_back(c);
  }

  void set_trace_dist(vector<CInode*>& tr, 
					  vector<string>& trace_dn,
					  int authority) {
	vector<CInode*>::iterator it = tr.begin();
	int p = 0;
	while (it != tr.end()) {
	  CInode *in = *(it++);
	  c_inode_info *i = new c_inode_info;
	  i->inode = in->inode;

	  in->get_dist_spec(i->dist, authority);

	  if (p) 
		i->ref_dn = trace_dn[p-1];
	  else
		i->ref_dn = ""; // root
	  p++;
	  trace.push_back(i);
	}
  }
};

#endif
