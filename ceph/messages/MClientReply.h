#ifndef __MCLIENTREPLY_H
#define __MCLIENTREPLY_H

#include "../include/Message.h"
#include "../include/CInode.h"

#include <vector>
using namespace std;

class CInode;

class MClientReply : public Message {
 public:
  long tid;
  int op;

  // reply data
  string path;
  vector<bit_vector> trace_dist;
  vector<inodeno_t>  trace_ino;
  vector<string>     trace_dn;

  MClientReply(MClientRequest *req) : 
	Message(MSG_CLIENT_REPLY) {
	this->tid = req->tid;
	this->op = req->op;
	this->path = req->path;
  }
  ~MClientReply() {
  }

  void set_trace_dist(vector<CInode*>& trace, 
					  vector<string>& trace_dn,
					  MDS *mds) {
	vector<CInode*>::iterator it = trace.begin();
	while (it != trace.end()) {
	  CInode *in = *(it++);
	  trace_dist.push_back( in->get_dist_spec(mds) );
	  trace_ino.push_back( in->inode.ino );
	}

	this->trace_dn = trace_dn;
  }
};

#endif
