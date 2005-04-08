#ifndef __MCLIENTREQUEST_H
#define __MCLIENTREQUEST_H

#include "msg/Message.h"
#include "include/filepath.h"
#include "mds/MDS.h"

typedef struct {
  long tid;
  int client;
  int op;
  inodeno_t ino;
  int iarg;
} MClientRequest_st;


class MClientRequest : public Message {
  MClientRequest_st st;
  filepath path;
  string arg;
  string arg2;

 public:
  MClientRequest() {}
  MClientRequest(long tid, int op, int client) : Message(MSG_CLIENT_REQUEST) {
	this->st.tid = tid;
	this->st.op = op;
	this->st.client = client;
	this->st.ino = 0;
	this->st.iarg = 0;
  }
  virtual char *get_type_name() { return "creq"; }

  void set_path(string& p) { path.set_path(p); }
  void set_ino(inodeno_t ino) { st.ino = ino; }
  void set_iarg(int i) { st.iarg = i; }
  void set_arg(string& arg) { this->arg = arg; }
  void set_arg2(string& arg) { this->arg2 = arg; }

  long get_tid() { return st.tid; }
  int get_op() { return st.op; }
  int get_client() { return st.client; }
  inodeno_t get_ino() { return st.ino; }
  string& get_path() { return path.get_path(); }
  filepath& get_filepath() { return path; }
  int get_iarg() { return st.iarg; }
  string& get_arg() { return arg; }
  string& get_arg2() { return arg2; }

  virtual void decode_payload(crope& s) {
	s.copy(0, sizeof(st), (char*)&st);
	path.set_path( s.c_str() + sizeof(st) );
	int off = sizeof(st) + path.length() + 1;
	arg = s.c_str() + off;	
	off += arg.length()+1;
	arg2 = s.c_str() + off;
	off += arg2.length()+1;
  }

  virtual void encode_payload(crope& r) {
	r.append((char*)&st, sizeof(st));
	r.append(path.c_str());
	r.append((char)0);
	r.append(arg.c_str());
	r.append((char)0);
	r.append(arg2.c_str());
	r.append((char)0);
  }
};

inline ostream& operator<<(ostream& out, MClientRequest& req) {
  out << "client" << req.get_client() 
	  << "." << req.get_tid() 
	  << ":";
  switch(req.get_op()) {
  case MDS_OP_TOUCH: 
	out << "touch"; break;
  case MDS_OP_CHMOD: 
	out << "chmod"; break;
  case MDS_OP_STAT: 
	out << "stat"; break;
  case MDS_OP_READDIR: 
	out << "readdir"; break;
  case MDS_OP_OPEN: 
	out << "open"; break;
  case MDS_OP_UNLINK:
	out << "unlink"; break;
  case MDS_OP_CLOSE: 
	out << "close"; break;
  default: 
	out << req.get_op();
  }
  if (req.get_path().length()) 
	out << "=" << req.get_path();
  return out;
}

#endif
