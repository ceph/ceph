#ifndef __MCLIENTREQUEST_H
#define __MCLIENTREQUEST_H

#include "include/Message.h"

typedef struct {
  long tid;
  int op;
  int client;
  inodeno_t ino;
} MClientRequest_st;


class MClientRequest : public Message {
  MClientRequest_st st;
  string path;

 public:
  MClientRequest() {}
  MClientRequest(long tid, int op, int client) : Message(MSG_CLIENT_REQUEST) {
	this->st.tid = tid;
	this->st.op = op;
	this->st.client = client;
	this->st.ino = 0;
  }
  virtual char *get_type_name() { return "creq"; }

  void set_path(string& p) { path = p; }
  void set_ino(inodeno_t ino) { st.ino = ino; }

  long get_tid() { return st.tid; }
  int get_op() { return st.op; }
  int get_client() { return st.client; }
  inodeno_t get_ino() { return st.ino; }
  string& get_path() { return path; }

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(st), (char*)&st);
	path = s.c_str() + sizeof(st);
	return 0;
  }

  virtual crope get_payload() {
	crope r;
	r.append((char*)&st, sizeof(st));
	r.append(path.c_str());
	return r;
  }
};

inline ostream& operator<<(ostream& out, MClientRequest& req) {
  out << "client" << req.get_client() 
	  << "." << req.get_tid() 
	  << ":" << req.get_op();
  if (req.get_path().length()) 
	out << "@" << req.get_path();
  return out;
}

#endif
