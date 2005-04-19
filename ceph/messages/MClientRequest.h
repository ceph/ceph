#ifndef __MCLIENTREQUEST_H
#define __MCLIENTREQUEST_H

#include "msg/Message.h"
#include "include/filepath.h"
#include "mds/MDS.h"

typedef struct {
  long tid;
  int client;
  int op;

  int caller_uid, caller_gid;

  inodeno_t ino;
  int    iarg, iarg2;
  time_t targ, targ2;
} MClientRequest_st;


class MClientRequest : public Message {
  MClientRequest_st st;
  filepath path;
  string sarg;
  string sarg2;


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
  void set_caller_uid(int u) { st.caller_uid = u; }
  void set_caller_gid(int g) { st.caller_uid = g; }
  void set_iarg(int i) { st.iarg = i; }
  void set_iarg2(int i) { st.iarg2 = i; }
  void set_targ(time_t& t) { st.targ = t; }
  void set_targ2(time_t& t) { st.targ2 = t; }
  void set_sarg(string& arg) { this->sarg = arg; }
  void set_sarg2(string& arg) { this->sarg2 = arg; }

  long get_tid() { return st.tid; }
  int get_op() { return st.op; }
  int get_client() { return st.client; }
  inodeno_t get_ino() { return st.ino; }
  int get_caller_uid() { return st.caller_uid; }
  int get_caller_gid() { return st.caller_gid; }
  string& get_path() { return path.get_path(); }
  filepath& get_filepath() { return path; }
  int get_iarg() { return st.iarg; }
  int get_iarg2() { return st.iarg2; }
  time_t get_targ() { return st.targ; }
  time_t get_targ2() { return st.targ2; }
  string& get_sarg() { return sarg; }
  string& get_sarg2() { return sarg2; }

  virtual void decode_payload(crope& s) {
	s.copy(0, sizeof(st), (char*)&st);
	path.set_path( s.c_str() + sizeof(st) );
	int off = sizeof(st) + path.length() + 1;
	sarg = s.c_str() + off;	
	off += sarg.length()+1;
	sarg2 = s.c_str() + off;
	off += sarg2.length()+1;
  }

  virtual void encode_payload(crope& r) {
	r.append((char*)&st, sizeof(st));
	r.append(path.c_str());
	r.append((char)0);
	r.append(sarg.c_str());
	r.append((char)0);
	r.append(sarg2.c_str());
	r.append((char)0);
  }
};

inline ostream& operator<<(ostream& out, MClientRequest& req) {
  out << &req << " ";
  out << "client" << req.get_client() 
	  << "." << req.get_tid() 
	  << ":";
  switch(req.get_op()) {
  case MDS_OP_STAT: 
	out << "stat"; break;
  case MDS_OP_TOUCH: 
	out << "touch"; break;
  case MDS_OP_UTIME: 
	out << "utime"; break;
  case MDS_OP_CHMOD: 
	out << "chmod"; break;
  case MDS_OP_CHOWN: 
	out << "chown"; break;

  case MDS_OP_READDIR: 
	out << "readdir"; break;
  case MDS_OP_MKNOD: 
	out << "mknod"; break;
  case MDS_OP_LINK: 
	out << "link"; break;
  case MDS_OP_UNLINK:
	out << "unlink"; break;
  case MDS_OP_RENAME:
	out << "rename"; break;

  case MDS_OP_MKDIR: 
	out << "mkdir"; break;
  case MDS_OP_RMDIR: 
	out << "rmdir"; break;
  case MDS_OP_SYMLINK: 
	out << "symlink"; break;

  case MDS_OP_OPEN: 
	out << "open"; break;
  case MDS_OP_TRUNCATE: 
	out << "truncate"; break;
  case MDS_OP_FSYNC: 
	out << "fsync"; break;
  case MDS_OP_CLOSE: 
	out << "close"; break;
  default: 
	out << req.get_op();
  }
  if (req.get_path().length()) 
	out << "=" << req.get_path();
  if (req.get_sarg().length())
	out << " " << req.get_sarg();
  return out;
}

#endif
