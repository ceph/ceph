#ifndef __MCLIENTREQUEST_H
#define __MCLIENTREQUEST_H

#include <vector>

#include "msg/Message.h"
#include "include/filepath.h"
#include "mds/MDS.h"

/**
 *
 * MClientRequest - container for a client METADATA request.  created/sent by clients.  
 *    can be forwarded around between MDS's.
 *
 *   int client - the originating client
 *   long pcid  - procedure call id, used to match request+response.
 *   long tid   - transaction id, unique among requests for that client.  probably just a counter!
 *                -> the MDS passes the Request to the Reply constructor, so this always matches.
 *  
 *   int op - the metadata op code.  MDS_OP_RENAME, etc.
 *   int caller_uid, _gid - guess
 * 
 * arguments:  one or more of these are defined, depending on the metadata op:
 *   inodeno  ino  - used by close(), along with fh.  not strictly necessary except MDS is currently coded lame.
 *   filepath path - main file argument (almost everything)
 *   string   sarg - string argument (if a second arg is needed, e.g. rename, symlink)
 *   int  iarg     - int arg... file mode for open, fh for close, mode for mkdir, etc.
 *   int  iarg2    - second int arg... gid for chown (iarg is uid)
 *   time_t targ, targ2  - time args, used by utime
 *
 * That's basically it!
 *  
 */


typedef struct {
  long pcid;   // procedure call id, to match request+response
  long tid;
  int client;
  int op;

  int caller_uid, caller_gid;
  inodeno_t ino;

  int    iarg, iarg2;
  time_t targ, targ2;

  size_t sizearg;
} MClientRequest_st;


class MClientRequest : public Message {
  MClientRequest_st st;
  filepath path;
  string sarg;
  string sarg2;


 public:
  MClientRequest() {}
  MClientRequest(int op, int client) : Message(MSG_CLIENT_REQUEST) {
	memset(&st, 0, sizeof(st));
	this->st.op = op;
	this->st.client = client;
	this->st.iarg = 0;
  }
  virtual char *get_type_name() { return "creq"; }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  // normal fields
  void set_tid(long t) { st.tid = t; }
  void set_path(string& p) { path.set_path(p); }
  void set_path(const char *p) { path.set_path(p); }
  void set_caller_uid(int u) { st.caller_uid = u; }
  void set_caller_gid(int g) { st.caller_gid = g; }
  void set_ino(inodeno_t ino) { st.ino = ino; }
  void set_iarg(int i) { st.iarg = i; }
  void set_iarg2(int i) { st.iarg2 = i; }
  void set_targ(time_t& t) { st.targ = t; }
  void set_targ2(time_t& t) { st.targ2 = t; }
  void set_sarg(string& arg) { this->sarg = arg; }
  void set_sarg(const char *arg) { this->sarg = arg; }
  void set_sarg2(string& arg) { this->sarg2 = arg; }
  void set_sizearg(size_t s) { st.sizearg = s; }

  int get_client() { return st.client; }
  long get_tid() { return st.tid; }
  int get_op() { return st.op; }
  int get_caller_uid() { return st.caller_uid; }
  int get_caller_gid() { return st.caller_gid; }
  inodeno_t get_ino() { return st.ino; }
  string& get_path() { return path.get_path(); }
  filepath& get_filepath() { return path; }
  int get_iarg() { return st.iarg; }
  int get_iarg2() { return st.iarg2; }
  time_t get_targ() { return st.targ; }
  time_t get_targ2() { return st.targ2; }
  string& get_sarg() { return sarg; }
  string& get_sarg2() { return sarg2; }
  size_t get_sizearg() { return st.sizearg; }

  virtual void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(st), (char*)&st);
	off += sizeof(st);
	path._decode(payload, off);
	_decode(sarg, payload, off);
	_decode(sarg2, payload, off);
  }

  virtual void encode_payload() {
	payload.append((char*)&st, sizeof(st));
	path._encode(payload);
	_encode(sarg, payload);
	_encode(sarg2, payload);
  }
};

inline ostream& operator<<(ostream& out, MClientRequest& req) {
  out << &req << " ";
  out << "client" << req.get_client() 
	  << "." << req.get_tid() 
	  << ".pcid=" << req.get_pcid() 
	  << ":";
  switch(req.get_op()) {
  case MDS_OP_STAT: 
	out << "stat"; break;
  case MDS_OP_LSTAT: 
	out << "lstat"; break;
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
	out << "unknown=" << req.get_op();
  }
  if (req.get_path().length()) 
	out << "=" << req.get_path();
  if (req.get_sarg().length())
	out << " " << req.get_sarg();
  return out;
}

#endif
