#ifndef __MLOCK_H
#define __MLOCK_H

#include "msg/Message.h"

#define LOCK_OTYPE_IHARD  1
#define LOCK_OTYPE_ISOFT  2
#define LOCK_OTYPE_DIR    3
#define LOCK_OTYPE_DN     4

// for replicas
#define LOCK_AC_SYNC          0
#define LOCK_AC_ASYNC         1

#define LOCK_AC_SYNC_MODE     2
#define LOCK_AC_LOCK_MODE     3
#define LOCK_AC_ASYNC_MODE    4

#define LOCK_AC_LOCK          5  // nakable
#define LOCK_AC_GSYNC         6  //  "
#define LOCK_AC_GLOCK         7  //  "
#define LOCK_AC_GASYNC        8  //  "


#define LOCK_AC_FOR_REPLICA(a)  ((a) <= 8)
#define LOCK_AC_FOR_AUTH(a)     ((a) >= 9)

#define LOCK_AC_NAKOFFSET     4  // be careful with numbering!

// for auth
#define LOCK_AC_LOCKNAK       9
#define LOCK_AC_GSYNCNAK     10
#define LOCK_AC_GLOCKNAK     11
#define LOCK_AC_GASYNCNAK    12
#define LOCK_AC_LOCKACK      13
#define LOCK_AC_GSYNCACK     14
#define LOCK_AC_GLOCKACK     15
#define LOCK_AC_GASYNCACK    16
#define LOCK_AC_REQREAD      17
#define LOCK_AC_REQWRITE     18

#define LOCK_AC_REQXLOCK     20
#define LOCK_AC_REQXLOCKACK  21
#define LOCK_AC_REQXLOCKNAK  21

#define lock_ac_name(x)      


class MLock : public Message {
  int       asker;  // who is initiating this request
  int       action;  // action type

  char      otype;  // lock object type
  inodeno_t ino;    // ino ref, or possibly
  string    dn;     // dentry name
  crope     data;   // and possibly some data
  string    path;   // possibly a path too (for dentry lock discovers)

 public:
  inodeno_t get_ino() { return ino; }
  string& get_dn() { return dn; }
  crope& get_data() { return data; }
  int get_asker() { return asker; }
  int get_action() { return action; }
  int get_otype() { return otype; }
  string& get_path() { return path; }

  MLock() {}
  MLock(int action, int asker) :
	Message(MSG_MDS_LOCK) {
	this->action = action;
	this->asker = asker;
  }
  virtual char *get_type_name() { return "ILock"; }
  
  void set_ino(inodeno_t ino, char ot) {
	otype = ot;
	this->ino = ino;
  }
  void set_dirino(inodeno_t dirino) {
	otype = LOCK_OTYPE_DIR;
	this->ino = ino;
  }
  void set_dn(inodeno_t dirino, string& dn) {
	otype = LOCK_OTYPE_DN;
	this->ino = dirino;
	this->dn = dn;
  }
  void set_data(crope& data) {
	this->data = data;
  }
  void set_path(const string& p) {
	path = p;
  }
  
  virtual void decode_payload(crope& s) {
	int off = 0;
	s.copy(off,sizeof(action), (char*)&action);
	off += sizeof(action);

	s.copy(off,sizeof(asker), (char*)&asker);
	off += sizeof(asker);
	
	s.copy(off,sizeof(otype), (char*)&otype);
	off += sizeof(otype);

	s.copy(off,sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	
	dn = s.c_str() + off;
	off += dn.length() + 1;

	path = s.c_str() + off;
	off += path.length() + 1;

	int len;
	s.copy(off, sizeof(len), (char*)&len);
	off += sizeof(len);
	data = s.substr(off, len);
	off += len;
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&action, sizeof(action));
	s.append((char*)&asker, sizeof(asker));

	s.append((char*)&otype, sizeof(otype));

	s.append((char*)&ino, sizeof(inodeno_t));

	s.append((char*)dn.c_str(), dn.length()+1);
	s.append((char*)path.c_str(), path.length()+1);

	int len = data.length();
	s.append((char*)&len, sizeof(len));
	s.append(data);
  }

};

#endif
