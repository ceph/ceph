#ifndef __MLOCK_H
#define __MLOCK_H

#include "include/Message.h"

#define LOCK_OTYPE_INO    1
#define LOCK_OTYPE_DIRINO 2
#define LOCK_OTYPE_DN     3

// basic messages
#define LOCK_AC_LOCK          1
#define LOCK_AC_LOCKACK       2
#define LOCK_AC_SYNC          3
#define LOCK_AC_SYNCACK       4
#define LOCK_AC_REQSYNC       5
#define LOCK_AC_DELETE        6
#define LOCK_AC_DELETEACK     7

// async messages
#define LOCK_AC_ASYNC         8
#define LOCK_AC_ASYNCACK      9
#define LOCK_AC_REQASYNC     10


class MLock : public Message {
  int       asker;  // who is initiating this request
  int       action;  // action type

  char      otype;  // lock object type
  inodeno_t ino;    // ino ref, or possibly
  string    dn;     // dentry name
  crope     data;   // and possibly some data

 public:
  inodeno_t get_ino() { return ino; }
  string& get_dn() { return dn; }
  crope& get_data() { return data; }
  int get_asker() { return asker; }
  int get_action() { return action; }
  int get_otype() { return otype; }

  MLock() {}
  MLock(int action, int asker) :
	Message(MSG_MDS_LOCK) {
	this->action = action;
	this->asker = asker;
  }
  virtual char *get_type_name() { return "ILock"; }
  
  void set_ino(inodeno_t ino) {
	otype = LOCK_OTYPE_INO;
	this->ino = ino;
  }
  void set_dirino(inodeno_t dirino) {
	otype = LOCK_OTYPE_DIRINO;
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
  
  virtual int decode_payload(crope s) {
	int off = 0;
	s.copy(off,sizeof(action), (char*)&action);
	off += sizeof(action);

	s.copy(off,sizeof(asker), (char*)&asker);
	off += sizeof(asker);
	
	s.copy(off,sizeof(otype), (char*)&otype);
	off += sizeof(otype);

	s.copy(off,sizeof(inodeno_t), (char*)&ino);
	off += sizeof(ino);
	
	dn = s.c_str() + off;
	off += dn.length() + 1;

	int len;
	s.copy(off, sizeof(len), (char*)&len);
	off += sizeof(len);
	data = s.substr(off, len);
	off += len;
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&action, sizeof(action));
	s.append((char*)&asker, sizeof(asker));

	s.append((char*)&otype, sizeof(otype));

	s.append((char*)&ino, sizeof(inodeno_t));

	s.append((char*)dn.c_str(), dn.length()+1);

	int len = data.length();
	s.append((char*)len, sizeof(len));
	s.append(data);
	return s;
  }

};

#endif
