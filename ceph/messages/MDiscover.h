#ifndef __MDISCOVER_H
#define __MDISCOVER_H

#include "include/Message.h"
#include "mds/CDir.h"
#include "include/filepath.h"

#include <vector>
#include <string>
using namespace std;


class MDiscover : public Message {
  int             asker;
  inodeno_t       base_ino;          // 0 -> none, want root
  bool            want_base_dir;
  bool            want_root_inode;
  
  filepath        want;   // ... [/]need/this/stuff

 public:
  int       get_asker() { return asker; }
  inodeno_t get_base_ino() { return base_ino; }
  filepath& get_want() { return want; }
  string&   get_dentry(int n) { return want[n]; }
  bool      wants_base_dir() { return want_base_dir; }

  MDiscover() { }
  MDiscover(int asker, 
			inodeno_t base_ino,
			filepath& want,
			bool want_base_dir = true,
			bool want_root_inode = false) :
	Message(MSG_MDS_DISCOVER) {
	this->asker = asker;
	this->base_ino = base_ino;
	this->want = want;
	this->want_base_dir = want_base_dir;
  }
  virtual char *get_type_name() { return "Dis"; }

  virtual void decode_payload(crope& r) {
    int off = 0;
	r.copy(off, sizeof(asker), (char*)&asker);
    off += sizeof(asker);
    r.copy(off, sizeof(base_ino), (char*)&base_ino);
    off += sizeof(base_ino);
    r.copy(off, sizeof(bool), (char*)&want_base_dir);
    off += sizeof(bool);
    want._unrope(r.substr(off, r.length()-off));
  }
  virtual void encode_payload(crope& r) {
	r.append((char*)&asker, sizeof(asker));
    r.append((char*)&base_ino, sizeof(base_ino));
    r.append((char*)&want_base_dir, sizeof(want_base_dir));
    r.append(want._rope());
  }

};

#endif
