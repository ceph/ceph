#ifndef __MEXPORTDIR_H
#define __MEXPORTDIR_H

#include "include/Message.h"

#include <ext/rope>

#include <vector>
using namespace std;

//typedef struct {
//} MExportDirRec_t;

typedef struct {
  inode_t        inode;
  __uint64_t     version;
  DecayCounter   popularity;
  int            ref;  // fyi for debugging?

  int            ncached_by;  // ints follow
} Inode_Export_State_t;

typedef struct {
  __uint64_t     nitems;
  __uint64_t     version;
  int            dir_auth;
  int            dir_rep;
  unsigned       state;
  int            ndir_rep_by;
  // ints follow
} Dir_Export_State_t;
  

#define BUFLEN   100000  // 100k
#define BUFSLOP   10000  // 10k before we make a new buf!


class MExportDir : public Message {
 public:
  string path;
  inodeno_t ino;

  crope  rope;

  char   *buf;
  int    bufend; 
  

  // ...?

  MExportDir(CInode *in) : 
	Message(MSG_MDS_EXPORTDIR) {
	this->ino = in->inode.ino;
	in->make_path(path);
	rope = new crope;
	buf = 0;
	bufend = 0;
  }
  virtual char *get_type_name() { return "exp"; }

  char *get_end_ptr(int want = BUFSLOP) {
	if (bufend >= BUFLEN - want) {
	  rope->append(buf, bufend);
	  buf = 0;
	}
	if (!buf) {
	  buf = new char[BUFLEN];
	  bufend = 0;
	}
	return buf + bufend;
  }
  void set_end_ptr(char *p) {
	bufend = p - buf;
  }
  
  crope *get_rope() {
	if (bufend && buf) {
	  crope->append(buf, bufend);
	  buf = 0;
	}
  }

};

#endif
