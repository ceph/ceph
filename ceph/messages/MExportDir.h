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
  //int            ref;         // hmm, fyi for debugging?
  int            dir_auth;

  int            ncached_by;  // ints follow
} Inode_Export_State_t;

typedef struct {
  inodeno_t      ino;
  __uint64_t     nitems;
  __uint64_t     version;
  int            dir_auth;
  int            dir_rep;
  unsigned       state;
  DecayCounter   popularity;
  int            ndir_rep_by;
  // ints follow
} Dir_Export_State_t;
  

#define BUFLEN   100000  // 100k
#define BUFSLOP   10000  // 10k before we make a new buf!


class MExportDir : public Message {
 public:
  string path;
  inodeno_t ino;
  double ipop;

  int    ndirs;
  crope  state;
  

  // ...?

  MExportDir(CInode *in, double pop) : 
	Message(MSG_MDS_EXPORTDIR) {
	this->ino = in->inode.ino;
	in->make_path(path);
	ipop = pop;
	ndirs = 0;
  }
  virtual char *get_type_name() { return "exp"; }

  void add_dir(crope& dir) {
	state.append( dir );
	ndirs++;
  }

};

#endif
