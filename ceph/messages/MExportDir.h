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

  bool           dirty;       // dirty inode?

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
  inodeno_t ino;
  double ipop;
  int    ndirs;
  crope  state;
  
 public:  
  MExportDir() {}
  MExportDir(CInode *in, double pop) : 
	Message(MSG_MDS_EXPORTDIR) {
	this->ino = in->inode.ino;
	ipop = pop;
	ndirs = 0;
  }
  virtual char *get_type_name() { return "Ex"; }

  inodeno_t get_ino() { return ino; }
  double get_ipop() { return ipop; }
  int get_ndirs() { return ndirs; }
  crope& get_state() { return state; }

  void add_dir(crope& dir) {
	state.append( dir );
	ndirs++;
  }

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
	s.copy(sizeof(ino), sizeof(ipop), (char*)&ipop);
	s.copy(sizeof(ino)+sizeof(ipop), sizeof(ndirs), (char*)&ndirs);
	int off = sizeof(ino)+sizeof(ipop)+sizeof(ndirs);
	state = s.substr(off, s.length() - off);
	return 0;
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&ipop, sizeof(ipop));
	s.append((char*)&ndirs, sizeof(ndirs));
	s.append(state);
	return s;
  }

};

#endif
