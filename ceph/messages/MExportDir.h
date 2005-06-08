#ifndef __MEXPORTDIR_H
#define __MEXPORTDIR_H

#include "msg/Message.h"

#include <ext/rope>
using namespace std;


class MExportDir : public Message {
  inodeno_t ino;
  
  int    ndirs;
  crope  state;
  
  list<inodeno_t> exports;

  // hashed pre-discovers
  //map<inodeno_t, set<string> > hashed_prediscover;

 public:  
  MExportDir() {}
  MExportDir(CInode *in) : 
	Message(MSG_MDS_EXPORTDIR) {
	this->ino = in->inode.ino;
	ndirs = 0;
  }
  virtual char *get_type_name() { return "Ex"; }

  inodeno_t get_ino() { return ino; }
  int get_ndirs() { return ndirs; }
  crope& get_state() { return state; }
  list<inodeno_t>& get_exports() { return exports; }
  
  void add_dir(crope& dir) {
	state.append( dir );
	ndirs++;
  }
  void add_export(CDir *dir) { exports.push_back(dir->ino()); }


  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	s.copy(off, sizeof(ndirs), (char*)&ndirs);
	off += sizeof(ndirs);

    // exports
    int nex;
    s.copy(off, sizeof(nex), (char*)&nex);
    off += sizeof(int);
	dout(12) << nex << " nested exports out" << endl;
	for (int i=0; i<nex; i++) {
	  inodeno_t dirino;
	  s.copy(off, sizeof(dirino), (char*)&dirino);
	  off += sizeof(dirino);
      exports.push_back(dirino);
	}

	// dir data
	size_t len;
	s.copy(off, sizeof(len), (char*)&len);
	off += sizeof(len);
	state = s.substr(off, len);
	off += len;
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&ndirs, sizeof(ndirs));

    // exports
    int nex = exports.size();
	dout(12) << nex << " nested exports in" << endl;
    s.append((char*)&nex, sizeof(int));
	for (list<inodeno_t>::iterator it = exports.begin();
		 it != exports.end();
		 it++) {
	  inodeno_t ino = *it;
	  s.append((char*)&ino, sizeof(ino));
	}
	
	// dir data
	size_t len = state.length();
	s.append((char*)&len, sizeof(len));
	s.append(state);
  }

};

#endif
