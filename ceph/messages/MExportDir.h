#ifndef __MEXPORTDIR_H
#define __MEXPORTDIR_H

#include "include/Message.h"

#include <ext/rope>
using namespace std;


class MExportDir : public Message {
  inodeno_t ino;
  double ipop;
  
  int    ndirs;
  crope  state;
  
  list<inodeno_t> exports;

  // hashed pre-discovers
  //map<inodeno_t, set<string> > hashed_prediscover;

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
  list<inodeno_t>& get_exports() { return exports; }
  
  void add_dir(crope& dir) {
	state.append( dir );
	ndirs++;
  }
  void add_export(CDir *dir) { exports.push_back(dir->ino()); }

  // prediscover crap
  /*
    void add_prediscover(inodeno_t dirino, const string& dentry) {
	hashed_prediscover[dirino].insert(dentry);
  }
  void remove_prediscover(inodeno_t dirino, const string& dentry) {
	assert(hashed_prediscover.count(dirino));
	hashed_prediscover[dirino].erase(dentry);
	if (hashed_prediscover[dirino].empty())
	  hashed_prediscover.erase(dirino);
  }
  bool any_prediscovers() {
	return !hashed_prediscover.empty();
  }
  
  // this should really be a custom iterator if we want
  // clean encapsulation
  map<inodeno_t, set<string> >::iterator prediscover_begin() {
	return hashed_prediscover.begin();
  }
  map<inodeno_t, set<string> >::iterator prediscover_end() {
	return hashed_prediscover.end();
  }
  */

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
	s.copy(sizeof(ino), sizeof(ipop), (char*)&ipop);
	s.copy(sizeof(ino)+sizeof(ipop), sizeof(ndirs), (char*)&ndirs);
	int off = sizeof(ino)+sizeof(ipop)+sizeof(ndirs);

    // exports
    int nex;
    s.copy(off, sizeof(nex), (char*)&nex);
    off += sizeof(nex);
	for (int i=0; i<ndirs; i++) {
	  inodeno_t dirino;
	  s.copy(off, sizeof(dirino), (char*)&dirino);
	  off += sizeof(dirino);
      exports.push_back(dirino);
	}

	// prediscover
    /*
	int ndirs;
	s.copy(off, sizeof(ndirs), (char*)&ndirs);
	off += sizeof(ndirs);
	for (int i=0; i<ndirs; i++) {
	  inodeno_t dirino;
	  int nden;
	  s.copy(off, sizeof(dirino), (char*)&dirino);
	  off += sizeof(dirino);
	  s.copy(off, sizeof(nden), (char*)&nden);
	  off += sizeof(nden);
	  for (int j=0; j<nden; j++) {
		string dn = s.substr(off, s.length()-off).c_str();
		off += dn.length() + 1;
		hashed_prediscover[dirino].insert(dn);
	  }
	}
    */

	// dir data
	state = s.substr(off, s.length() - off);
	return 0;
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&ipop, sizeof(ipop));
	s.append((char*)&ndirs, sizeof(ndirs));

    // exports
    int nex = exports.size();
    s.append((char*)&nex, sizeof(int));
    
	// prediscover
    /*
	int ndirs = hashed_prediscover.size();
	s.append((char*)&ndirs, sizeof(ndirs));
	for (map<inodeno_t, set<string> >::iterator it = hashed_prediscover.begin();
		 it != hashed_prediscover.end();
		 it++) {
	  int nden = it->second.size();
	  s.append((char*)&nden, sizeof(nden));
	  for (set<string>::iterator dit = it->second.begin();
		   dit != it->second.end();
		   dit++) {
		s.append((*dit).c_str());
		s.append((char)0);
	  }
	}
    */
	
	// dir data
	s.append(state);
	return s;
  }

};

#endif
