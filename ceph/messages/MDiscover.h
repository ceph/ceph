#ifndef __MDISCOVER_H
#define __MDISCOVER_H

#include "include/Message.h"

#include <vector>
#include <string>
using namespace std;


typedef struct {
  inode_t    inode;
  set<int>   cached_by;

  // dir stuff
  int        dir_auth;
  int        dir_rep;
  set<int>   dir_rep_by;
} MDiscoverRec_t;


class MDiscover : public Message {
  int asker;
  string          basepath; // /have/this/
  vector<string> *want;     //            but/not/this 
  vector<MDiscoverRec_t> trace;   // results

 public:
  int get_asker() { return asker; }
  string& get_basepath() { return basepath; }
  vector<string> *get_want() { return want; }
  vector<MDiscoverRec_t>& get_trace() { return trace; }

  MDiscover() {
	want = 0;
  }
  MDiscover(int asker, 
			string basepath,
			vector<string> *want) :
	Message(MSG_MDS_DISCOVER) {
	this->asker = asker;
	this->basepath = basepath;
	this->want = want;
  }
  ~MDiscover() {
	if (want) { delete want; want = 0; }
  }
  virtual char *get_type_name() { return "disc"; }

  virtual int decode_payload(crope r) {
	r.copy(0, sizeof(asker), (char*)&asker);
	basepath = r.c_str() + sizeof(asker);
	unsigned off = sizeof(asker) + basepath.length() + 1;
	
	want = new vector<string>;
	int num_want;
	r.copy(off, sizeof(int), (char*)&num_want);
	off += sizeof(int);
	for (int i=0; i<num_want; i++) {
	  string w = r.c_str() + off;
	  off += w.length() + 1;
	  want->push_back(w);
	}
	
	int ntrace;
	r.copy(off, sizeof(int), (char*)&ntrace);
	off += sizeof(int);
	for (int i=0; i<ntrace; i++) {
	  MDiscoverRec_t dr;
	  r.copy(off, sizeof(dr), (char*)&dr);
	  off += sizeof(dr);
	}
  }
  virtual crope get_payload() {
	crope r;
	r.append((char*)&asker, sizeof(asker));
	r.append(basepath.c_str());

	if (!want) want = new vector<string>;
	int num_want = want->size();
	r.append((char*)&num_want,sizeof(int));
	for (int i=0; i<num_want; i++)
	  r.append((*want)[i].c_str());
	
	int ntrace = trace.size();
	r.append((char*)&ntrace, sizeof(int));
	for (int i=0; i<ntrace; i++)
	  r.append((char*)&trace[i], sizeof(MDiscoverRec_t));
	
	return r;
  }

  
  // ---

  void add_bit(CInode *in, int auth) {
	MDiscoverRec_t bit;

	bit.inode = in->inode;
	bit.cached_by = in->cached_by;
	bit.cached_by.insert( auth );  // obviously the authority has it too
	bit.dir_auth = in->dir_auth;
	if (in->is_dir() && in->dir) {
	  bit.dir_rep = in->dir->dir_rep;
	  bit.dir_rep_by = in->dir->dir_rep_by;
	}

	trace.push_back(bit);
  }

  string current_base() {
	string c = basepath;
	for (int i=0; i<trace.size(); i++) {
	  c += "/";
	  c += (*want)[i];
	}
	return c;
  }

  string current_need() {
	if (want == NULL)
	  return string("");  // just root

	string a = current_base();
	a += "/";
	a += next_dentry();
	return a;
  }

  string& next_dentry() {
	return (*want)[trace.size()];
  }

  bool just_root() {
	if (want == NULL ||
		want->size() == 0) return true;
	return false;
  }
  
  bool done() {
	// just root?
	if (just_root() &&
		trace.size() > 0) return true;

	// normal
	if (trace.size() == want->size()) return true;
	return false;
  }
};

#endif
