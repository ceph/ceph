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
 public:
  int asker;

  string          basepath; // /have/this/
  vector<string> *want;     //            but/not/this 

  vector<MDiscoverRec_t> trace; 
  
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

  string next_dentry() {
	return (*want)[trace.size()];
  }

  bool want_root() {
	if (want == NULL) return true;
	return false;
  }
  
  bool done() {
	// just root?
	if (want == NULL) {
	  if (trace.size() < 1) return false; 
	  return true;
	}

	// normal
	if (trace.size() == want->size()) return true;
	return false;
  }
};

#endif
