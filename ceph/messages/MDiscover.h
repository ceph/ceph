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

  void add_bit(MDiscoverRec_t b) {
	trace.push_back(b);
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
