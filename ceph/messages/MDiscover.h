#ifndef __MDISCOVER_H
#define __MDISCOVER_H

#include "include/Message.h"
#include "mds/CDir.h"

#include <vector>
#include <string>
using namespace std;


struct MDiscoverRec_t {
  inode_t    inode;
  set<int>   cached_by;
  int        dir_auth;

  // dir stuff
  int        dir_rep;
  set<int>   dir_rep_by;

  crope _rope() {
	crope r;
	r.append((char*)&inode, sizeof(inode));

	int n = cached_by.size();
	r.append((char*)&n, sizeof(int));
	for (set<int>::iterator it = cached_by.begin(); 
		 it != cached_by.end();
		 it++) {
	  int j = *it;
	  r.append((char*)&j, sizeof(j));
	}

	r.append((char*)&dir_auth, sizeof(int));
	r.append((char*)&dir_rep, sizeof(int));
	
	n = dir_rep_by.size();
	r.append((char*)&n, sizeof(int));
	for (set<int>::iterator it = dir_rep_by.begin(); 
		 it != dir_rep_by.end();
		 it++) {
	  int j = *it;
	  r.append((char*)&j, sizeof(j));
	}
	
	return r;
  }

  int _unrope(crope s) {
	s.copy(0,sizeof(inode_t), (char*)&inode);
	int off = sizeof(inode_t);
	
	int n;
	s.copy(off, sizeof(int), (char*)&n);
	off += sizeof(int);
	for (int i=0; i<n; i++) {
	  int j;
	  s.copy(off, sizeof(int), (char*)&j);
	  cached_by.insert(j);
	  off += sizeof(int);
	}

	s.copy(off, sizeof(int), (char*)&dir_auth);
	off += sizeof(int);
	s.copy(off, sizeof(int), (char*)&dir_rep);
	off += sizeof(int);

	s.copy(off, sizeof(int), (char*)&n);
	off += sizeof(int);
	for (int i=0; i<n; i++) {
	  int j;
	  s.copy(off, sizeof(int), (char*)&j);
	  dir_rep_by.insert(j);
	  off += sizeof(int);
	}
	return off;
  }  
} ;


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
	  off += dr._unrope(r.substr(off, r.length()));
	  trace.push_back(dr);
	}
  }
  virtual crope get_payload() {
	crope r;
	r.append((char*)&asker, sizeof(asker));
	r.append(basepath.c_str());
	r.append((char)0);

	if (!want) want = new vector<string>;
	int num_want = want->size();
	r.append((char*)&num_want,sizeof(int));
	for (int i=0; i<num_want; i++) {
	  r.append((*want)[i].c_str());
	  r.append((char)0);
	}
	
	int ntrace = trace.size();
	r.append((char*)&ntrace, sizeof(int));
	for (int i=0; i<ntrace; i++)
	  r.append(trace[i]._rope());
	
	return r;
  }

  
  // ---

  void add_bit(CInode *in, int auth) {
	MDiscoverRec_t bit;

	bit.inode = in->inode;
	bit.cached_by = in->get_cached_by();
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
	if (just_root())
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
