#ifndef __CLNODE_H
#define __CLNODE_H

#include <string>
#include <ext/hash_map>
using namespace std;

#include "../include/types.h"
#include "../include/lru.h"

class ClNode : public LRUObject {
 protected:
 public:

  // if didstat, the following are defined
  inodeno_t ino;
  bool isdir;
  bool havedircontents;
  
  string ref_name;
  ClNode *parent;  

  hash_map<string, ClNode*> children;
  vector<int> dist;

  bool dangling;

  ClNode() {
	parent = 0;
	refs = 0;
	isdir = havedircontents = false;
	dangling = false;
  }
  
  int depth() {
	if (parent == 0)
	  return 1;
	else
	  return 1 + parent->depth();
  }
  
  int refs;  // reference count

  void get() {
	refs++;
	lru_expireable = false;
  }
  void put() {
	refs--;
	assert(refs >= 0);
	if (!refs) lru_expireable = true;
  }

  void detach() {
	if (parent) {
	  parent->unlink(ref_name);
	  parent = NULL;
	}
  }

  void full_path(string& p, vector<string>& vec) {
	if (parent)
	  parent->full_path(p, vec);
	if (p.length()) {
	  p.append("/");
	} 
	p.append(ref_name);
	vec.push_back(ref_name);
  }

  void link(string name, ClNode* node) {
	if (children.size() == 0) 
	  get();
	children[ name ] = node;
	node->parent = this;
	node->ref_name = name;
  }
  void unlink(string name) {
	children.erase(name);
	if (children.size() == 0)
	  put();
	havedircontents = false;
  }

  ClNode *lookup(string name) {
	hash_map<string, ClNode*>::iterator it = children.find(name);
	if (it == children.end()) return NULL;
	return it->second;
  }

  // ...

};

#endif
