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
  inodeno_t ino;

  string ref_name;
  ClNode *parent;  

  hash_map<string, ClNode*> children;
  bit_vector dist;


  ClNode() {
	parent = 0;
  }
  
  void get() {
	lru_expireable = false;
  }
  void put() {
	lru_expireable = true;
  }

  void detach() {
	if (parent) {
	  parent->unlink(ref_name);
	}
  }

  void full_path(string& p) {
	if (parent)
	  parent->full_path(p);
	if (p.length()) {
	  p.append("/");
	}
	p.append(ref_name);
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
  }

  ClNode *lookup(string name) {
	hash_map<string, ClNode*>::iterator it = children.find(name);
	if (it == children.end()) return NULL;
	return it->second;
  }

  // ...

};

#endif
