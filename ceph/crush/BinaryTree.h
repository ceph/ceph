#ifndef __crush_BINARYTREE_H
#define __crush_BINARYTREE_H

#include <cassert>
#include <iostream>
//#include <map>
#include <vector>
//#include <set>
using namespace std;

namespace crush {

  class BinaryTree {
  private:
	// tree def
	int             root_node;       // 0 for empty tree.
	int             alloc;
	vector<int>     node_nested;     // all existing nodes in this map
	vector<float>   node_weight;     // and this one
	vector<int>     node_complete;   // only nodes with all possible children
	
  public:
	BinaryTree() : root_node(0), alloc(0) {}
	
	// accessors
	bool  empty() const { return root_node == 0; }
	bool  exists(int n) const { return n < alloc && node_nested[n]; }
	int   nested(int n) const { return exists(n) ? node_nested[n]:0; }
	float weight(int n) const { return exists(n) ? node_weight[n]:0; }
	bool  complete(int n) const { return exists(n) ? node_complete[n]:false; }
	int   root() const { return root_node; }
	
	int   realloc(int n) {
	  while (alloc <= n) {
		node_nested.push_back(0);
		node_weight.push_back(0);
		node_complete.push_back(0);
		alloc++;
	  }
	}

	// tree navigation
	bool terminal(int n) const { return n & 1; }  // odd nodes are leaves.
	int height(int n) const {
	  assert(n);
	  int h = 0;
	  while ((n & 1) == 0) {
		assert(n > 0);
		h++; n = n >> 1;
	  }
	  return h;
	}
	int left(int n) const { 
	  int h = height(n);
	  //cout << "left of " << n << " is " << (n - (1 << h)) << endl;
	  return n - (1 << (h-1));
	}
	int right(int n) const {
	  int h = height(n);
	  //cout << "right of " << n << " is " << (n + (1 << h)) << endl;
	  return n + (1 << (h-1));
	}
	bool on_right(int n, int h = -1) const { 
	  if (h < 0) h = height(n);
	  return n & (1 << (h+1)); 
	}
	bool on_left(int n) const { return !on_right(n); }
	int parent(int n) const {
	  int h = height(n);
	  if (on_right(n, h))
		return n - (1<<h);
	  else
		return n + (1<<h);
	}
	
	// modifiers
	void remove_node(int n) {
	  assert(exists(n));
	  
	  // erase node
	  node_nested[n] = 0;
	  node_weight[n] = 0;

	  // adjust parents (!complete, -weight)
	  int p = n;
	  while (p != root_node) {
		p = parent(p);

		node_complete[p] = 0;
		node_weight[p] = weight(left(p)) + weight(right(p));
		node_nested[p]--;

		if (nested(p) == 0) {
		  node_weight[p] = 0;
		  node_nested[p] = 0;
		}
	  }
	  
	  // hose root?
	  while (!terminal(root_node) &&
			 (nested(left(root_node)) == 0 ||
			 nested(right(root_node)) == 0)) {
		// root now one child..
		node_weight[root_node] = 0;
		node_nested[root_node] = 0;
		if (nested(left(root_node)) == 0)
		  root_node = right(root_node);
		else 
		  root_node = left(root_node);
	  }

	  if (terminal(root_node) && 
		  nested(root_node) == 0) {
		// empty!
		node_weight[root_node] = 0;
		node_nested[root_node] = 0;
		root_node = 0;
	  }

	}
	
	int add_node(float w) {
	  int n;
	  if (!root_node) {
		// empty tree!
		root_node = n = 1;
	  } else {
		// existing tree.
		// expand tree?
		if (complete(root_node)) {
		  // add new root
		  int newroot = parent(root_node);
		  realloc(newroot);
		  node_weight[newroot] = node_weight[root_node];
		  node_nested[newroot] = nested(root_node);

		  // go right or left?
		  if (left(newroot) == root_node)
			n = right(newroot);
		  else
			n = left(newroot);
		  root_node = newroot;

		  // then go left until terminal
		  while (!terminal(n))
			n = left(n);
		}
		else {
		  // tree isn't complete.
		  n = root_node;
		  while (!terminal(n)) {
			if (!exists(left(n)) || !complete(left(n))) {
			  // left isn't complete
			  n = left(n);
			} else {
			  assert(!exists(right(n)) || !complete(right(n)));
			  // right isn't complete
			  n = right(n);
			}
		  }
		}
	  }
	  
	  // create at n
	  //cout << "creating " << n << endl;
	  realloc(n);
	  node_weight[n] = w;
	  node_nested[n] = 1;
	  node_complete[n] = 1;

	  // ancestors: create, adjust weight, complete as appropriate
	  int p = n;
	  while (p != root_node) {
		p = parent(p);
		realloc(p);

		// complete?
		if (!complete(p) &&
			complete(left(p)) && 
			complete(right(p))) 
		  node_complete[p] = 1;
		
		// weight (and implicitly create)
		node_weight[p] += w;
		node_nested[p]++;
	  }

	  return n;
	}
	

  };


  // print it out
  void print_node(ostream& out, const BinaryTree& tree, int n, int i) {
	for (int t=i; t>0; t--) out << "  ";
	if (tree.root() == n)
	  out << "root  ";
	else {
	  if (tree.on_left(n))
		out << "left  ";
	  else
		out << "right ";
	}
	out << n << " : nested " << tree.nested(n) << "   weight " << tree.weight(n);
	if (tree.complete(n)) out << "  complete";
	out << endl;
	if (!tree.terminal(n)) {
	  if (tree.exists(tree.left(n)))
		print_node(out, tree, tree.left(n), i+2);
	  if (tree.exists(tree.right(n)))
		print_node(out, tree, tree.right(n), i+2);
	}
  }
  
  ostream& operator<<(ostream& out, const BinaryTree& tree) {
	if (tree.empty()) 
	  return out << "tree is empty";
	print_node(out, tree, tree.root(), 0);	
	return out;
  }
  
}

#endif
