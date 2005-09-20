#ifndef __crush_BUCKET_H
#define __crush_BUCKET_H

#include "BinaryTree.h"
#include "Hash.h"

#include <vector>
#include <map>
using namespace std;

namespace crush {

  /** abstract bucket **/

  class Bucket {
  protected:
	int         id;
	int         type;
	float       weight;

  public:
	Bucket(int _id,
		   int _type,
		   float _weight) :
	  id(_id), 
	  type(_type),
	  weight(_weight) { }
	
	int          get_id() const { return id; } 
	int          get_type() const { return type; }
	float        get_weight() const { return weight; }
	virtual int  get_size() const = 0;

	void         set_weight(float w)  { weight = w; }

	virtual bool is_uniform() const = 0;
	virtual int choose_r(int x, int r, Hash& h) const = 0;

  };


  /** uniform bucket **/
  class UniformBucket : public Bucket {	
  protected:
  public:
	vector<int> items;
	int    item_type;
	float  item_weight;

	vector<int> primes;

  public:
	UniformBucket(int _id, int _type, int _item_type,
				  float _item_weight, vector<int>& _items) :
	  Bucket(_id, _type, _item_weight*_items.size()),
	  item_type(_item_type),
	  item_weight(_item_weight) {
	  items = _items;
	}

	int get_size() const { return items.size(); }
	int get_item_type() const { return item_type; }
	float get_item_weight() const { return item_weight; }
	bool is_uniform() const { return true; }

	int get_prime(int j) const {
	  return primes[ j % primes.size() ];
	}

	void make_primes(Hash& h) {
	  // start with odd number > num_items
	  int x = items.size() + 1;             // this is the minimum!
	  x += h(get_id()) % (3*items.size());  // bump it up some
	  x |= 1;                               // make it odd

	  while (primes.size() < items.size()) {
		int j;
		for (j=2; j*j<=x; j++) 
		  if (x % j == 0) break;
		if (j*j > x) {
		  primes.push_back(x);
		}
		x += 2;
	  }
	}

	int choose_r(int x, int r, Hash& h) const {
	  //cout << "uniformbucket.choose_r(" << x << ", " << r << ")" << endl;
	  //if (r >= get_size()) cout << "warning: r " << r << " >= " << get_size() << " uniformbucket.size" << endl;
	  
	  int v = h(x, get_id(), 1) % get_size();
	  int p = get_prime( h(x, get_id(), 2) );  // choose a prime based on hash(x, get_id(), 2)
	  int s = (x + v + (r+1)*p) % get_size();
	  return items[s];
	}
  };



  // mixed bucket, based on RUSH_T type binary tree
  
  class MixedBucket : public Bucket {
  protected:
	vector<float>  item_weight;

  public:
	BinaryTree     tree;
	map<int,int>   node_map;     // node id -> item

  public:
	MixedBucket(int _id, int _type) : Bucket(_id, _type, 0) {
	}

	//float       get_item_weight(int i) { return item_weight[i]; }
	bool        is_uniform() const { return false; }
	int get_size() const { return node_map.size(); }

	void add_item(int item, float w) {
	  int n = tree.add_node(w);
	  node_map[n] = item;
	  weight += w;
	}

	int choose_r(int x, int r, Hash& h) const {
	  //cout << "mixedbucket.choose_r(" << x << ", " << r << ")" << endl;
	  int n = tree.root();
	  while (!tree.terminal(n)) {
		// pick a point in [0,w)
		float w = tree.weight(n);
		float f = (float)(h(x, n, r, get_id()) % 1000) * w / 1000.0;

		// left or right?
		int l = tree.left(n);
		if (tree.exists(l) && 
			f < tree.weight(l))
		  n = l;
		else
		  n = tree.right(n);
	  }
	  assert(node_map.count(n));
	  return ((map<int,int>)node_map)[n];
	}


  };






}








#endif
