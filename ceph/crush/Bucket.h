#ifndef __crush_BUCKET_H
#define __crush_BUCKET_H

namespace crush {


  class Bucket {
  protected:
	int         id;
	int         type;
	float       weight;
	vector<int> items;

  public:
	Bucket(int _id) : id(_id), weight(0) { }

	int         get_id() { return id; }
	int         get_type() { return type; }
	float       get_weight() { return weight; }
	vector<int> get_items() { return items; }
	int         get_size() { return items.size(); }

	float       get_item_weight() = 0;
	bool        is_uniform() = 0;
	float       calc_weight() = 0;

	
	void choose_r(int r, int type, vector<int>& in, vector<int>& out) = 0;

  };


 
  class UniformBucket : public Bucket {	void choose_r(int r, int type, vector<int>& in, vector<int>& out) = 0;
  protected:
	float  item_weight;
  public:
	float       get_item_weight(int i) { return item_weight; }
	bool        is_uniform() { return true; }

	float calc_weight() {
	  if (item.empty())
		weight = 0;
	  else 
		weight = items.size() * get_item_weight(item[0]);
	  return weight;
	}

	int choose_r(pg_t x, int r, Hash& h) {
	  assert(type == get_type());
	  if (r >= get_size()) cour << "warning: r " << r << " >= " << get_size() << " uniformbucket.size" << endl;
	  
	  int v = h(x, r, get_id(), 1) * get_size();
	  int pmin = get_weight();
	  int p = 0; // choose a prime based on hash(x, r, get_id(), 2)
	  //int s = x + v + r *p(? % get_size())
	  //return get_item[s % get_size()];
	}
  };



  // mixed bucket.. RUSH_T
  

  class MixedBucket : public Bucket {
  protected:
	vector<float>  item_weight;

	BinaryTree     tree;
	map<int,int>   node_map;     // node id -> item

  public:
	float       get_item_weight(int i) {
	  return item_weight[i];
	}
	bool        is_uniform() { return false; }

	float calc_weight() {
	  weight = 0;
	  for (unsigned i=0; i<items.size(); i++) {
		weight += get_item_weight(i);
	  }
	  return weight;
	}


	void make_new_tree(vector<int>& items) {
	  assert(tree.empty());
	  
	  for (unsigned i=0; i<items.size(); i++) {
		int n = tree.add_node(item_weight[i]);
		node_map[n] = items[i];
	  }
	}

	int choose_r(pg_t x, int r, Hash& h) {
	  int n = tree.root();
	  while (!tree.terminal(n)) {
		float f = h(x, n, r, 0);
		if (tree.weight(tree.left(n)) <= f)
		  node = tree.left(n);
		else
		  node = tree.right(n);
	  }
	
	  return node_map[n];
	}


  };






}








#endif
