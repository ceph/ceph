// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/unordered_map.h"
/*
 * ReuseDist implements the hole-based MRC construction algorithm from the
 * paper "Efficient MRC Construction with SHARDS".
 * It utilizes Red-Black Tree where the value of a node is an integer 
 * interval, and the intervals of any two nodes don't overlap. We just add 
 * a new operation dist(int i), which inserts a new interval [i, i] into 
 * the tree, merges adjacent intervals, and returns the number of integers 
 * larger than i in the tree.
 */
enum COLOR {red,black};

class Interval
{
  public:
    int left, right;
    Interval() {
      left = right = 0;
    }
    Interval(int k) {
      left = right = k;
    }
    bool operator> (const Interval& Int) const {
      if (left > Int.right) {
	return true;
      } else {
	return false;
      }
    }
    bool operator< (const Interval& Int) const {
      if (right < Int.left) {
	return true;
      } else {
	return false;
      }
    }
};

template <typename T> class RBTree;

template <typename T>
class node
{
  private:
    friend class RBTree<T>;
    T key;
    COLOR color;
    node *parent;
    node *left;
    node *right;
    int sum;
    node() {}
  public:
    node(const T &k,COLOR c = red)
      : key(k), color(c), parent(NULL),
      left(NULL), right(NULL), sum(0) {}

};


template <typename T>
class RBTree
{
  public:
    node<T> *nil;
    node<T> *root;
    RBTree(const RBTree&);
    RBTree operator=(const RBTree&);
    void leftRotate(node<T>*);
    void rightRotate(node<T>*);
    void insertFixup(node<T>*);
    void eraseFixup(node<T>*);
    RBTree() {
      nil = new node<T>;
      root = nil;
      root->parent = nil;
      root->left = nil;
      root->right = nil;
      root->color = black;
    }
    RBTree(node<T> *rbt) : root(rbt) {
      nil = new node<T>;
      root->parent = nil;
      root->left = nil;
      root->right = nil;
      root->color = black;
    }
    void create();
    void erase(node<T>*);
    node<T>* minMum() const;
    node<T>* maxMum() const;
    node<T>* successor(node<T>*) const;
    node<T>* predecessor(node<T>*) const;
    void traverse(node<T>*, int);
    void destroy();
    void clear();
    bool empty() const { return root == nil; }
    int dist(int);
    int dist(node<T>*, int);
};


template <typename T>
void RBTree<T>::leftRotate(node<T> *curr)
{
  if (curr->right != nil) {
    node<T> *rchild = curr->right;
    curr->right = rchild->left;
    if (rchild->left != nil) {
      rchild->left->parent = curr;
    }
    rchild->parent = curr->parent;
    if (curr->parent == nil) {
      root = rchild;
    } else if (curr == curr->parent->left) {
      curr->parent->left = rchild;
    } else {
      curr->parent->right = rchild;
    }
    curr->parent = rchild;
    rchild->left = curr;
    curr->sum = curr->sum - (rchild->key.right - rchild->key.left + 1) - rchild->sum;
  }
}

template <typename T>
void RBTree<T>::rightRotate(node<T> *curr)
{
  if (curr->left != nil) {
    node<T> *lchild = curr->left;
    curr->left = lchild->right;
    if (lchild->right != nil) {
      lchild->right->parent = curr;
    }
    lchild->parent = curr->parent;
    if (curr->parent == nil) {
      root = lchild;
    } else if (curr == curr->parent->left) {
      curr->parent->left = lchild;
    } else {
      curr->parent->right = lchild;
    }
    lchild->right = curr;
    curr->parent = lchild;
    lchild->sum = lchild->sum + (curr->key.right - curr->key.left + 1) + curr->sum;
  }
}

template <typename T>
int RBTree<T>:: dist(int p) {
  return dist(root, p);
}

template <typename T>
int RBTree<T>:: dist(node<T>* n, int p) {
  if (root == nil && n == root) {
    //the tree is empty, create a new tree
    root = new node<T>(p);
    root->left = root->right = root->parent = nil;
    root->color = black;
    return 0;
  } else if (p < n->key.left - 1) {
    if (n->left != nil) {
      //insert p in the left subtree of n
      return n->key.right - n->key.left + 1 + n->sum + dist(n->left, p);
    } else {
      //insert p as the left child of n
      n->left = new node<T>(p);
      n->left->left = n->left->right = nil;
      n->left->parent = n;
      int r = n->sum + n->key.right - n->key.left + 1;
      insertFixup(n->left);
      return r;
    }
  } else if (p > n->key.right + 1) {
    if (n->right != nil) {
      //insert p in the right subtree of n
      n->sum += 1;
      return dist(n->right, p);
    } else {
      //insert p as the right child of n
      n->right = new node<T>(p);
      n->right->left = n->right->right = nil;
      n->right->parent = n;
      n->sum += 1;
      insertFixup(n->right);
      return 0;
    }
  } else if (p == n->key.left - 1) {
    //p is next to n on the left

    node<T> *pre = n->left;
    int r = 0;
    if (pre != nil && p == pre->key.right + 1) {
      //p connects pre and n, merge them all
      n->key.left = pre->key.left;
      r = n->key.right - p + n->sum;
      erase(pre);
    } else {
      //just merge p into n
      n->key.left = p;
      r = n->key.right - p + n->sum;
    }
    return r;
  } else if (p == n->key.right + 1) {
    //p is next to n on the right

    node<T> *suc = n->right;
    if (suc != nil && p == suc->key.left - 1) {
      //p connects n and suc, merge them all
      int r = n->sum;
      n->key.right = suc->key.right;
      n->sum = n->sum - (suc->key.right - suc->key.left + 1);
      erase(suc);
      return r;
    } else {
      //just merge p into n
      n->key.right = p;
      return n->sum;
    }
  } else {
    printf("ERROR");
    return -1;
  }
}


template <typename T>
void RBTree<T>::insertFixup(node<T> *curr)
{
  while (curr->parent->color == red) {
    if (curr->parent == curr->parent->parent->left) {
      node<T> *uncle = curr->parent->parent->right;
      if (uncle != nil && uncle->color == red) {
	curr->parent->color = black;
	uncle->color = black;
	curr->parent->parent->color = red;
	curr = curr->parent->parent;
      } else if (curr == curr->parent->right) {
	curr = curr->parent;
	leftRotate(curr);
      } else {
	curr->parent->color = black;
	curr->parent->parent->color = red;
	rightRotate(curr->parent->parent);
      }
    } else {
      node<T> *uncle = curr->parent->parent->left;
      if (uncle != nil && uncle->color == red) {
	curr->parent->color = black;
	uncle->color = black;
	curr->parent->parent->color = red;
	curr = curr->parent->parent;
      } else if (curr == curr->parent->left) {
	curr = curr->parent;
	rightRotate(curr);
      } else {
	curr->parent->color = black;
	curr->parent->parent->color = red;
	leftRotate(curr->parent->parent);
      }
    }
  }
  root->color = black;
}


template <typename T>
node<T>* RBTree<T>::successor(node<T>* curr) const
{
  if (curr->right != nil) {
    RBTree RIGHT(curr->right);
    return RIGHT.minMum();
  }
  node<T> *p = curr->parent;
  while (p != nil && curr == p->right) {
    curr = p;
    p = p->parent;
  }
  return p;
}

template <typename T>
node<T>* RBTree<T>::minMum() const
{
  node<T> *curr = root;
  while (curr->left != nil) {
    curr = curr->left;
  }
  return curr;
}

template <typename T>
node<T>* RBTree<T>::maxMum() const
{
  node<T> *curr = root;
  while (curr->right != nil)
    curr = curr->right;
  return curr;
}

template <typename T>
node<T>* RBTree<T>::predecessor(node<T>* curr) const
{
  if (curr->left != nil) {
    RBTree LEFT(curr->left);
    return LEFT.maxMum();
  }
  node<T> *p = curr->parent;
  while (p != nil && curr == p->left) {
    curr = p;
    p = p->parent;
  }
  return p;
}

template <typename T>
void RBTree<T>::erase(node<T>* curr)
{
  node<T> *pdel,*child,*k = curr;
  if (curr->left == nil || curr->right == nil) {
    pdel = curr;
  } else {
    pdel = successor(k);
  }
  if (pdel->left != nil) {
    child = pdel->left;
  } else {
    child = pdel->right;
  }
  child->parent = pdel->parent;
  if (pdel->parent == nil) {
    root = child;
  } else if (pdel == pdel->parent->left) {
    pdel->parent->left = child;
  } else {
    pdel->parent->right = child;
  }
  if (curr != pdel) {
    curr->key = pdel->key;
  }
  if (pdel->color == black) {
    eraseFixup(child);
  }
  delete pdel;
}

template <typename T>
void RBTree<T>::eraseFixup(node<T> *curr)
{
  while (curr != root && curr->color == black) {
    if (curr == curr->parent->left) {
      node<T> *brother = curr->parent->right;
      if (brother->color == red) {
	brother->color = black;
	curr->parent->color = red;
	leftRotate(curr->parent);
	brother = curr->parent->right;
      }
      if (brother->left->color == black && brother->right->color == black) {
	brother->color = red;
	curr = curr->parent;
      } else if (brother->right->color == black) {
	brother->color = red;
	brother->left->color = black;
	rightRotate(brother);
	brother = curr->parent->right;
      } else {
	brother->color = curr->parent->color;
	curr->parent->color = black;
	brother->right->color = black;
	leftRotate(curr->parent);
	curr = root;
      }
    } else {
      node<T> *brother = curr->parent->left;
      if (brother->color == red) {
	brother->color = black;
	curr->parent->color = red;
	rightRotate(curr->parent);
	brother = curr->parent->left;
      }
      if (brother->right->color == black && brother->left->color == black)
      {
	brother->color = red;
	curr = curr->parent;
      } else if (brother->left->color == black) {
	brother->color = red;
	brother->right->color = black;
	leftRotate(brother);
	brother = curr->parent->left;
      } else {
	brother->color = curr->parent->color;
	curr->parent->color = black;
	brother->left->color = black;
	rightRotate(curr->parent);
	curr = root;
      }
    }
  }
  curr->color = black;
}

template <typename T>
void RBTree<T>::destroy()
{
  while (root != nil) {
    erase(root);
  }
  delete nil;
}

template <typename T>
void RBTree<T>::clear()
{
  while (root != nil) {
    erase(root);
  }
}

template <typename T>
void RBTree<T>::traverse(node<T>* n, int d) {
  if (n->left != nil) {
    traverse(n->left, d+1);
  }
  //cout << d<<"["<<n->key.left<<","<<n->key.right<<") "<<n->sum<<" ";
  if (n->right != nil) {
    traverse(n->right, d+1);
  }
}


class ReuseDist
{
  RBTree<Interval> holes;
  ceph::unordered_map<string, unsigned int> P;
  vector<int> histogram;
  unsigned int size, step;
  unsigned int T, M;
  unsigned int count;

  public:
  ReuseDist(int size, int step, unsigned int T, unsigned int M)
    : size(size), step(step), T(T), M(M), count(0) {
      histogram.resize(size);
    }
  ~ReuseDist() {
    holes.destroy();
  }

  void clear() {
    holes.clear();
    P.clear();
    for (unsigned int i = 0; i < size; i++) {
      histogram[i] = 0;
    }
    count = 0;
  }

  unsigned int BKDRHash(const char *str) {
    unsigned int seed = 131;
    unsigned int hash = 0;

    while (*str) {
      hash = hash * seed + (*str++);
    }

    return (hash & 0x7FFFFFFF);
  }

  void access(string x) {
    access(x, count);
  }

  void access(string x, int t) {
    if (BKDRHash(x.data()) % M > T) {
      return;
    }
    count = t + 1;
    if (P.find(x) == P.end()) {
      P[x] = t;
      histogram[size-1]++;
    } else {
      unsigned int d = (t - P[x] - holes.dist(P[x]) - 1) / step;
      if (d >= size - 1) {
	histogram[size-1]++;
      } else {
	histogram[d]++;
      }
      P[x] = t;
    }
  }

  const vector<int>& get_histogram() {
    return histogram;
  }

  unsigned int getM() const {
    return M;
  }

  unsigned int getSize() const {
    return size;
  }

  unsigned int getStep() const {
    return step;
  }

  unsigned int getT() const {
    return T;
  }
};

