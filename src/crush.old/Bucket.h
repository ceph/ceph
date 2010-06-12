// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_crush_BUCKET_H
#define CEPH_crush_BUCKET_H

#include "BinaryTree.h"
#include "Hash.h"

#include <list>
#include <vector>
#include <map>
#include <set>
using namespace std;

#include <math.h>

#include "include/buffer.h"

namespace crush {


  const int CRUSH_BUCKET_UNIFORM = 1;
  const int CRUSH_BUCKET_TREE = 2;
  const int CRUSH_BUCKET_LIST = 3;
  const int CRUSH_BUCKET_STRAW = 4;

  /** abstract bucket **/
  class Bucket {
  protected:
    int         id;
    int         parent;
    int         type;
    float       weight;

  public:
    Bucket(int _type,
           float _weight) :
      id(0), parent(0),
      type(_type),
      weight(_weight) { }

    Bucket(bufferlist& bl, int& off) {
      bl.copy(off, sizeof(id), (char*)&id);
      off += sizeof(id);
      bl.copy(off, sizeof(parent), (char*)&parent);
      off += sizeof(parent);
      bl.copy(off, sizeof(type), (char*)&type);
      off += sizeof(type);
      bl.copy(off, sizeof(weight), (char*)&weight);
      off += sizeof(weight);
    }

    virtual ~Bucket() { }
    
    virtual const char *get_bucket_type() const = 0;
    virtual bool is_uniform() const = 0;

    int          get_id() const { return id; } 
    int          get_type() const { return type; }
    float        get_weight() const { return weight; }
    int          get_parent() const { return parent; }
    virtual int  get_size() const = 0;

    void         set_id(int i) { id = i; }
    void         set_parent(int p) { parent = p; }
    void         set_weight(float w)  { weight = w; }

    virtual void get_items(vector<int>& i) const = 0;
    virtual float get_item_weight(int item) const = 0;
    virtual void add_item(int item, float w, bool back=false) = 0;
    virtual void adjust_item_weight(int item, float w) = 0;
    virtual void set_item_weight(int item, float w) {
      adjust_item_weight(item, w - get_item_weight(item));
    }

    virtual int choose_r(int x, int r, Hash& h) const = 0;

    virtual void _encode(bufferlist& bl) = 0;
  };




  /** uniform bucket **/
  class UniformBucket : public Bucket {    
  protected:
  public:
    vector<int> items;
    int    item_type;
    float  item_weight;

    // primes
    vector<unsigned> primes;

    int get_prime(int j) const {
      return primes[ j % primes.size() ];
    }
    void make_primes() {
      if (items.empty()) return;

      //cout << "make_primes " << get_id() << " " << items.size() << endl;
      Hash h(123+get_id());
      primes.clear();

      // start with odd number > num_items
      unsigned x = items.size() + 1;             // this is the minimum!
      x += h(items.size()) % (3*items.size());  // bump it up some
      x |= 1;                               // make it odd

      while (primes.size() < items.size()) {
        unsigned j;
        for (j=2; j*j<=x; j++) 
          if (x % j == 0) break;
        if (j*j > x) {
          primes.push_back(x);
          //cout << "prime " << x << endl;
        }
        x += 2;
      }
    }

  public:
    UniformBucket(int _type, int _item_type) :
      Bucket(_type, 0),
      item_type(_item_type) { }
    UniformBucket(int _type, int _item_type,
                  float _item_weight, vector<int>& _items) :
      Bucket(_type, _item_weight*_items.size()),
      item_type(_item_type),
      item_weight(_item_weight) {
      items = _items;
      make_primes();
    }

    UniformBucket(bufferlist& bl, int& off) : Bucket(bl, off) {
      bl.copy(off, sizeof(item_type), (char*)&item_type);
      off += sizeof(item_type);
      bl.copy(off, sizeof(item_weight), (char*)&item_weight);
      off += sizeof(item_weight);
      ::_decode(items, bl, off);
      make_primes();
    }

    void _encode(bufferlist& bl) {
      char t = CRUSH_BUCKET_UNIFORM;
      bl.append((char*)&t, sizeof(t));
      bl.append((char*)&id, sizeof(id));
      bl.append((char*)&parent, sizeof(parent));
      bl.append((char*)&type, sizeof(type));
      bl.append((char*)&weight, sizeof(weight));

      bl.append((char*)&item_type, sizeof(item_type));
      bl.append((char*)&item_weight, sizeof(item_weight));

      ::_encode(items, bl);
    }

    const char *get_bucket_type() const { return "uniform"; }
    bool is_uniform() const { return true; }

    int get_size() const { return items.size(); }

    // items
    void get_items(vector<int>& i) const {
      i = items;
    }
    int get_item_type() const { return item_type; }
    float get_item_weight(int item) const { return item_weight; }

    void add_item(int item, float w, bool back=false) {
      if (items.empty())
        item_weight = w;
      items.push_back(item);
      weight += item_weight;
      make_primes();
    }

    void adjust_item_weight(int item, float w) {
      assert(0);
    }

    int choose_r(int x, int r, Hash& hash) const {
      //cout << "uniformbucket.choose_r(" << x << ", " << r << ")" << endl;
      //if (r >= get_size()) cout << "warning: r " << r << " >= " << get_size() << " uniformbucket.size" << endl;
      
      unsigned v = hash(x, get_id());// % get_size();
      unsigned p = get_prime( hash(get_id(), x) );  // choose a prime based on hash(x, get_id(), 2)
      unsigned s = (x + v + (r+1)*p) % get_size();
      return items[s];
    }

  };




  
  // list bucket.. RUSH_P sorta
  
  class ListBucket : public Bucket {
  protected:
    list<int>        items;
    list<float>      item_weight;
    list<float>      sum_weight;
    
  public:
    ListBucket(int _type) : Bucket(_type, 0) { }

    ListBucket(bufferlist& bl, int& off) : Bucket(bl, off) {
      ::_decode(items, bl, off);
      ::_decode(item_weight, bl, off);
      ::_decode(sum_weight, bl, off);
    }

    void _encode(bufferlist& bl) {
      char t = CRUSH_BUCKET_LIST;
      bl.append((char*)&t, sizeof(t));
      bl.append((char*)&id, sizeof(id));
      bl.append((char*)&parent, sizeof(parent));
      bl.append((char*)&type, sizeof(type));
      bl.append((char*)&weight, sizeof(weight));

      ::_encode(items, bl);
      ::_encode(item_weight, bl);
      ::_encode(sum_weight, bl);
    }

    const char *get_bucket_type() const { return "list"; }
    bool        is_uniform() const { return false; }

    int get_size() const { return items.size(); }

    void get_items(vector<int>& i) const {
      for (list<int>::const_iterator it = items.begin();
           it != items.end();
           it++) 
        i.push_back(*it);
    }
    float get_item_weight(int item) const {
      list<int>::const_iterator i = items.begin();
      list<float>::const_iterator w = item_weight.begin();
      while (i != items.end()) {
        if (*i == item) return *w;
        i++; w++;
      }
      assert(0);
      return 0;
    }

    void add_item(int item, float w, bool back=false) {
      if (back) {
        items.push_back(item);
        item_weight.push_back(w);
        sum_weight.clear();
        float s = 0.0;
        for (list<float>::reverse_iterator i = item_weight.rbegin();
             i != item_weight.rend();
             i++) {
          s += *i;
          sum_weight.push_front(s);
        }
        weight += w;
        assert(weight == s);
      } else {
        items.push_front(item);
        item_weight.push_front(w);
        weight += w;
        sum_weight.push_front(weight);
      }
    }

    void adjust_item_weight(int item, float dw) {
      // find it
      list<int>::iterator p = items.begin();
      list<float>::iterator pw = item_weight.begin();
      list<float>::iterator ps = sum_weight.begin();

      while (*p != item) {
        *ps += dw;
        p++; pw++; ps++;  // next!
        assert(p != items.end());
      }

      assert(*p == item);
      *pw += dw;
      *ps += dw;
    }

    
    int choose_r(int x, int r, Hash& h) const {
      //cout << "linearbucket.choose_r(" << x << ", " << r << ")" << endl;

      list<int>::const_iterator p = items.begin();
      list<float>::const_iterator pw = item_weight.begin();
      list<float>::const_iterator ps = sum_weight.begin();

      while (p != items.end()) {
        const int item = *p;
        const float iw = *pw;
        const float tw = *ps;
        const float f = (float)(h(x, item, r, get_id()) % 10000) * tw / 10000.0;
        //cout << "item " << item << "  iw = " << iw << "  tw = " << tw << "  f = " << f << endl;
        if (f < iw) {
          //cout << "linearbucket.choose_r(" << x << ", " << r << ") = " << item << endl;
          return item;
        }
        p++; pw++; ps++;  // next!
      }
      assert(0);
      return 0;
    }    


  };




  // mixed bucket, based on RUSH_T type binary tree
  
  class TreeBucket : public Bucket {
  protected:
    //vector<float>  item_weight;

    //  public:
    BinaryTree     tree;
    map<int,int>   node_item;     // node id -> item
    vector<int>    node_item_vec; // fast version of above
    map<int,int>   item_node;     // item -> node id
    map<int,float> item_weight;

  public:
    TreeBucket(int _type) : Bucket(_type, 0) { }
    
    TreeBucket(bufferlist& bl, int& off) : Bucket(bl, off) {
      tree._decode(bl, off);
      
      ::_decode(node_item, bl, off);
      ::_decode(node_item_vec, bl, off);
      ::_decode(item_node, bl, off);
      ::_decode(item_weight, bl, off);
    }

    void _encode(bufferlist& bl) {
      char t = CRUSH_BUCKET_TREE;
      bl.append((char*)&t, sizeof(t));
      bl.append((char*)&id, sizeof(id));
      bl.append((char*)&parent, sizeof(parent));
      bl.append((char*)&type, sizeof(type));
      bl.append((char*)&weight, sizeof(weight));

      tree._encode(bl);

      ::_encode(node_item, bl);
      ::_encode(node_item_vec, bl);
      ::_encode(item_node, bl);
      ::_encode(item_weight, bl);
    }

    const char *get_bucket_type() const { return "tree"; }
    bool        is_uniform() const { return false; }

    int get_size() const { return node_item.size(); }

    // items
    void get_items(vector<int>& i) const {
      for (map<int,int>::const_iterator it = node_item.begin();
           it != node_item.end();
           it++) 
        i.push_back(it->second);    
    }
    float get_item_weight(int i) const { 
      assert(item_weight.count(i));
      return ((map<int,float>)item_weight)[i]; 
    }


    void add_item(int item, float w, bool back=false) {
      item_weight[item] = w;
      weight += w;

      unsigned n = tree.add_node(w);
      node_item[n] = item;
      item_node[item] = n;

      while (node_item_vec.size() <= n) 
        node_item_vec.push_back(0);
      node_item_vec[n] = item;
    }
    
    void adjust_item_weight(int item, float dw) {
      // adjust my weight
      weight += dw;
      item_weight[item] += dw;

      // adjust tree weights
      tree.adjust_node_weight(item_node[item], dw);
    }
    
    int choose_r(int x, int r, Hash& h) const {
      //cout << "mixedbucket.choose_r(" << x << ", " << r << ")" << endl;
      int n = tree.root();
      while (!tree.terminal(n)) {
        // pick a point in [0,w)
        float w = tree.weight(n);
        float f = (float)(h(x, n, r, get_id()) % 10000) * w / 10000.0;

        // left or right?
        int l = tree.left(n);
        if (tree.exists(l) && 
            f < tree.weight(l))
          n = l;
        else
          n = tree.right(n);
      }
      //assert(node_item.count(n));
      //return ((map<int,int>)node_item)[n];
      return node_item_vec[n];
    }
  };





  // straw bucket.. new thing!
  
  class StrawBucket : public Bucket {
  protected:
    map<int, float>  item_weight;
    map<int, float>  item_straw;

    list<int>   _items;
    list<float> _straws;

  public:
    StrawBucket(int _type) : Bucket(_type, 0) { }

    StrawBucket(bufferlist& bl, int& off) : Bucket(bl, off) {
      ::_decode(item_weight, bl, off);
      calc_straws();
    }

    void _encode(bufferlist& bl) {
      char t = CRUSH_BUCKET_TREE;
      bl.append((char*)&t, sizeof(t));
      bl.append((char*)&id, sizeof(id));
      bl.append((char*)&parent, sizeof(parent));
      bl.append((char*)&type, sizeof(type));
      bl.append((char*)&weight, sizeof(weight));

      ::_encode(item_weight, bl);
    }

    const char *get_bucket_type() const { return "straw"; }
    bool is_uniform() const { return false; }

    int get_size() const { return item_weight.size(); }


    // items
    void get_items(vector<int>& i) const {
      for (map<int,float>::const_iterator it = item_weight.begin();
           it != item_weight.end();
           it++) 
        i.push_back(it->first);
    }
    float get_item_weight(int item) const {
      assert(item_weight.count(item));
      return ((map<int,float>)item_weight)[item];
    }

    void add_item(int item, float w, bool back=false) {
      item_weight[item] = w;
      weight += w;
      calc_straws();
    }

    void adjust_item_weight(int item, float dw) {
      //cout << "adjust " << item << " " << dw << endl;
      weight += dw;
      item_weight[item] += dw;
      calc_straws();
    }
    
    
    /* calculate straw lengths.
       this is kind of ugly.  not sure if there's a closed form way to calculate this or not!    
     */
    void calc_straws() {
      //cout << get_id() << ": calc_straws ============" << endl;

      item_straw.clear();
      _items.clear();
      _straws.clear();

      // reverse sort by weight; skip zero weight items
      map<float, set<int> > reverse;
      for (map<int, float>::iterator p = item_weight.begin();
           p != item_weight.end();
           p++) {
        //cout << get_id() << ":" << p->first << " " << p->second << endl;
        if (p->second > 0) {
          //p->second /= minw;
          reverse[p->second].insert(p->first);
        }
      }

      /* 1:2:7 
         item_straw[0] = 1.0;
         item_straw[1] = item_straw[0]*sqrt(1.0/.6);
         item_straw[2] = item_straw[1]*2.0;
      */

      // work from low to high weights
      float straw = 1.0;
      float numleft = item_weight.size();
      float wbelow = 0.0;
      float lastw = 0.0;
      
      map<float, set<int> >::iterator next = reverse.begin();
      //while (next != reverse.end()) {
      while (1) {
        //cout << "hi " << next->first << endl;
        map<float, set<int> >::iterator cur = next;
        
        // set straw length for this set
        for (set<int>::iterator s = cur->second.begin();
             s != cur->second.end();
             s++) {
          item_straw[*s] = straw;
          //cout << "straw " << *s << " w " << item_weight[*s] << " -> " << straw << endl;
          _items.push_back(*s);
          _straws.push_back(straw);
        }
        
        next++;
        if (next == reverse.end()) break;
        
        wbelow += (cur->first-lastw) * numleft;
        //cout << "wbelow " << wbelow << endl;
        
        numleft -= 1.0 * (float)cur->second.size();
        //cout << "numleft now " << numleft << endl;
        
        float wnext = numleft * (next->first - cur->first);
        //cout << "wnext " << wnext << endl;
        
        float pbelow = wbelow / (wbelow+wnext);
        //cout << "pbelow " << pbelow << endl;
        
        straw *= pow((double)(1.0/pbelow), (double)1.0/numleft);
        
        lastw = cur->first;
      }
      //cout << "============" << endl;
    }

    int choose_r(int x, int r, Hash& h) const {
      //cout << "strawbucket.choose_r(" << x << ", " << r << ")" << endl;

      float high_draw = -1;
      int high = 0;

      list<int>::const_iterator pi = _items.begin();
      list<float>::const_iterator ps = _straws.begin();
      while (pi != _items.end()) {
        const int item = *pi;
        const float rnd = (float)(h(x, item, r) % 1000000) / 1000000.0;
        const float straw = *ps * rnd;
        
        if (high_draw < 0 ||
            straw > high_draw) {
          high = *pi;
          high_draw = straw;
        }

        pi++;
        ps++;
      }
      return high;
    }    
  };





  inline Bucket* decode_bucket(bufferlist& bl, int& off) {
    char t;
    bl.copy(off, sizeof(t), (char*)&t);
    off += sizeof(t);

    switch (t) {
    case CRUSH_BUCKET_UNIFORM:
      return new UniformBucket(bl, off);
    case CRUSH_BUCKET_LIST:
      return new ListBucket(bl, off);
    case CRUSH_BUCKET_TREE:
      return new TreeBucket(bl, off);
    case CRUSH_BUCKET_STRAW:
      return new StrawBucket(bl, off);
    default:
      assert(0);
    }
    return 0;
  }



}








#endif
