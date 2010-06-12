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

#ifndef CEPH_crush_CRUSH_H
#define CEPH_crush_CRUSH_H

#include <iostream>
#include <list>
#include <vector>
#include <set>
#include <map>
using std::set;
using std::map;
using std::vector;
using std::list;
#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;


#include "Bucket.h"

#include "include/buffer.h"


namespace crush {


  // *** RULES ***

  class RuleStep {
  public:
    int         cmd;
    vector<int> args;

    RuleStep(int c) : cmd(c) {}
    RuleStep(int c, int a) : cmd(c) {
      args.push_back(a);
    }
    RuleStep(int c, int a, int b) : cmd(c) {
      args.push_back(a);
      args.push_back(b);
    }
    RuleStep(int o, int a, int b, int c) : cmd(o) {
      args.push_back(a);
      args.push_back(b);
      args.push_back(c);
    }

    void _encode(bufferlist& bl) {
      bl.append((char*)&cmd, sizeof(cmd));
      ::_encode(args, bl);
    }
    void _decode(bufferlist& bl, int& off) {
      bl.copy(off, sizeof(cmd), (char*)&cmd);
      off += sizeof(cmd);
      ::_decode(args, bl, off);
    }
  };


  // Rule operations
  const int CRUSH_RULE_TAKE = 0;
  const int CRUSH_RULE_CHOOSE = 1;         // first n by default
  const int CRUSH_RULE_CHOOSE_FIRSTN = 1;
  const int CRUSH_RULE_CHOOSE_INDEP = 2;
  const int CRUSH_RULE_EMIT = 3;

  class Rule {
  public:
    vector< RuleStep > steps;

    void _encode(bufferlist& bl) {
      int n = steps.size();
      bl.append((char*)&n, sizeof(n));
      for (int i=0; i<n; i++)
        steps[i]._encode(bl);
    }
    void _decode(bufferlist& bl, int& off) {
      steps.clear();
      int n;
      bl.copy(off, sizeof(n), (char*)&n);
      off += sizeof(n);
      for (int i=0; i<n; i++) {
        steps.push_back(RuleStep(0));
        steps[i]._decode(bl, off);
      }
    }
  };




  // *** CRUSH ***

  class Crush {
  protected:
    map<int, Bucket*>  buckets;
    int bucketno;
    Hash h;

    hash_map<int, int> parent_map;  // what bucket each leaf/bucket lives in

  public:
    map<int, Rule>     rules;

    //map<int,int> collisions;
    //map<int,int> bumps;    

    void _encode(bufferlist& bl) {
      // buckets
      int n = buckets.size();
      bl.append((char*)&n, sizeof(n));
      for (map<int, Bucket*>::const_iterator it = buckets.begin();
           it != buckets.end();
           it++) {
        bl.append((char*)&it->first, sizeof(it->first));
        it->second->_encode(bl);
      }
      bl.append((char*)&bucketno, sizeof(bucketno));

      // hash
      int s = h.get_seed();
      bl.append((char*)&s, sizeof(s));

      //::_encode(out, bl);
      //::_encode(overload, bl);
      
      // rules
      n = rules.size();
      bl.append((char*)&n, sizeof(n));
      for(map<int, Rule>::iterator it = rules.begin();
          it != rules.end();
          it++) {
        bl.append((char*)&it->first, sizeof(it->first));
        it->second._encode(bl);
      }
        
    }

    void _decode(bufferlist& bl, int& off) {
      int n;
      bl.copy(off, sizeof(n), (char*)&n);
      off += sizeof(n);
      for (int i=0; i<n; i++) {
        int bid;
        bl.copy(off, sizeof(bid), (char*)&bid);
        off += sizeof(bid);
        Bucket *b = decode_bucket(bl, off);
        buckets[bid] = b;
      }
      bl.copy(off, sizeof(bucketno), (char*)&bucketno);
      off += sizeof(bucketno);

      int s;
      bl.copy(off, sizeof(s), (char*)&s);
      off += sizeof(s);
      h.set_seed(s);

      //::_decode(out, bl, off);
      //::_decode(overload, bl, off);

      // rules
      bl.copy(off, sizeof(n), (char*)&n);
      off += sizeof(n);
      for (int i=0; i<n; i++) {
        int r;
        bl.copy(off, sizeof(r), (char*)&r);
        off += sizeof(r);
        rules[r]._decode(bl,off);
      }
      
      // index
      build_parent_map();
    }

    void build_parent_map() {
      parent_map.clear();
      
      // index every bucket
      for (map<int, Bucket*>::iterator bp = buckets.begin();
	   bp != buckets.end();
	   ++bp) {
	// index bucket items
	vector<int> items;
	bp->second->get_items(items);
	for (vector<int>::iterator ip = items.begin();
	     ip != items.end();
	     ++ip)
	  parent_map[*ip] = bp->first;
      }
    }
    


  public:
    Crush(int seed=123) : bucketno(-1), h(seed) {}
    ~Crush() {
      // hose buckets
      for (map<int, Bucket*>::iterator it = buckets.begin();
           it != buckets.end();
           it++) {
        delete it->second;
      }
    }

    int print(ostream& out, int root, int indent=0) {
      for (int i=0; i<indent; i++) out << " ";
      Bucket *b = buckets[root];
      assert(b);
      out << b->get_weight() << "\t" << b->get_id() << "\t";
      for (int i=0; i<indent; i++) out << " ";
      out << b->get_bucket_type() << ": ";

      vector<int> items;
      b->get_items(items);

      if (buckets.count(items[0])) {
        out << std::endl;
        for (unsigned i=0; i<items.size(); i++)
          print(out, items[i], indent+1);
      } else {
        out << "[";
        for (unsigned i=0; i<items.size(); i++) {
          if (i) out << " ";
          out << items[i];
        }
        out << "]";
      }
      return 0;
    }


    int add_bucket( Bucket *b ) {
      int n = bucketno;
      bucketno--;
      b->set_id(n);
      buckets[n] = b;
      return n;
    }

    void add_item(int parent, int item, float w, bool back=false) {
      // add item
      assert(!buckets[parent]->is_uniform());
      Bucket *p = buckets[parent];
      
      p->add_item(item, w, back);

      // set item's parent
      Bucket *n = buckets[item];
      if (n)
        n->set_parent(parent);

      // update weights
      while (buckets.count(p->get_parent())) {
        int child = p->get_id();
        p = buckets[p->get_parent()];
        p->adjust_item_weight(child, w);
      }
    }


    /*
    this is a hack, fix me!  weights should be consistent throughout hierarchy!
    
     */
    void set_bucket_weight(int item, float w) {
      Bucket *b = buckets[item];
      float adj = w - b->get_weight();

      while (buckets.count(b->get_parent())) {
        Bucket *p = buckets[b->get_parent()];
        p->adjust_item_weight(b->get_id(), adj);
        b = p;
      }
    }


    /*
     * choose numrep distinct items of type type
     */
    void choose(int x,
                int numrep,
                int type,
                Bucket *inbucket,
                vector<int>& outvec,
                bool firstn,
                set<int>& outset, map<int,float>& overloadmap,
		bool forcefeed=false,
		int forcefeedval=-1) {
      int off = outvec.size();

      // for each replica
      for (int rep=0; rep<numrep; rep++) {
        int outv = -1;                   // my result
        
	// forcefeed?
	if (forcefeed) {
	  forcefeed = false;
	  outvec.push_back(forcefeedval);
	  continue;
	}
	
        // keep trying until we get a non-out, non-colliding item
        int ftotal = 0;
        bool skip_rep = false;
	
        while (1) {
          // start with the input bucket
          Bucket *in = inbucket;
          
          // choose through intervening buckets
          int flocal = 0;
          bool retry_rep = false;

          while (1) {
            // r may be twiddled to (try to) avoid past collisions
            int r = rep;
            if (in->is_uniform()) {
              // uniform bucket; be careful!
              if (firstn || numrep >= in->get_size()) {
                // uniform bucket is too small; just walk thru elements
                r += ftotal;                    // r' = r + f_total (first n)
              } else {
                // make sure numrep is not a multple of bucket size
                int add = numrep*flocal;        // r' = r + n*f_local
                if (in->get_size() % numrep == 0) {
                  add += add/in->get_size();         // shift seq once per pass through the bucket
                }
                r += add;
              }
            } else {
              // mixed bucket; just make a distinct-ish r sequence
              if (firstn)
                r += ftotal;          // r' = r + f_total
              else
                r += numrep * flocal; // r' = r + n*f_local
            }
            
            // choose
            outv = in->choose_r(x, r, h);                     
            
            // did we get the type we want?
            int itemtype = 0;          // 0 is terminal type
            Bucket *newin = 0;         // remember bucket we hit
            if (in->is_uniform()) {
              itemtype = ((UniformBucket*)in)->get_item_type();
            } else {
              if (buckets.count(outv)) {  // another bucket
                newin = buckets[outv];
                itemtype = newin->get_type();
              } 
            }
            if (itemtype == type) { // this is what we want!
              // collision?
              bool collide = false;
              for (int prep=0; prep<rep; prep++) {
                if (outvec[off+prep] == outv) {
                  collide = true;
                  break;
                }
              }

              // ok choice?
              bool bad = false;
              if (type == 0 && outset.count(outv)) 
                bad = true;
              if (overloadmap.count(outv)) {
                float f = (float)(h(x, outv) % 1000) / 1000.0;
                if (f > overloadmap[outv])
                  bad = true;
              }

              if (collide || bad) {
                ftotal++;
                flocal++;
                
                if (collide && flocal < 3) 
                  continue;  // try locally a few times!
                
                if (ftotal >= 10) {
                  // ok fine, just ignore dup.  FIXME.
                  skip_rep = true;
                  break;
                }
                
                retry_rep = true;
              }

              break;  // ok then!
            }

            // next
            in = newin;
          }
          
          if (retry_rep) continue;  // try again

          break;
        }

        // skip this rep? (e.g. too many collisions, we give up)
        if (skip_rep) continue; 

        // output this value
        outvec.push_back(outv);
      } // for rep

      // double check!
      if (0) {
        for (unsigned i=1; i<outvec.size(); i++) 
          for (unsigned j=0; j<i; j++)
            assert(outvec[i] != outvec[j]);
      }
    }


    void do_rule(Rule& rule, int x, vector<int>& result,
                 set<int>& outset, map<int,float>& overloadmap,
		 int forcefeed=-1) {
      //int numresult = 0;
      result.clear();

      // determine hierarchical context for forcefeed (if any)
      list<int> force_stack;
      if (forcefeed >= 0 && parent_map.count(forcefeed)) {
	int t = forcefeed;
	while (1) {
	  force_stack.push_front(t);
	  //cout << "push " << t << " onto force_stack" << std::endl;
	  if (parent_map.count(t) == 0) break;  // reached root, presumably.
	  //cout << " " << t << " parent is " << parent_map[t] << std::endl;
	  t = parent_map[t];
	}
      }
      
      // working vector
      vector<int> w;   // working variable
      
      // go through each statement
      for (vector<RuleStep>::iterator pc = rule.steps.begin();
           pc != rule.steps.end();
           pc++) {
        // move input?
        
        // do it
        switch (pc->cmd) {
        case CRUSH_RULE_TAKE:
          {    
	    const int arg = pc->args[0];
	    //cout << "take " << arg << std::endl;	      
	    
	    if (!force_stack.empty()) {
	      assert(force_stack.front() == arg);
	      force_stack.pop_front();
	    }
	    
	    w.clear();
	    w.push_back(arg);
          }
          break;
          
        case CRUSH_RULE_CHOOSE_FIRSTN:
        case CRUSH_RULE_CHOOSE_INDEP:
          {
            const bool firstn = pc->cmd == CRUSH_RULE_CHOOSE_FIRSTN;
            const int numrep = pc->args[0];
            const int type = pc->args[1];

            //cout << "choose " << numrep << " of type " << type << std::endl;

            assert(!w.empty());

            // reset output
            vector<int> out;

            // forcefeeding?
	    bool forcing = false;
	    int forceval = -1;
	    if (!force_stack.empty()) {
	      forceval = force_stack.front();
	      force_stack.pop_front();
	      //cout << "priming out with " << forceval << std::endl;
	      forcing = true;
	    } else if (forcefeed >= 0 && type == 0) {
	      //cout << "forcing context-less " << forcefeed << std::endl;
	      forceval = forcefeed;
	      forcefeed = -1;
	      forcing = true;
	    }
	    
            // do each row independently
            for (vector<int>::iterator i = w.begin();
                 i != w.end();
                 i++) {
              assert(buckets.count(*i));
              Bucket *b = buckets[*i];
	      choose(x, numrep, type, b, out, firstn,
		     outset, overloadmap,
		     forcing,
		     forceval);
	      forcing = false;  // only once
            } // for inrow
            
            // put back into w
            w.swap(out);
            out.clear();
          }
          break;

        case CRUSH_RULE_EMIT:
          {
            for (unsigned i=0; i<w.size(); i++)
              result.push_back(w[i]);
            //result[numresult++] = w[i];
            w.clear();
          }
          break;

        default:
          assert(0);
        }
      }

    }


  };

}

#endif
