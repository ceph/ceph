#ifndef __crush_CRUSH_H
#define __crush_CRUSH_H

#include <iostream>
#include <list>
#include <vector>
#include <set>
using namespace std;

ostream& operator<<(ostream& out, vector<int>& v)
{
  out << "[";
  for (int i=0; i<v.size(); i++) {
	if (i) out << " ";
	out << v[i];
  }
  out << "]";
  return out;
}


#include "Bucket.h"


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
  };


  // Rule
  const int CRUSH_RULE_TAKE = 0;
  const int CRUSH_RULE_CHOOSE = 1;
  const int CRUSH_RULE_VERT = 2;
  const int CRUSH_RULE_EMIT = 3;

  class Rule {
  public:
	vector< RuleStep > steps;
  };




  // *** CRUSH ***

  class Crush {
  protected:
	map<int, Bucket*>  buckets;
	int bucketno;

	Hash h;

  public:
	set<int>           failed;
	map<int, float>    overload;

	vector<int> collisions;
	vector<int> bumps;	

  public:
	Crush(int seed=123) : bucketno(-1), h(seed), collisions(20), bumps(20) {}
	~Crush() {
	  // hose buckets
	  for (map<int, Bucket*>::iterator it = buckets.begin();
		   it != buckets.end();
		   it++) {
		delete it->second;
	  }
	}





	int add_bucket( Bucket *b ) {
	  b->set_id(bucketno);
	  buckets[bucketno] = b;
	  int n = bucketno;
	  bucketno--;
	  return n;
	}

	/*
	 * choose numrep distinct items of type type
	 */
	
	static const int maxdepth = 20;

	void choose(int x,
				int numrep,
				int type,
				Bucket *inbucket,
				vector<int>& outvec) {

	  // for each replica
	  for (int rep=0; rep<numrep; rep++) {
		vector<int> add_r(maxdepth);    // re-choosing; initially zero
		int out = -1;                   // my result
		
		// keep trying until we get a non-failed, non-colliding item
		for (int attempt=0; ; attempt++) {
		  
		  // start with the input bucket
		  Bucket *in = inbucket;
		  int depth = 0;
		  
		  // choose through intervening buckets
		  while (1) {
			// r may be twiddled to (try to) avoid past collisions
			int r = rep;
			if (in->is_uniform()) {
			  // uniform bucket; be careful!
			  if (numrep >= in->get_size()) {
				// uniform bucket is too small; just walk thru elements
				r += add_r[depth];
			  } else {
				// make sure numrep is not a multple of bucket size
				int add = numrep*add_r[depth];
				if (in->get_size() % numrep == 0) {
				  add += add/in->get_size();         // shift seq once per pass through the bucket
				}
				r += add;
			  }
			} else {
			  // mixed bucket; just make a distinct-ish r sequence
			  r += numrep * add_r[depth];
			}
			
			// choose
			out = in->choose_r(x, r, h); 					
			
			// did we get the type we want?
			if (in->is_uniform() && 
				((UniformBucket*)in)->get_item_type() == type)
			  break;
			
			int itemtype = 0;          // 0 is terminal type
			if (buckets.count(out)) {  // another bucket
			  in = buckets[out];
			  itemtype = in->get_type();
			} 
			if (itemtype == type) break;  // this is what we want!
			depth++;
		  }
		  
		  // ok choice?
		  int bad_localness = 0;     // 1 -> no locality in replacement, >1 more locality
		  if (type == 0 && failed.count(out)) 
			bad_localness = 1;         // no locality
		  
		  if (overload.count(out)) {
			float f = (float)(h(x, out) % 1000) / 1000.0;
			if (f > overload[out])
			  bad_localness = 1;       // no locality
		  }
		  
		  for (int prep=0; prep<rep; prep++) 
			if (outvec[prep] == out) 
			  bad_localness = 4;       // local is better
		  
		  if (bad_localness) {
			// bump an 'r'
			depth++;   // want actual depth, not array index
			bad_localness--;
			if (bad_localness) {
			  // we want locality
			  int d = depth - 1 - ((attempt/bad_localness)%(depth));
			  add_r[d]++;
			  collisions[attempt]++;
			  bumps[d]++;
			} else {
			  // uniformly try a new disk; bump all r's
			  for (int j=0; j<depth; j++) {
				add_r[j]++;
				bumps[j]++;
			  }
			  collisions[attempt]++;
			}
			continue;
		  }
		  break;
		}
		
		outvec[rep] = out;
		//cout << "outrow[" << rep << "] = " << out << endl;
	  } // for rep
	}


	void do_rule(Rule& rule, int x, vector<int>& result) {
	  int numresult = 0;

	  // working variable
	  vector< Bucket* >       in;
	  vector< vector<int>  >  out;

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
			//cout << "take " << arg << endl;

			// input is some explicit existing bucket
			assert(buckets.count(arg));
			
			in.clear();
			if (true || out.empty()) {
			  // 1 row
			  in.push_back( buckets[arg] );
			} else {
			  // match rows in output?
			  for (vector< vector<int> >::iterator row = out.begin();
				   row != out.end();
				   row++) 
				in.push_back( buckets[arg] );
			}
		  }
		  break;

		case CRUSH_RULE_VERT:
		  {
			in.clear();
			
			// input is (currently always) old output

			for (vector< vector<int> >::iterator row = out.begin();
				 row != out.end();
				 row++) {
			  for (int i=0; i<row->size(); i++) {
				in.push_back( buckets[ (*row)[i] ] );
			  }
			}
		  }
		  break;
		  
		case CRUSH_RULE_CHOOSE:
		  {
			const int numrep = pc->args[0];
			const int type = pc->args[1];

			//cout << "choose " << numrep << " of type " << type << endl;

			// reset output
			out.clear();
			
			// do each row independently
			for (vector< Bucket* >::iterator inrow = in.begin();
				 inrow != in.end();
				 inrow++) {
			  // make new output row
			  out.push_back( vector<int>(numrep) );
			  vector<int>& outrow = out.back();
			  
			  choose(x, numrep, type, *inrow, outrow);
			  //cout << "outrow is " << outrow << endl;
			} // for inrow
		  }
		  break;

		case CRUSH_RULE_EMIT:
		  {
			int o = 0;
			for (int i=0; i<out.size(); i++)
			  for (int j=0; j<out[i].size(); j++)
				result[numresult++] = out[i][j];
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
