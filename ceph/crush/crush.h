#ifndef __crush_CRUSH_H
#define __crush_CRUSH_H

#include "Bucket.h"

#include <list>
using namespace std;

namespace crush {

  // *** RULES ***

  // rule commands
  const int CRUSH_RULE_TAKE = 0;
  const int CRUSH_RULE_CHOOSE = 1;
  const int CRUSH_RULE_VERT = 2;

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
  };

  class Rule {
  public:
	vector< RuleStep > steps;
  };



  // *** CRUSH ***

  class Crush {
  protected:
	map<int, Bucket*>  buckets;
	map<int, Rule>     rules;
	Hash h;

  public:
	Crush(int seed=123) : h(seed) {}

	void add_bucket( Bucket *b ) {
	  buckets[b->get_id()] = b;
	}
	void add_rule( int id, Rule& r ) {
	  rules[id] = r;
	}

	void choose(int rno, int x, vector<int>& result) {
	  assert(rules.count(rno));
	  Rule& r = rules[rno];

	  // working variable
	  vector< Bucket* >       in;
	  vector< vector<int>  >  out;

	  list< MixedBucket >     temp_buckets;

	  // go through each statement
	  for (vector<RuleStep>::iterator pc = r.steps.begin();
		   pc != r.steps.end();
		   pc++) {
		// move input?
		
		// do it
		switch (pc->cmd) {
		case CRUSH_RULE_TAKE:
		  {
			const int arg = pc->args[0];
			//cout << "take " << arg << endl;

			in.clear();
			temp_buckets.clear();

			if (arg == 0) {  
			  // input is old output

			  for (vector< vector<int> >::iterator row = out.begin();
				   row != out.end();
				   row++) {
				
				if (row->size() == 1) {
				  in.push_back( buckets[ (*row)[0] ] );
				} else {
				  // make a temp bucket!
				  temp_buckets.push_back( MixedBucket( rno, -1 ) );
				  in.push_back( &temp_buckets.back() );
				  
				  // put everything in.
				  for (int j=0; j<row->size(); j++)
					temp_buckets.back().add_item( (*row)[j],
												  buckets[ (*row)[j] ]->get_weight() );
				}
			  }
			  
			  // reset output variable
			  //out.clear();
			  out = vector< vector<int> >(in.size());

			} else {         
			  // input is some explicit existing bucket
			  assert(buckets.count(arg));

			  if (out.empty()) {
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
			
			/*
			cout << "take: in is [";
			for (int i=0; i<in.size(); i++) 
			  cout << " " << in[i]->get_id();
			cout << "]" << endl;			  
			*/
		  }
		  break;

		case CRUSH_RULE_VERT:
		  {
			in.clear();
			temp_buckets.clear();
			
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
			const int num = pc->args[0];
			const int type = pc->args[1];
			
			// reset output
			out.clear();
			
			// do each row independently
			for (vector< Bucket* >::iterator row = in.begin();
				 row != in.end();
				 row++) {
			  // make new output row
			  out.push_back( vector<int>() );
			  vector<int>& outrow = out.back();
			  
			  // for each replica
			  for (int r=0; r<num; r++) {
				// start with input bucket
				const Bucket *b = *row;
				
				// choose through any intervening buckets
				while (1) {
				  if (b->is_uniform() && 
					  ((UniformBucket*)b)->get_item_type() == type) 
					break;
				  
				  int next = b->choose_r(x, r, h);
				  int itemtype = 0;  // 0 is a terminal type
				  if (buckets.count(next)) {
					b = buckets[next];
					itemtype = b->get_type();
				  } 
				  if (itemtype == type) break;  // this is what we want!
				}

				// choose in this bucket!
				int item = b->choose_r(x, r, h);
				outrow.push_back(item);
			  }
			}
		  }
		  break;

		default:
		  assert(0);
		}
		
		
	  }

	  // assemble result
	  int o = 0;
	  for (int i=0; i<out.size(); i++)
		for (int j=0; j<out[i].size(); j++)
		  result[o++] = out[i][j];
	}
	
  };
  
}

#endif
