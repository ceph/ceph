#ifndef __crush_CRUSH_H
#define __crush_CRUSH_H

#include "Bucket.h"

#include <list>
using namespace std;

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

  class Rule {
  public:
	vector< RuleStep > steps;
  };

  // CRule
  const int CRULE_TAKEB = 1;
  const int CRULE_CHOOSER = 2;

  class CRule {
  public:
	vector< vector<RuleStep> > steps;
	int nchoose;
	int nrep;

	CRule() {}
	CRule(int nr) : steps(nr), nrep(nr) {}
  };


  // *** CRUSH ***

  class Crush {
  protected:
	map<int, Bucket*>  buckets;
	map<int, Rule>     rules;
	map<int, CRule>     crules;
	Hash h;

	set<int>           failed;

  public:
	Crush(int seed=123) : h(seed) {}

	void add_bucket( Bucket *b ) {
	  buckets[b->get_id()] = b;
	}
	void add_rule( int id, Rule& r ) {
	  rules[id] = r;
	}
	void add_crule( int id, CRule& r ) {
	  crules[id] = r;
	}

	void crule_choose(int ruleno, int x, vector<int>& result) {
	  CRule& rule = crules[ruleno];
	  
	  // input
	  Bucket *in = 0;
	  int out = -1;
	  
	  // for replicas
	  for (int rep=0; rep<rule.nrep; rep++) {
		// initially zero
		vector<int> add_r(rule.nchoose);
		
		for (int attempt=0; ; attempt++) {
		  
		  // steps
		  int nchoose = 0;
		  for (int pc=0; pc<rule.steps[rep].size(); pc++) {
			switch (rule.steps[rep][pc].cmd) {
			case CRULE_TAKEB:
			  {
				const int arg = rule.steps[rep][pc].args[0];
				assert(buckets.count(arg));
				in = buckets[arg];
			  }
			  break;
			  
			case CRULE_CHOOSER:
			  {
				int r = rule.steps[rep][pc].args[0];
				const int rperiod = rule.steps[rep][pc].args[1];
				const int type = rule.steps[rep][pc].args[2];
				
				// adjust to skip unusable
				r += rperiod * add_r[nchoose];
				nchoose++;
				
				//cout << "choose_r " << r << " type " << type << endl;
				
				// choose through any intervening buckets
				while (1) {
				  // choose in this bucket
				  out = in->choose_r(x, r, h);

				  if (in->is_uniform() && 
					  ((UniformBucket*)in)->get_item_type() == type)
					break;

				  int itemtype = 0;  // 0 is a terminal type
				  if (buckets.count(out)) {
					in = buckets[out];
					itemtype = in->get_type();
				  } 
				  if (itemtype == type) break;  // this is what we want!
				}				
				
				if (type != 0) {  // translate back into a bucket
				  assert(buckets.count(out));
				  in = buckets[out];
				}
			  }
			  break;
			  
			default:
			  assert(0);
			}
		  }

		  // disk failed?
		  bool bad = false;
		  if (failed.count(out)) 
			bad = true;
		  
		  for (int prep=0; prep<rep; prep++) {
			if (result[prep] == out) 
			  bad = true;
		  }

		  if (bad) {
			// bump an 'r'
			//cout << "failed|repeat " << attempt << endl;
			add_r[ rule.nchoose - 1 - ((attempt/2)%rule.nchoose) ]++;
			continue;
		  }

		  break;		  // disk is fine.
		}
		
		// ok!
		result[rep] = out;
	  }
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
