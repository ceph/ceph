#ifndef __crush_CRUSH_H
#define __crush_CRUSH_H

namespace crush {

#define CRUSH_RULE_TAKE    0
#define CRUSH_RULE_CHOOSE  1
#define CRUSH_RULE_VERT    2


  class RuleStep {
	int       cmd;
	list<int> args;
  };

  class Rule {
	list< RuleStatement > steps;
  };



  class Crush {
  protected:
	map<int, Bucket>  buckets;
	map<int, Rule>    rules;

  public:

	void do_rule(int rno) {
	  assert(rules.count(rno));
	  Rule& r = rules[rno];

	  // working variable
	  list< Bucket* >       in;
	  list< vector<int>  >  out;

	  list< Bucket >        temp_buckets;

	  // go through each statement
	  for (list<RuleStatement>::iterator pc = r.steps.begin();
		   pc != r.steps.end();
		   pc++) {
		// move input?
		
		// do it
		switch (pc->cmd) {
		case CRUSH_RULE_TAKE:
		  {
			in.clear();
			temp_buckets.clear();
			
			int arg = r.args[0];
			if (arg == 0) {  
			  // input is old output

			  for (list< vector<int> >::iterator row = out.begin();
				   row != out.end();
				   row++) {
				
				if (row->size() == 1) {
				  in.push_back( &buckets[ (*row)[0] ] );
				} else {
				  // make a temp bucket!
				  temp_buckets.push_back( MixedBucket( rno, -1 ) );
				  in.push_back( &temp_buckets.back() );
				  
				  // put everything in.
				  temp_buckets.back().make_new_tree( *row );
				}
			  }
			  
			  // clear output variable
			  out.clear();

			} else {         
			  // input is some explicit existing bucket
			  assert(buckets.count(arg));

			  // match rows in output?
			  for (list< vector<int> >::iterator row = out.begin();
				   row != out.end();
				   row++) 
				in.push_back( &buckets[arg] );
			}
			
		  }
		  break;

		case CRUSH_RULE_VERT:
		  {
			in.clear();
			temp_buckets.clear();
			
			// input is (always) old output

			for (list< vector<int> >::iterator row = out.begin();
				 row != out.end();
				 row++) {
			  for (int i=0; i<row->size(); i++) {
				in.push_back( &buckets[ (*row)[i] ] );
			  }
			}
		  }s.front();
		  break;

		case CRUSH_RULE_CHOOSE:
		  {
			int num = r.args[0];
			int type = r.args[1];

			// reset output
			out.clear();
			
			// do each row independently
			for (list< Bucket* >::iterator row = in.begin();
				 row != in.end();
				 row++) {
			  // make new output row
			  out.push_back( vector<int> );
			  vector<int>& outrow = out.back();
			  
			  // for each replica
			  for (int r=0; r<num; r++) {
				// start with input bucket
				Bucket *b = *row;
				
				// choose through any intervening buckets
				while (1) {
				  if (b->is_uniform() && 
					  ((UniformBucket*)b)->get_item_type() == type) 
					break;
				  
				  int next = b->choose_r(x, r, h);
				  int itemtype = 0;  // 0 is a terminal type
				  if (buckets.count(next)) {
					b = &buckets[next];
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
	  vector<int> result;
	  list< vector<int> >::iterator row = out.begin();
	  result.swap( *row );
	  while (row != out.end()) {
		result.append( result.end(), row->begin(), row->end() );
	  }
	  

	}

  }

}

#endif
