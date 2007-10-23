
#include "../../common/Clock.h"
#include "../crush.h"
using namespace crush;


Clock g_clock;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;


int numrep = 1;


double go(int n, int bucket) 
{
  Hash h(73232313);

  // crush
  Crush c;


  // buckets
  int root = -1;
  int ndisks = 0;

  Bucket *b;
  vector<int> items;
  if (bucket == 0) b = new UniformBucket(1,0,10,items);
  if (bucket == 1) b = new TreeBucket(1);
  if (bucket == 2) b = new ListBucket(1);
  if (bucket == 3) b = new StrawBucket(1);

  for (int d=0; d<n; d++)
	b->add_item(ndisks++, 1);

  //if (!bucket)	((UniformBucket*)b)->make_primes(h);

  root = c.add_bucket(b);

  // rule
  Rule rule;
  rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
  rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
  rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));


  int place = 1000000;


  vector<int> v(numrep);
  set<int> out;
  map<int,float> overload;

  utime_t start = g_clock.now();

  for (int x=1; x <= place; x++)
	c.do_rule(rule, x, v, out, overload);

  utime_t end = g_clock.now();

  end -= start;
  double el = (double)end;

  //cout << "\t" << ndisks;

  return el;
}


int main() 
{

  for (int n=4; n<=50; n += 4) {
	cout << n;
	for (int b=0; b<4; b++) {
	  double el = go(n,b);
	  cout << "\t" << el;
	}
	cout << endl;
  }
}
