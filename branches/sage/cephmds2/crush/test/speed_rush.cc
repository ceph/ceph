
#include "../../common/Clock.h"
#include "../crush.h"
using namespace crush;


Clock g_clock;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;


int branching = 10;
bool linear = false;
int numrep = 1;

Bucket *make_bucket(Crush& c, vector<int>& wid, int h, int& ndisks)
{
  if (h == 0) {
	// uniform
	Hash hash(123);
	vector<int> disks;
	for (int i=0; i<wid[h]; i++)
	  disks.push_back(ndisks++);
	UniformBucket *b = new UniformBucket(1, 0, 10, disks);
	//b->make_primes(hash);  
	c.add_bucket(b);
	//cout << h << " uniformbucket with " << wid[h] << " disks" << endl;
	return b;
  } else {
	// mixed
	Bucket *b;
	if (linear)
	  b = new ListBucket(h+1);
	else
	  b = new TreeBucket(h+1);
	for (int i=0; i<wid[h]; i++) {
	  Bucket *n = make_bucket(c, wid, h-1, ndisks);
	  b->add_item(n->get_id(), n->get_weight());
	}
	c.add_bucket(b);
	//cout << h << " mixedbucket with " << wid[h] << endl;
	return b;
  }
}

int make_hierarchy(Crush& c, vector<int>& wid, int& ndisks)
{
  Bucket *b = make_bucket(c, wid, wid.size()-1, ndisks);
  return b->get_id();
}


double go(int s) 
{
  int dep = 2;
  Hash h(73232313);

  // crush
  Crush c;


  // buckets
  int root = -1;
  int ndisks = 0;

  vector<int> wid;
  if (1) {
	//for (int d=0; d<dep; d++)
	wid.push_back(8);
	wid.push_back(s/8);
  }
  if (0) {
	if (dep == 0) 
	  wid.push_back(1000);
	if (dep == 1) {
	  wid.push_back(1);
	  wid.push_back(1000);
	}
	if (dep == 2) {
	  wid.push_back(5);
	  wid.push_back(5);
	  wid.push_back(8);
	  wid.push_back(5);
	}	
  }

  if (1) {
	root = make_hierarchy(c, wid, ndisks);
  }
  


  // rule
  Rule rule;
  rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
  rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
  rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));


  int place = 1000000;


  vector<int> v(numrep);

  utime_t start = g_clock.now();

  for (int x=1; x <= place; x++)
	c.do_rule(rule, x, v);

  utime_t end = g_clock.now();

  end -= start;
  double el = (double)end;

  cout << "\t" << ndisks;

  return el;
}


int main() 
{
  branching = 8;

  int d = 2;
  numrep = 2;

  for (int s = 64; s <= 32768; s *= 8) {
	cout << "t";
	linear = false;
	double el = go(s, d);
	cout << "\t" << el;

	cout << "\tp";
	linear = true;
	el = go(s, d);
	cout << "\t" << el;

	cout << endl;
  }
}
