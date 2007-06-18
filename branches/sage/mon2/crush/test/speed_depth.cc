
#include "../../common/Clock.h"
#include "../crush.h"
using namespace crush;


Clock g_clock;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;


int uniform = 10;
int branching = 10;
int buckettype = 0;
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
	if (buckettype == 0)
	  b = new TreeBucket(h+1);
	else if (buckettype == 1 || buckettype == 2)
	  b = new ListBucket(h+1);
	else if (buckettype == 3)
	  b = new StrawBucket(h+1);
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


double go(int dep, int per) 
{
  Hash h(73232313);

  // crush
  Crush c;


  // buckets
  int root = -1;
  int ndisks = 0;

  vector<int> wid;
  if (1) {
	wid.push_back(uniform);
	for (int d=1; d<dep; d++)
	  wid.push_back(per);
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

  set<int> out;
  map<int,float> overload;

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
  uniform = branching = 8;

  cout << "// dep\tuniform\tbranch\tndisks" << endl;

  for (int d=2; d<=5; d++) {
	cout << d;// << "\t" << branching;
	cout << "\t" << uniform;
	cout << "\t" << branching;

	int n = 1;
	for (int i=0; i<d; i++)
	  n *= branching;
	cout << "\t" << n;

	numrep = 2;

	// crush
	for (buckettype = 0; buckettype <= 3; buckettype++) {
	  switch (buckettype) {
	  case 0: cout << "\ttree"; break;
	  case 1: cout << "\tlist"; break;
	  case 2: continue;
	  case 3: cout << "\tstraw"; break;
	  }

	  //for (numrep = 1; numrep <= 3; numrep++) {
	  //cout << "\t" << numrep;
	  
	  double el = go(d, branching);
	  cout << "\t" << el;
	}

	// rush

	buckettype = 0;
	cout << "\trush_T\t" << go(2, n/uniform);

	buckettype = 1;
	cout << "\trush_P\t" << go(2, n/uniform);

	cout << endl;
  }
}
