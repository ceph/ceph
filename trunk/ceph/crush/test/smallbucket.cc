

#include "../crush.h"
using namespace crush;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;


Bucket *make_bucket(Crush& c, vector<int>& wid, int h, map< int, list<Bucket*> >& buckets, int& ndisks)
{
  if (h == 0) {
	// uniform
	Hash hash(123);
	vector<int> disks;
	for (int i=0; i<wid[h]; i++)
	  disks.push_back(ndisks++);
	UniformBucket *b = new UniformBucket(1, 0, 1, disks);
	b->make_primes(hash);  
	c.add_bucket(b);
	//cout << h << " uniformbucket with " << wid[h] << " disks" << endl;
	buckets[h].push_back(b);
	return b;
  } else {
	// mixed
	Bucket *b = new TreeBucket(h+1);
	c.add_bucket(b);
	for (int i=0; i<wid[h]; i++) {
	  Bucket *n = make_bucket(c, wid, h-1, buckets, ndisks);
	  b->add_item(n->get_id(), n->get_weight());
	  n->set_parent(b->get_id());
	}
	buckets[h].push_back(b);
	//cout << b->get_id() << " mixedbucket with " << wid[h] << " at " << h << endl;
	return b;
  }
}

int make_hierarchy(Crush& c, vector<int>& wid, map< int, list<Bucket*> >& buckets, int& ndisks)
{
  Bucket *b = make_bucket(c, wid, wid.size()-1, buckets, ndisks);
  return b->get_id();
}


void place(Crush& c, Rule& rule, int numpg, int numrep, vector<int>& ocount)
{
  vector<int> v(numrep);
  //map<int,int> ocount;

  for (int x=1; x<=numpg; x++) {
	
	//cout << H(x) << "\t" << h(x) << endl;
	c.do_rule(rule, x, v);
	//cout << "v = " << v << endl;// " " << v[0] << " " << v[1] << "  " << v[2] << endl;
	
	bool bad = false;
	for (int i=0; i<numrep; i++) {
	  //int d = b.choose_r(x, i, h);
	  //v[i] = d;
	  ocount[v[i]]++;
	  for (int j=i+1; j<numrep; j++) {
		if (v[i] == v[j]) 
		  bad = true;
	  }
	}
	if (bad)
	  cout << "bad set " << x << ": " << v << endl;
	
	//placement[x] = v;

	//cout << v << "\t" << ocount << endl;
  }
  

}


int main()//float testmovement(int depth, int branching, int udisks)
{
  Hash h(73232313);

  // crush
  Crush c;


  // buckets
  int root = -1;
  int ndisks = 0;
  
  vector<int> wid;
  wid.push_back(10);
  wid.push_back(2);

  map< int, list<Bucket*> > buckets;
  root = make_hierarchy(c, wid, buckets, ndisks);

  // add small bucket
  vector<int> disks;
  for (int i=0; i<3; i++)
	disks.push_back(ndisks++);
  UniformBucket *b = new UniformBucket(1, 0, 1, disks);
  b->make_primes(h);
  Bucket *o = buckets[1].back();
  c.add_bucket(b);
  //cout << " adding under " << o->get_id() << endl;
  c.add_item(o->get_id(), b->get_id(), b->get_weight());
  

  // rule
  int numrep = 6;
  Rule rule;
  rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
  rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
  rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));

  //c.overload[10] = .1;

  int pg_per = 10000;
  int numpg = pg_per*ndisks/numrep;
  
  vector<int> ocount(ndisks);

  c.print(cout, root);

  place(c, rule, numpg, numrep, ocount);
  
  for (int i=0; i<ocount.size(); i++) {
	cout << "disk " << i << " = " << ocount[i] << endl;
  }

  return 0;
}


