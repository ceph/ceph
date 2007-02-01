

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
	MixedBucket *b = new MixedBucket(h+1);
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


void place(Crush& c, Rule& rule, int numpg, int numrep, map<int, set<int> >& placement)
{
  vector<int> v(numrep);
  map<int,int> ocount;

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
	  placement[v[i]].insert(x);
	}
	if (bad)
	  cout << "bad set " << x << ": " << v << endl;
	

	//cout << v << "\t" << ocount << endl;
  }
  
  if (0) 
	for (map<int,int>::iterator it = ocount.begin();
		 it != ocount.end();
		 it++) 
	  cout << it->first << "\t" << it->second << endl;

}


float testmovement(int depth, int branching, int udisks)
{
  Hash h(73232313);

  // crush
  Crush c;


  // buckets
  int root = -1;
  int ndisks = 0;
  
  vector<int> wid;
  wid.push_back(udisks);
  for (int d=1; d<depth; d++)
	wid.push_back(branching);

  map< int, list<Bucket*> > buckets;

  if (1) {
	root = make_hierarchy(c, wid, buckets, ndisks);
  }
  if (0) {
	MixedBucket *b = new MixedBucket(1);
	for (int i=0; i<10000; i++)
	  b->add_item(ndisks++, 10);
	root = c.add_bucket(b);
  }
  if (0) {
	vector<int> disks;
	for (int i=0; i<10000; i++)
	  disks.push_back(ndisks++);
	UniformBucket *b = new UniformBucket(1, 0, 1, disks);
	Hash h(123);
	b->make_primes(h);
	root = c.add_bucket(b);
  }
  


  // rule
  int numrep = 2;
  Rule rule;
  rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
  rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
  rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));

  //c.overload[10] = .1;


  int pg_per = 100;
  int numpg = pg_per*ndisks/numrep;
  
  vector<int> ocount(ndisks);

  /*
  cout << ndisks << " disks, " << endl;
  cout << pg_per << " pgs per disk" << endl;
    cout << numpg << " logical pgs" << endl;
  cout << "numrep is " << numrep << endl;
  */
  map<int, set<int> > placement1, placement2;

  //c.print(cout, root);

  place(c, rule, numpg, numrep, placement1);

  float over = .5;
  
  if (1) {
	// failed

	//for (int i=500; i<1000; i++)
	//c.failed.insert(i);
	//c.failed.insert(0);
	c.overload[0] = over;
  }

  int olddisks = ndisks;



  if (0) {
	int n = udisks;
	//cout << " adding " << n << " disks" << endl;
	vector<int> disks;
	for (int i=0; i<n; i++)
	  disks.push_back(ndisks++);
	UniformBucket *b = new UniformBucket(1, 0, 1, disks);
	Hash h(123);
	b->make_primes(h);
	Bucket *o = buckets[1].back();
	c.add_bucket(b);
	//cout << " adding under " << o->get_id() << endl;
	c.add_item(o->get_id(), b->get_id(), b->get_weight());
	//((MixedBucket*)o)->add_item(b->get_id(), b->get_weight());
  }

  //c.print(cout, root);
  place(c, rule, numpg, numrep, placement2);

  vector<int> moved(ndisks);

  //int moved = 0;
  for (int d=0; d<ndisks; d++) {
	for (set<int>::iterator it = placement1[d].begin();
		 it != placement1[d].end();
		 it++) {
	  placement2[d].erase(*it);
	}
  }

  float avg = 0;
  for (int d=0; d<ndisks; d++) {
	moved[d] = placement2[d].size();
	avg += moved[d];
  }
  avg /= (float)ndisks;
  float var = 0;
  for (int d=0; d<ndisks; d++) {
	var += (moved[d]-avg)*(moved[d]-avg);
  }
  var /= (float)ndisks;

  float expected = over * 100.0 / (float)(ndisks-1);

  cout << ndisks << "\t" << expected << "\t" << avg << "\t" << var << endl;
  /*
  float f = (float)moved / (float)(numpg*numrep);
  float ideal = (float)(ndisks-olddisks) / (float)(ndisks);
  float fac = f/ideal;
  //cout << moved << " moved or " << f << ", ideal " << ideal << ", factor of " << fac <<  endl;
  return fac;
  */
}


int main() 
{
  
  int udisks = 10;
  int ndisks = 10;
  for (int depth = 2; depth <= 4; depth++) {
	vector<float> v;
	cout << depth;
	for (int branching = 3; branching < 16; branching += 1) {
	  float fac = testmovement(depth, branching, udisks);
	  v.push_back(fac);
	  int n = udisks * pow((float)branching, (float)depth-1);
	  //cout << "\t" << n;
	  //cout << "\t" << fac;
	}
	//for (int i=0; i<v.size(); i++)
	//cout << "\t" << v[i];
	//cout << endl;

  }

}

