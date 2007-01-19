

#include "../crush.h"
using namespace crush;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;


int buckettype = 2;  // 0 = mixed, 1 = linear, 2 = straw

int big_one_skip = 255;
int big_one_size;
Bucket *big_one = 0;

Bucket *make_bucket(Crush& c, vector<int>& wid, int h, map< int, list<Bucket*> >& buckets, int& ndisks)
{
  if (h == 0) {
	// uniform
	Hash hash(123);
	vector<int> disks;
	
	int s = wid[h];
	if (big_one_skip > 0) 
	  big_one_skip--;	  
	if (!big_one_skip && !big_one)
	  s = big_one_size;


	for (int i=0; i<s; i++)
	  disks.push_back(ndisks++);
	UniformBucket *b = new UniformBucket(1, 0, 1, disks);
	if (!big_one_skip && !big_one) big_one = b;
	b->make_primes(hash);  
	c.add_bucket(b);
	//cout << h << " uniformbucket with " << wid[h] << " disks " << disks.size()<< endl;
	buckets[h].push_back(b);
	return b;
  } else {
	// mixed
	Bucket *b;
	if (buckettype == 0)
	  b = new TreeBucket(h+1);
	else if (buckettype == 1)
	  b = new ListBucket(h+1);
	else if (buckettype == 2)
	  b = new StrawBucket(h+1);
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


void place(Crush& c, Rule& rule, int numpg, int numrep, map<int, vector<int> >& placement)
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
	}
	if (bad)
	  cout << "bad set " << x << ": " << v << endl;
	
	placement[x] = v;

	//cout << v << "\t" << ocount << endl;
  }
  
  if (0) 
	for (map<int,int>::iterator it = ocount.begin();
		 it != ocount.end();
		 it++) 
	  cout << it->first << "\t" << it->second << endl;

}


float testmovement(int depth, int branching, int udisks, int add)
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
	wid.push_back(branching + ((d==2)?1:0));

  map< int, list<Bucket*> > buckets;

  big_one_size = add;
  big_one = 0;
  
  //cout << "making tree" << endl;
  root = make_hierarchy(c, wid, buckets, ndisks);
  
  //c.print(cout, root);


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
  map<int, vector<int> > placement1, placement2;

  //c.print(cout, root);

  int olddisks = ndisks;


  place(c, rule, numpg, numrep, placement1);
  
  if (1) {
	// remove disks
	assert(big_one);
	c.adjust_item(big_one->get_id(), 0);
  }

  int newdisks = ndisks - add;

  //c.print(cout, root);
  place(c, rule, numpg, numrep, placement2);

  int moved = 0;
  for (int x=1; x<=numpg; x++) 
	if (placement1[x] != placement2[x]) 
	  for (int j=0; j<numrep; j++)
		if (placement1[x][j] != placement2[x][j]) 
		  moved++;

  int total = numpg*numrep;
  float actual = (float)moved / (float)(total);
  //float ideal = (float)(newdisks-olddisks) / (float)(ndisks);
  float ideal = (float)(olddisks-newdisks) / (float)(olddisks);
  float fac = actual/ideal;
  cout << add << "\t" << olddisks << "\t" << ndisks << "\t" << moved << "\t" << total << "\t" << actual << "\t" << ideal << "\t" << fac << endl;
  return fac;
}


int main() 
{
  
  int udisks = 10;
  int ndisks = 10;
  int depth = 4;
  int branching = 9;
  int add = udisks;

  //cout << "\t" << n;
  //  cout << endl;

  buckettype = 2;  // 0 = tree, 1 = linear, 2 = straw

  int n = udisks * pow((float)branching, (float)depth-1);
  for (int add = udisks; add <= n; add *= 3) {
	big_one_skip = 0;
	big_one_skip = 9;
	testmovement(depth, branching, udisks, add);
  }
  
  /*
  cout << "##" << endl;
  for (map<int, map<float,float> >::iterator i = r.begin();
	   i != r.end();
	   i++) {
	cout << i->first;
	for (map<float,float>::iterator j = i->second.begin();
		 j != i->second.end();
		 j++)
	  cout << "\t" << j->first << "\t" << j->second;
	cout << endl;
  }
  */
}

