

#include "../crush.h"
using namespace crush;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;


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
	//if (bad)
	// cout << "bad set " << x << ": " << v << endl;
	
	placement[x] = v;

	//cout << v << "\t" << ocount << endl;
  }
  
  if (0) 
	for (map<int,int>::iterator it = ocount.begin();
		 it != ocount.end();
		 it++) 
	  cout << it->first << "\t" << it->second << endl;

}


float testmovement(int n, float f, int buckettype)
{
  Hash h(73232313);

  // crush
  Crush c;

  int ndisks = 0;

  // bucket
  Bucket *b;
  if (buckettype == 0)
	b = new TreeBucket(1);
  else if (buckettype == 1 || buckettype == 2)
	b = new ListBucket(1);
  else if (buckettype == 3)
	b = new StrawBucket(1);
  else if (buckettype == 4)
	b = new UniformBucket(0,0);

  for (int i=0; i<n; i++)
	b->add_item(ndisks++,1);

  c.add_bucket(b);
  int root = b->get_id();
  
  //c.print(cout,root);

  // rule
  int numrep = 2;
  Rule rule;
  rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
  rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
  rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));

  //c.overload[10] = .1;


  int pg_per = 1000;
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

  
  // ORIGINAL
  place(c, rule, numpg, numrep, placement1);
  
  int olddisks = ndisks;

  // add item
  if (buckettype == 2) {
	// start over!
	ndisks = 0;
	b = new ListBucket(1);
	for (int i=0; i<=n; i++)
	  b->add_item(ndisks++,1);
	c.add_bucket(b);
	root = b->get_id();

	rule.steps.clear();
	rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
	rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
	rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));

  }
  else
	b->add_item(ndisks++, 1);


  // ADDED
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
  float ideal = (float)(ndisks-olddisks) / (float)(ndisks);
  float fac = actual/ideal;
  //cout << add << "\t" << olddisks << "\t" << ndisks << "\t" << moved << "\t" << total << "\t" << actual << "\t" << ideal << "\t" << fac << endl;
  cout << "\t" << fac;
  return fac;
}


int main() 
{
  //cout << "//  " << depth << ",  modifydepth " << modifydepth << ",  branching " << branching << ",  disks " << n << endl;
  cout << "n\ttree\tlhead\tltail\tstraw\tuniform" << endl;

  //for (int s=2; s<=64; s+= (s<4?1:(s<16?2:4))) {
  for (int s=2; s<=64; s+= (s<4?1:4)) {
	float f = 1.0 / (float)s;
	//cout << f << "\t" << s;
	cout << s;
	for (int buckettype=0; buckettype<5; buckettype++)
	  testmovement(s, f, buckettype);
	cout << endl;
  }
}

