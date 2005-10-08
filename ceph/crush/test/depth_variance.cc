

#include "../crush.h"
using namespace crush;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;


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
	Bucket *b = new TreeBucket(h+1);
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


float go(int dep) 
{
  Hash h(73232313);

  // crush
  Crush c;


  // buckets
  int root = -1;
  int ndisks = 0;

  vector<int> wid;
  if (1) {
	for (int d=0; d<dep; d++)
	  wid.push_back(10);
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
  int numrep = 1;
  Rule rule;
  rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
  rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
  rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));

  //c.overload[10] = .1;


  int pg_per = 100;
  int numpg = pg_per*ndisks/numrep;
  
  vector<int> ocount(ndisks);
  //cout << ndisks << " disks, " << endl;
  //cout << pg_per << " pgs per disk" << endl;
  //  cout << numpg << " logical pgs" << endl;
  //cout << "numrep is " << numrep << endl;


  int place = 100000;
  int times = place / numpg;
  if (!times) times = 1;

  cout << "#looping " << times << " times" << endl;
  
  float tvar = 0;
  int tvarnum = 0;
  float tavg = 0;

  int x = 0;
  for (int t=0; t<times; t++) {
	vector<int> v(numrep);
	
	for (int z=0; z<ndisks; z++) ocount[z] = 0;

	for (int xx=1; xx<numpg; xx++) {
	  x++;

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
	  
	  //cout << v << "\t" << ocount << endl;
	}
	
	/*
	  for (int i=0; i<ocount.size(); i++) {
	  cout << "disk " << i << " has " << ocount[i] << endl;
	  }
	*/
	
	//cout << "collisions: " << c.collisions << endl;
	//cout << "r bumps: " << c.bumps << endl;
	
	
	float avg = 0.0;
	for (int i=0; i<ocount.size(); i++)
	  avg += ocount[i];
	avg /= ocount.size();
	float var = 0.0;
	for (int i=0; i<ocount.size(); i++)
	  var += (ocount[i] - avg) * (ocount[i] - avg);
	var /= ocount.size();
	
	if (times < 10) 
	  cout << "avg " << avg << "   evar " << sqrt(avg) << "   sd " << sqrt(var) << endl;
	//cout << avg << "\t";
	
	tvar += var;
	tavg += avg;
	tvarnum++;
  }

  tavg /= tvarnum;
  tvar /= tvarnum;

  cout << "total variance " << sqrt(tvar) << "   expected " << sqrt(tavg) << endl;

  return tvar;
}


int main() 
{
  for (int d=2; d<=5; d++) {
	float var = go(d);
	//cout << "## depth = " << d << endl;
	//cout << d << "\t" << var << "\t" << sqrt(var) << endl;
  }
}
