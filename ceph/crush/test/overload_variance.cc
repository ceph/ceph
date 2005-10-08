

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
	b->make_primes(hash);  
	c.add_bucket(b);
	//cout << h << " uniformbucket with " << wid[h] << " disks" << endl;
	return b;
  } else {
	// mixed
	MixedBucket *b = new MixedBucket(h+1);
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


Bucket *make_random(Crush& c, int wid, int height, int& ndisks)
{
  int w = rand() % (wid-1) + 2;

  if (height == 0) {
	// uniform
	Hash hash(123);
	vector<int> disks;
	for (int i=0; i<w; i++)
	  disks.push_back(ndisks++);
	UniformBucket *b = new UniformBucket(1, 0, 10, disks);
	b->make_primes(hash);  
	c.add_bucket(b);
	//cout << h << " uniformbucket with " << wid[h] << " disks" << endl;
	return b;
  } else {
	// mixed
	int h = rand() % height + 1;
	MixedBucket *b = new MixedBucket(h+1);
	for (int i=0; i<w; i++) {
	  Bucket *n = make_random(c, wid, h-1, ndisks);
	  b->add_item(n->get_id(), n->get_weight());
	}
	c.add_bucket(b);
	//cout << h << " mixedbucket with " << wid[h] << endl;
	return b;
  }

}


float go(int dep, int overloadcutoff) 
{
  Hash h(73232313);

  // crush
  Crush c;


  // buckets
  int root = -1;
  int ndisks = 0;

  vector<int> wid;
  for (int d=0; d<dep; d++)
	wid.push_back(10);

  if (1) {
	root = make_hierarchy(c, wid, ndisks);
  }
  if (0) {
	Bucket *r = make_random(c, 20,  4, ndisks);
	root = r->get_id();
	//c.print(cout, root);
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
	UniformBucket *b = new UniformBucket(1, 0, 10000, disks);
	Hash h(123);
	b->make_primes(h);
	root = c.add_bucket(b);
  }
  //cout << ndisks << " disks" << endl;
  


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


  int place = 1000000;
  int times = place / numpg;
  if (!times) times = 1;
  

  //cout << "looping " << times << " times" << endl;
  
  float tvar = 0;
  int tvarnum = 0;

  float overloadsum = 0.0;
  float adjustsum = 0.0;
  float afteroverloadsum = 0.0;
  int chooses = 0;
  int xs = 1;
  for (int t=0; t<times; t++) {
	vector<int> v(numrep);
	
	c.overload.clear();

	for (int z=0; z<ndisks; z++) ocount[z] = 0;

	for (int x=xs; x<numpg+xs; x++) {

	  //cout << H(x) << "\t" << h(x) << endl;
	  c.do_rule(rule, x, v);
	  chooses += numrep;
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
	  
	  //cout << v << "\t" << ocount << endl;
	}

	// overloaded?
	int overloaded = 0;
	int adjusted = 0;
	for (int i=0; i<ocount.size(); i++) {
	  if (ocount[i] > overloadcutoff) 
		overloaded++;

	  if (ocount[i] > 100+(overloadcutoff-100)/2) {
		adjusted++;
		c.overload[i] = 100.0 / (float)ocount[i];
		//cout << "disk " << i << " has " << ocount[i] << endl;
	  }
	  ocount[i] = 0;
	}
	//cout << overloaded << " overloaded" << endl;
	overloadsum += (float)overloaded / (float)ndisks;
	adjustsum += (float)adjusted / (float)ndisks;


	for (int x=xs; x<numpg+xs; x++) {

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
	  
	  //cout << v << "\t" << ocount << endl;
	}
	xs += numpg;

	int still = 0;
	for (int i=0; i<ocount.size(); i++) {
	  if (ocount[i] > overloadcutoff) {
		still++;
		//c.overload[ocount[i]] = 100.0 / (float)ocount[i];
		//cout << "disk " << i << " has " << ocount[i] << endl;
	  }
	}
	//if (still) cout << "overload was " << overloaded << " now " << still << endl;
	afteroverloadsum += (float)still / (float)ndisks;
	
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
	
	//cout << "avg " << avg << "  var " << var << "   sd " << sqrt(var) << endl;
	
	tvar += var;
	tvarnum++;
  }

  overloadsum /= tvarnum;
  adjustsum /= tvarnum;
  tvar /= tvarnum;
  afteroverloadsum /= tvarnum;

  int collisions = c.collisions[0] + c.collisions[1] + c.collisions[2] + c.collisions[3];
  float crate = (float) collisions / (float)chooses;
  //cout << "collisions: " << c.collisions << endl;


  //cout << "total variance " << tvar << endl;
  //cout << " overlaod " << overloadsum << endl;

  cout << overloadcutoff << "\t" << (10000.0 / (float)overloadcutoff) << "\t" << tvar << "\t" << overloadsum << "\t" << adjustsum << "\t" << afteroverloadsum << "\t" << crate << endl;
  return tvar;
}


int main() 
{
  for (int d=140; d>100; d -= 5) {
	float var = go(3,d);
	//cout << "## depth = " << d << endl;
	//cout << d << "\t" << var << endl;
  }
}
