

#include "../crush/crush.h"
using namespace crush;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;

/*
ostream& operator<<(ostream& out, vector<int>& v)
{
  out << "[";
  for (int i=0; i<v.size(); i++) {
	if (i) out << " ";
	out << v[i];
  }
  out << "]";
  return out;
}
*/

void make_disks(int n, int& no, vector<int>& d) 
{
  d.clear();
  while (n) {
	d.push_back(no);
	no++;
	n--;
  }
}

int main() 
{
  Hash h(73232313);

  // crush
  Crush c;


  // buckets
  vector<int> disks;
  int ndisks = 0;
  
  if (0) {
	make_disks(12, ndisks, disks);
	UniformBucket ub1(-1, 1, 0, 30, disks);
	ub1.make_primes(h);
	cout << "ub1 primes are " << ub1.primes << endl;
	c.add_bucket(&ub1);
	
	make_disks(17, ndisks, disks);
	UniformBucket ub2(-2, 1, 0, 30, disks);
	ub2.make_primes(h);  
	cout << "ub2 primes are " << ub2.primes << endl;
	c.add_bucket(&ub2);
	
	make_disks(4, ndisks, disks);
	UniformBucket ub3(-3, 1, 0, 30, disks);
	ub3.make_primes(h);  
	cout << "ub3 primes are " << ub3.primes << endl;
	c.add_bucket(&ub3);
	
	make_disks(20, ndisks, disks);
	MixedBucket umb1(-4, 1);
	for (int i=0; i<20; i++)
	  umb1.add_item(disks[i], 30);
	c.add_bucket(&umb1);
	
	MixedBucket b(-100, 1);
	//b.add_item(-2, ub1.get_weight());
	b.add_item(-4, umb1.get_weight());
	//b.add_item(-2, ub2.get_weight());
	//b.add_item(-3, ub3.get_weight());
  }

  if (1) {
	int bucket = -1;
	MixedBucket *root = new MixedBucket(bucket--, 2);

	for (int i=0; i<5; i++) {
	  MixedBucket *b = new MixedBucket(bucket--, 1);

	  int n = 5;
	  for (int j=0; j<n; j++) {

		MixedBucket *d = new MixedBucket(bucket--, 1);

		make_disks(n, ndisks, disks);
		for (int k=0; k<n; k++)
		  d->add_item(disks[k], 10);
		
		//b->add_item(disks[j], 10);
		c.add_bucket(d);
		b->add_item(d->get_id(), d->get_weight());
	  }

	  c.add_bucket(b);
	  root->add_item(b->get_id(), b->get_weight());
	}

	c.add_bucket(root);
  }



  // rule
  int numrep = 1;

  Rule rule;
  if (0) {
	rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, -100));
	rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
	c.add_rule(numrep, rule);
  }
  if (1) {
	/*
	rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, -4));
	rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, 2, 0));
	rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));
	*/
	rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, -1));
	rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, 1, 0));
	rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));
	c.add_rule(numrep, rule);
  }

  c.overload[10] = .1;

  
  vector<int> ocount(ndisks);

  vector<int> v(numrep);
  int numo = 10000*ndisks/numrep;
  cout << "nrep is " << numrep << endl;
  cout << "placing " << numo << " logical,  " << numo*numrep << " total" << endl;
  for (int x=1; x<numo; x++) {
	//cout << H(x) << "\t" << h(x) << endl;
	c.choose(numrep, x, v);
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

  for (int i=0; i<ocount.size(); i++) {
	cout << "disk " << i << " has " << ocount[i] << endl;
  }

  cout << "collisions: " << c.collisions << endl;
  cout << "r bumps: " << c.bumps << endl;

  
  float avg = 0.0;
  for (int i=0; i<ocount.size(); i++)
	avg += ocount[i];
  avg /= ocount.size();
  float var = 0.0;
  for (int i=0; i<ocount.size(); i++)
	var += (ocount[i] - avg) * (ocount[i] - avg);
  var /= ocount.size();

  cout << "avg " << avg << "  var " << var << "   sd " << sqrt(var) << endl;


}
