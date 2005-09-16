

#include "../crush/crush.h"
using namespace crush;

#include <iostream>
#include <vector>
using namespace std;


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
  Hash h(73);
  int numrep = 5;


  // buckets
  vector<int> disks;
  int ndisks = 0;
  
  make_disks(12, ndisks, disks);
  UniformBucket ub1(1, 1, 0, 30, disks);
  ub1.make_primes(h);
  cout << "ub1 primes are " << ub1.primes << endl;
  
  make_disks(17, ndisks, disks);
  UniformBucket ub2(2, 1, 0, 30, disks);
  ub2.make_primes(h);  
  cout << "ub2 primes are " << ub2.primes << endl;

  make_disks(4, ndisks, disks);
  UniformBucket ub3(3, 1, 0, 30, disks);
  ub3.make_primes(h);  
  cout << "ub3 primes are " << ub3.primes << endl;

  MixedBucket b(100, 1);
  b.add_item(1, ub1.get_weight());
  b.add_item(2, ub2.get_weight());
  b.add_item(3, ub3.get_weight());

  // rule
  Rule rule;
  rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, 100));
  rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
  CRule crule(numrep);
  crule.nchoose = 2;
  for (int j=0; j<numrep; j++) {
	crule.steps[j].push_back(RuleStep(CRULE_TAKEB, 100));
	crule.steps[j].push_back(RuleStep(CRULE_CHOOSER, j, numrep, 1));
	crule.steps[j].push_back(RuleStep(CRULE_CHOOSER, j, numrep, 0));
  }

  // crush
  Crush c;
  c.add_bucket(&ub1);
  c.add_bucket(&ub2);
  c.add_bucket(&ub3);
  c.add_bucket(&b);
  c.add_rule(numrep, rule);
  c.add_crule(numrep, crule);


  
  vector<int> ocount(ndisks);

  vector<int> v(numrep);
  int numo = 100000*ndisks/numrep;
  cout << "placing " << numo << " logical,  " << numo*numrep << " total" << endl;
  for (int x=1; x<numo; x++) {
	//cout << H(x) << "\t" << h(x) << endl;
	c.crule_choose(numrep, x, v);
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

}
