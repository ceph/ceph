#include "../crush/crush.h"
using namespace crush;

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;

void make_disks(int n, int& no, vector<int>& d) 
{
  d.clear();
  while (n) {
    d.push_back(no);
    no++;
    n--;
  }
}

Bucket *make_bucket(Crush& c, vector<int>& wid, int h, int& ndisks, int& nbuckets)
{
  if (h == 0) {
    // uniform
    Hash hash(123);
    vector<int> disks;
    for (int i=0; i<wid[h]; i++)
      disks.push_back(ndisks++);
    UniformBucket *b = new UniformBucket(nbuckets--, 1, 0, 10, disks);
    b->make_primes(hash);  
    c.add_bucket(b);
    return b;
  } else {
    // mixed
    MixedBucket *b = new MixedBucket(nbuckets--, h+1);
    for (int i=0; i<wid[h]; i++) {
      Bucket *n = make_bucket(c, wid, h-1, ndisks, nbuckets);
      b->add_item(n->get_id(), n->get_weight());
    }
    c.add_bucket(b);
    return b;
  }
}

int make_hierarchy(Crush& c, vector<int>& wid, int& ndisks, int& nbuckets)
{
  Bucket *b = make_bucket(c, wid, wid.size()-1, ndisks, nbuckets);
  return b->get_id();
}



int main() 
{
  Hash h(73232313);

  // crush
  Crush c;

  // buckets
  vector<int> disks;
  int root = -1;
  int nbuckets = -1;
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
    b.add_item(-4, umb1.get_weight());
  }

  if (0) {
    int bucket = -1;
    MixedBucket *root = new MixedBucket(bucket--, 2);

    for (int i=0; i<5; i++) {
      MixedBucket *b = new MixedBucket(bucket--, 1);

      int n = 5;

      if (1) {
        // add n buckets of n disks
        for (int j=0; j<n; j++) {
          
          MixedBucket *d = new MixedBucket(bucket--, 1);
          
          make_disks(n, ndisks, disks);
          for (int k=0; k<n; k++)
            d->add_item(disks[k], 10);
          
          c.add_bucket(d);
          b->add_item(d->get_id(), d->get_weight());
        }
        
        c.add_bucket(b);
        root->add_item(b->get_id(), b->get_weight());
      } else {
        // add n*n disks
        make_disks(n*n, ndisks, disks);
        for (int k=0; k<n*n; k++)
          b->add_item(disks[k], 10);

        c.add_bucket(b);
        root->add_item(b->get_id(), b->get_weight());
      }
    }

    c.add_bucket(root);
  }


  if (1) {
    vector<int> wid;
    for (int d=0; d<5; d++)
      wid.push_back(10);
    root = make_hierarchy(c, wid, ndisks, nbuckets);
  }
  
  // rule
  int numrep = 1;

  Rule rule;
  if (0) {
    rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, -100));
    rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
  }
  if (1) {
    rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
    rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, 1, 0));
    rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));
  }

  int pg_per = 100;
  int numpg = pg_per*ndisks/numrep;
  
  vector<int> ocount(ndisks);
  cout << ndisks << " disks, " << 1-nbuckets << " buckets" << endl;
  cout << pg_per << " pgs per disk" << endl;
  cout << numpg << " logical pgs" << endl;
  cout << "numrep is " << numrep << endl;


  int place = 1000000;
  int times = place / numpg;
  if (!times) times = 1;

  cout << "looping " << times << " times" << endl;
  
  float tvar = 0;
  int tvarnum = 0;

  int x = 0;
  for (int t=0; t<times; t++) {
    vector<int> v(numrep);
    
    for (int z=0; z<ndisks; z++) ocount[z] = 0;

    for (int xx=1; xx<numpg; xx++) {
      x++;

      c.do_rule(rule, x, v);
      
      bool bad = false;
      for (int i=0; i<numrep; i++) {
        ocount[v[i]]++;
        for (int j=i+1; j<numrep; j++) {
          if (v[i] == v[j]) 
            bad = true;
        }
      }
      if (bad)
        cout << "bad set " << x << ": " << v << endl;
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
    
    tvar += var;
    tvarnum++;
  }

  tvar /= tvarnum;

  cout << "total variance " << tvar << endl;
}
