

#include "../crush.h"
using namespace crush;

#include "../../common/Clock.h"

#include <math.h>

#include <iostream>
#include <vector>
using namespace std;


Clock g_clock;


Bucket *make_bucket(Crush& c, vector<int>& wid, int h, int& ndisks)
{
  if (h == 0) {
	// uniform
	Hash hash(123);
	vector<int> disks;
	for (int i=0; i<wid[h]; i++)
	  disks.push_back(ndisks++);
	float w = ((ndisks-1)/100+1)*10;
	UniformBucket *b = new UniformBucket(1, 0, w, disks);
	//b->make_primes(hash);  
	c.add_bucket(b);
	//cout << h << " uniformbucket with " << wid[h] << " disks weight " << w << endl;
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



float go(int dep, int utilization ) 
{
  Hash h(73232313);

  int overloadcutoff = (int)((float)10000.0 / (float)utilization);

  //cout << "util " << utilization << " cutoff " << overloadcutoff << endl;
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

  //cout << ndisks << " disks" << endl;
  


  // rule
  int numrep = 1;
  Rule rule;
  rule.steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
  rule.steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, numrep, 0));
  rule.steps.push_back(RuleStep(CRUSH_RULE_EMIT));

  //c.overload[10] = .1;

  int pg_per_base = 20;
  int pg_med = 10*pg_per_base;
  int pg_per = pg_per_base*5.5;//100;
  int numpg = pg_per*ndisks/numrep;
  
  vector<int> ocount(ndisks);
  //cout << ndisks << " disks, " << endl;
  //cout << pg_per << " pgs per disk" << endl;
  //  cout << numpg << " logical pgs" << endl;
  //cout << "numrep is " << numrep << endl;


  int place = 100000;
  int times = place / numpg;
  if (!times) times = 1;
  

  //cout << "looping " << times << " times" << endl;
  
  float tavg[10];
  float tvar[10];
  for (int j=0;j<10;j++) {
	tvar[j] = 0;
	tavg[j] = 0;
  }
  int tvarnum = 0;

  float overloadsum = 0.0;
  float adjustsum = 0.0;
  float afteroverloadsum = 0.0;
  float aslowdown = 0.0;
  int chooses = 0;
  int xs = 1;
  for (int t=0; t<times; t++) {
	vector<int> v(numrep);
	
	c.overload.clear();

	for (int z=0; z<ndisks; z++) ocount[z] = 0;

	
	utime_t t1a = g_clock.now();

	for (int x=xs; x<numpg+xs; x++) {

	  //cout << H(x) << "\t" << h(x) << endl;
	  c.do_rule(rule, x, v);
	  chooses += numrep;
	  //cout << "v = " << v << endl;// " " << v[0] << " " << v[1] << "  " << v[2] << endl;
	  
	  for (int i=0; i<numrep; i++) {
		//int d = b.choose_r(x, i, h);
		//v[i] = d;
		ocount[v[i]]++;
	  }
	  
	  //cout << v << "\t" << ocount << endl;
	}

	utime_t t1b = g_clock.now();

	// overloaded?
	int overloaded = 0;
	int adjusted = 0;
	for (int i=0; i<ocount.size(); i++) {
	  int target = (i/100+1)*pg_per_base;
	  int cutoff = target * overloadcutoff / 100;
	  int adjoff = target + (cutoff - target)*3/4;
	  if (ocount[i] > cutoff) 
		overloaded++;

	  if (ocount[i] > adjoff) {
		adjusted++;
		c.overload[i] = (float)target / (float)ocount[i];
		//cout << "setting overload " << i << " to " << c.overload[i] << endl;
		//cout << "disk " << i << " has " << ocount[i] << endl;
	  }
	  ocount[i] = 0;
	}
	//cout << overloaded << " overloaded" << endl;
	overloadsum += (float)overloaded / (float)ndisks;
	adjustsum += (float)adjusted / (float)ndisks;



	// keep adjusting!
	for (int bla=0; bla<5; bla++) {
	  utime_t t2a = g_clock.now();

	  // second pass
	  for (int x=xs; x<numpg+xs; x++) {
		
		//cout << H(x) << "\t" << h(x) << endl;
		c.do_rule(rule, x, v);
		//cout << "v = " << v << endl;// " " << v[0] << " " << v[1] << "  " << v[2] << endl;
		
		for (int i=0; i<numrep; i++) {
		  //int d = b.choose_r(x, i, h);
		  //v[i] = d;
		  ocount[v[i]]++;
		}
		
		//cout << v << "\t" << ocount << endl;
	  }

	  utime_t t2b = g_clock.now();

	  int numover = 0;
	  for (int i=0; i<ocount.size(); i++) {
		int target = (i/100+1)*pg_per_base;
		int cutoff = target * overloadcutoff / 100;
		int adjoff = cutoff;//target + (cutoff - target)*3/4;

		if (ocount[i] >= adjoff) {
		  numover++;
		  if (c.overload.count(i) == 0) {
			c.overload[i] = 1.0;
			adjusted++;
		  }
		  //else cout << "(re)adjusting " << i << endl;
		  c.overload[i] *= (float)target / (float)ocount[i];
		  //cout << "setting overload " << i << " to " << c.overload[i] << endl;
		  //cout << "disk " << i << " has " << ocount[i] << endl;
		}
		ocount[i] = 0;
	  }
	  if (!numover) break;
	  cout << "readjusting" << endl;
	}

	utime_t t3a = g_clock.now();

	for (int x=xs; x<numpg+xs; x++) {

	  //cout << H(x) << "\t" << h(x) << endl;
	  c.do_rule(rule, x, v);
	  //cout << "v = " << v << endl;// " " << v[0] << " " << v[1] << "  " << v[2] << endl;
	  
	  for (int i=0; i<numrep; i++) {
		//int d = b.choose_r(x, i, h);
		//v[i] = d;
		ocount[v[i]]++;
	  }
	  
	  //cout << v << "\t" << ocount << endl;
	}
	xs += numpg;

	utime_t t3b = g_clock.now();

	t1b -= t1a;
	double t1 = (double)t1b;
	t3b -= t3a;
	double t3 = (double)t3b;
	double slowdown = t3/t1;
	//cout << "slowdown " << slowdown << endl;
	aslowdown += slowdown;

	int still = 0;
	for (int i=0; i<ocount.size(); i++) {
	  int target = (i/100+1)*pg_per_base;
	  int cutoff = target * overloadcutoff / 100;
	  //int adjoff = target + (cutoff - target)/3;

	  if (ocount[i] > cutoff) {
		still++;
		//c.overload[ocount[i]] = 100.0 / (float)ocount[i];
		if (c.overload.count(i)) cout << "[adjusted] ";
		cout << "disk " << i << " has " << ocount[i] << endl;
	  }
	}
	//if (still) cout << "overload was " << overloaded << " now " << still << endl;
	afteroverloadsum += (float)still / (float)ndisks;
	
	//cout << "collisions: " << c.collisions << endl;
	//cout << "r bumps: " << c.bumps << endl;
	
	int n = ndisks/10;
	float avg[10];
	float var[10];
	for (int i=0;i<10;i++) {
	  int s = n*i;
	  avg[i] = 0.0;
	  for (int j=0; j<n; j++)
		avg[i] += ocount[j+s];
	  avg[i] /= n;//ocount.size();
	  var[i] = 0.0;
	  for (int j=0; j<n; j++)
		var[i] += (ocount[j+s] - avg[i]) * (ocount[j+s] - avg[i]);
	  var[i] /= n;//ocount.size();

	  tvar[i] += var[i];
	  tavg[i] += avg[i];
	}
	//cout << "avg " << avg << "  var " << var << "   sd " << sqrt(var) << endl;
	
	tvarnum++;
  }

  overloadsum /= tvarnum;
  adjustsum /= tvarnum;
  float avar = 0.0;
  for (int j=0;j<10;j++) {
	tvar[j] /= tvarnum;
	tavg[j] /= tvarnum;
	avar += tvar[j];
  }
  avar /= 10;
  avar = sqrt(avar);
  avar /= 5.5 * (float)pg_per_base;
  afteroverloadsum /= tvarnum;
  aslowdown /= tvarnum;

  //int collisions = c.collisions[0] + c.collisions[1] + c.collisions[2] + c.collisions[3];
  //float crate = (float) collisions / (float)chooses;
  //cout << "collisions: " << c.collisions << endl;


  //cout << "total variance " << tvar << endl;
  //cout << " overlaod " << overloadsum << endl;
  
  cout << overloadcutoff << "\t" << utilization 
	   << "\t" << overloadsum << "\t" << adjustsum << "\t" << afteroverloadsum 
	   << "\t" << aslowdown << "\t" << avar << "\t-"; 

  for (int i=0;i<10;i++)
	cout << "\t" << tavg[i] << "\t" << tvar[i];// << "\t" << tvar[i]/tavg[i];
  cout << endl;
  return tvar[0];
}


int main() 
{
  float var = go(3,50);
  /*  for (int d=70; d<100; d += 5) {
	float var = go(3,d);
	//cout << "## depth = " << d << endl;
	//cout << d << "\t" << var << endl;
	}*/
  go(3,96);
  go(3,97);
  go(3,98);
  go(3,99);
  

}
