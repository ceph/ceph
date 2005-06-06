#ifndef __DISTRIBUTION_H
#define __DISTRIBUTION_H

#include <cassert>
#include <vector>
using namespace std;

class Distribution {
  vector<float> p;
  vector<int> v;

 public:
  //Distribution() { 
  //}
  
  int get_width() {
	return p.size();
  }

  void clear() {
	p.clear();
	v.clear();
  }
  void add(int val, float pr) {
	p.push_back(pr);
	v.push_back(val);
  }

  void random() {
	float sum = 0.0;
	for (int i=0; i<p.size(); i++) {
	  p[i] = (float)(rand() % 10000);
	  sum += p[i];
	}
	for (int i=0; i<p.size(); i++) 
	  p[i] /= sum;
  }

  int sample() {
	float s = (float)(rand() % 10000) / 10000.0;
	for (int i=0; i<p.size(); i++) {
	  if (s < p[i]) return v[i];
	  s -= p[i];
	}
	assert(0);
	return v[p.size() - 1];  // hmm.  :/
  }

  float normalize() {
	float s = 0.0;
	for (int i=0; i<p.size(); i++)
	  s += p[i];
	for (int i=0; i<p.size(); i++)
	  p[i] /= s;
	return s;
  }

};

#endif
