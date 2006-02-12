// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

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
  
  unsigned get_width() {
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
	for (unsigned i=0; i<p.size(); i++) {
	  p[i] = (float)(rand() % 10000);
	  sum += p[i];
	}
	for (unsigned i=0; i<p.size(); i++) 
	  p[i] /= sum;
  }

  int sample() {
	float s = (float)(rand() % 10000) / 10000.0;
	for (unsigned i=0; i<p.size(); i++) {
	  if (s < p[i]) return v[i];
	  s -= p[i];
	}
	assert(0);
	return v[p.size() - 1];  // hmm.  :/
  }

  float normalize() {
	float s = 0.0;
	for (unsigned i=0; i<p.size(); i++)
	  s += p[i];
	for (unsigned i=0; i<p.size(); i++)
	  p[i] /= s;
	return s;
  }

};

#endif
