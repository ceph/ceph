#ifndef __INTERVAL_SET_H
#define __INTERVAL_SET_H

#include <map>
#include <ostream>
#include <cassert>
using namespace std;


template<typename T>
class interval_set {
 public:
  map<T,T> m;   // map start -> len  

  // helpers
 private:
  typename map<T,T>::iterator find_inc(T start) {
	typename map<T,T>::iterator p = m.lower_bound(start);
	if (p != m.begin() &&
		(p->first > start || p == m.end())) {
	  p--;   // might overlap?
	  if (p->first + p->second <= start)
		p++; // it doesn't.
	}
	return p;
  }
  
  typename map<T,T>::iterator find_adj(T start) {
	typename map<T,T>::iterator p = m.lower_bound(start);
	if (p != m.begin() &&
		(p->first > start || p == m.end())) {
	  p--;   // might touch?
	  if (p->first + p->second < start)
		p++; // it doesn't.
	}
	return p;
  }
  
 public:

  void clear() {
	m.clear();
  }

  bool contains(T i) {
	typename map<T,T>::iterator p = find_inc(i);
	if (p == m.end()) return false;
	if (p->first > i) return false;
	if (p->first+p->second <= i) return false;
	assert(p->first <= i && p->first+p->second > i);
	return true;
  }
  bool contains(T start, T len) {
	typename map<T,T>::iterator p = find_inc(start);
	if (p == m.end()) return false;
	if (p->first > start) return false;
	if (p->first+p->second <= start) return false;
	assert(p->first <= start && p->first+p->second > start);
	if (p->first+p->second < start+len) return false;
	return true;
  }

  // outer range of set
  bool empty() {
	return m.empty();
  }
  T start() {
	assert(!empty());
	typename map<T,T>::iterator p = m.begin();
	return p->first;
  }
  T end() {
	assert(!empty());
	typename map<T,T>::iterator p = m.end();
	p--;
	return p->first+p->second;
  }

  // interval start after p (where p not in set)
  bool starts_after(T i) {
	assert(!contains(i));
	typename map<T,T>::iterator p = find_inc(i);
	if (p == m.end()) return false;
	return true;
  }
  T start_after(T i) {
	assert(!contains(i));
	typename map<T,T>::iterator p = find_inc(i);
	return p->first;
  }

  // interval end that contains start
  T end_after(T start) {
	assert(contains(start));
	typename map<T,T>::iterator p = find_inc(start);
	return p->first+p->second;
  }
  
  void insert(T start, T len) {
	typename map<T,T>::iterator p = find_adj(start);
	if (p == m.end()) {
	  m[start] = len;                  // new interval
	} else {
	  if (p->first < start) {
		assert(p->first + p->second == start);
		p->second += len;		       // append to end
		
		typename map<T,T>::iterator n = p;
		n++;
		if (start+len == n->first) {   // combine with next, too!
		  p->second += n->second;
		  m.erase(n);
		}
	  } else {
		if (start+len == p->first) {
		  m[start] = len + p->second;  // append to front 
		  m.erase(p);
		} else {
		  assert(p->first > start+len);
		  m[start] = len;              // new interval
		}
	  }
	}
  }
  
  void erase(T start, T len) {
	typename map<T,T>::iterator p = find_inc(start);

	assert(p != m.end());
	assert(p->first <= start);

	T before = start - p->first;
	assert(p->second >= before+len);
	T after = p->second - before - len;
	
	if (before) 
	  p->second = before;        // shorten bit before
	else
	  m.erase(p);
	if (after)
	  m[start+len] = after;
  }


  void subtract(interval_set &a) {
	for (typename map<T,T>::iterator p = a.m.begin();
		 p != a.m.end();
		 p++)
	  erase(p->first, p->second);
  }

  void insert(interval_set &a) {
	for (typename map<T,T>::iterator p = a.m.begin();
		 p != a.m.end();
		 p++)
	  insert(p->first, p->second);
  }


  void intersection_of(interval_set &a, interval_set &b) {
	typename map<T,T>::iterator pa = a.m.begin();
	typename map<T,T>::iterator pb = b.m.begin();
	
	while (pa != a.m.end() && pb != b.m.end()) {
	  // passing?
	  if (pa->first + pa->second <= pb->first) 
		{ pa++;  continue; }
	  if (pb->first + pb->second <= pa->first) 
		{ pb++;  continue; }
	  T start = MAX(pa->first, pb->first);
	  T end = MIN(pa->first+pa->second, pb->first+pb->second);
	  assert(end > start);
	  insert(start, end-start);
	  pa++; 
	  pb++;
	}
  }

  void union_of(interval_set &a, interval_set &b) {
	typename map<T,T>::iterator pa = a.m.begin();
	typename map<T,T>::iterator pb = b.m.begin();
	
	while (pa != a.m.end() || pb != b.m.end()) {
	  // passing?
	  if (pb == b.m.end() || pa->first + pa->second <= pb->first) {
		insert(pa->first, pa->second);
		pa++;  continue; 
	  }
	  if (pa == a.m.end() || pb->first + pb->second <= pa->first) {
		insert(pb->first, pb->second);
		pb++;  continue; 
	  }
	  T start = MIN(pa->first, pb->first);
	  T end = MAX(pa->first+pa->second, pb->first+pb->second);
	  insert(start, end-start);
	  pa++; 
	  pb++;
	}
  }
  void union_of(interval_set &a) {
	interval_set b;
	b.m.swap(m);
	union_of(a, b);
  }

  bool subset_of(interval_set &big) {
	for (typename map<T,T>::iterator i = m.begin();
		 i != m.end();
		 i++) 
	  if (!big.contains(i->first, i->second)) return false;
	return true;
  }  
  
};

template<class T>
inline ostream& operator<<(ostream& out, interval_set<T> &s) {
  out << "[";
  for (typename map<T,T>::iterator i = s.m.begin();
	   i != s.m.end();
	   i++) {
	if (i != s.m.begin()) out << ",";
	out << i->first << "~" << i->second;
  }
  out << "]";
  return out;
}


#endif
