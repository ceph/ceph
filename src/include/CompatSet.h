// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __CEPH_COMPATSET_H
#define __CEPH_COMPATSET_H
#include "include/buffer.h"
#include <vector>

struct CompatSet {
  set<string> compat, ro_compat, incompat;
  vector<set<string> *> all_features;
  
  CompatSet(const char **compat_a, int c_size,
	    const char **ro_compat_a, int ro_size,
	    const char **incompat_a, int inc_size) :
    compat(), ro_compat(), incompat(){
    for (int i = 0; i < c_size; ++i)
      compat.insert(string(compat_a[i]));
    for (int j = 0; j < ro_size; ++j)
      ro_compat.insert(string(ro_compat_a[j]));
    for (int k = 0; k < inc_size; ++k)
      incompat.insert(string(incompat_a[k]));
    all_features.push_back(&compat);
    all_features.push_back(&ro_compat);
    all_features.push_back(&incompat);
  }
  
  CompatSet() : compat(), ro_compat(), incompat() {
    all_features.push_back(&compat);
    all_features.push_back(&ro_compat);
    all_features.push_back(&incompat);
  }
  
  /* does this filesystem implementation have the
     features required to read the other? */
  bool readable(CompatSet other) {
    //make sure other's incompat string is a subset of ours
    for ( set<string>::iterator i = other.incompat.begin();
	  i != other.incompat.end();
	  ++i) {
      if (!incompat.count(*i)) return false; //we don't have that flag!
    }
    return true;
  }
  
  /* does this filesystem implementation have the
     features required to write the other? */
  bool writeable(CompatSet other) {
    if (!readable(other)) return false;
    for (set<string>::iterator i = other.ro_compat.begin();
	 i != other.ro_compat.end();
	 ++i) {
      if (!ro_compat.count(*i))	return false;
    }
    return true;
  }
  
  /* Compare this CompatSet to another.
   * CAREFULLY NOTE: This operation is NOT commutative.
   * a > b DOES NOT imply that b < a.
   * If returns:
   * 0: The CompatSets have the same feature set.
   * 1: This CompatSet's features are a strict superset of the other's.
   * -1: This CompatSet is missing at least one feature
   *     described in the other. It may still have more features, though.
   */
  int compare(CompatSet other) {
    //check sizes, that's fast and easy
    if (other.compat.size() > compat.size() ||
	other.ro_compat.size() > ro_compat.size() ||
	other.incompat.size() > incompat.size() ) {
      return -1;
    }
    //well, we have at least as many features, let's compare them all
    if (!writeable(other)) return -1; //compares ro_compat and incompat
    for (set<string>::iterator i = other.compat.begin();
	 i != other.compat.end();
	 ++i) {
      if (!compat.count(*i)) return -1;
    }
    //if we make it this far we have all the features other does
    //do we have more?
    if (other.compat.size() < compat.size() ||
	other.ro_compat.size() < ro_compat.size() ||
	other.incompat.size() < incompat.size() ) {
      return 1;
    }
    //apparently we have the exact same feature set
    return 0;
  }
  
  /* Get the features supported by other CompatSet but not this one,
   * as a CompatSet.
   */
  CompatSet unsupported(CompatSet other) {
    CompatSet difference;
    for (set<string>::iterator i = other.compat.begin();
	 i != other.compat.end();
	 ++i) {
      if (!compat.count(*i)) difference.compat.insert(*i);
    }
    for (set<string>::iterator j = other.ro_compat.begin();
	 j != other.ro_compat.end();
	 ++j) {
      if (!ro_compat.count(*j))	difference.ro_compat.insert(*j);
    }
    for (set<string>::iterator k = other.incompat.begin();
	 k != other.incompat.end();
	 ++k) {
      if (!incompat.count(*k)) difference.incompat.insert(*k);
    }
    return difference;
  }
  
  class iterator {
  private:
    friend class CompatSet;
    CompatSet *cset;
    vector<set<string> *>::iterator feature_it;
    set<string>::iterator it;
  public:
    iterator(CompatSet *comset) : cset(comset){
      feature_it = cset->all_features.begin();
      it = (*feature_it)->begin();
    }
    
    string operator*() {
      //check that we actually have contents, here
      if ( cset->all_features.end() != feature_it) { //if not at end
	return (*it);
      }
      return string();
    }
    
    iterator& operator++() {
      if (feature_it == cset->all_features.end()) return *this;//at end
      if (it != (*feature_it)->end()) { //if not at end of cur stringset
	if (++it != (*feature_it)->end()) return *this; //move along current set
      }
      if (++feature_it != cset->all_features.end()) { //move to next set
	it = (*feature_it)->begin(); //start it over in new set
	if ((*feature_it)->end() == it) return operator++(); //damn, keep moving
	return *this; //yay, done
      }
      return *this; //at full end now, returning end iterator
    }
    
    bool operator==(const iterator& other) const {
      return (other.cset == cset
	      && other.feature_it == feature_it
	      && other.it == it);
    }
    
    bool operator!=(const iterator& other) const { return !operator==(other);}
  };
  
  iterator begin() {
    iterator it(this);
    if ((*it.feature_it)->end() == it.it) ++it;
    return it;
  }
  
  iterator end() {
    iterator it(this);
    it.feature_it = all_features.end();
    it.it = all_features[2]->end();
    return it;
  }
  
  void encode(bufferlist& bl) const {
    ::encode(compat, bl);
    ::encode(ro_compat, bl);
    ::encode(incompat, bl);
  }
  
  void decode(bufferlist::iterator& bl) {
    ::decode(compat, bl);
    ::decode(ro_compat, bl);
    ::decode(incompat, bl);
  }
};
#endif
