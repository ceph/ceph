// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __RECENTPOPULARITY_H
#define __RECENTPOPULARITY_H

class RecentPopularity {
  // confidence parameter (4 is a happy default)
  int J;
  // sequence size parameter (6 is a happy default)
  int K;
  map < string, deque<string> > inode_sequences;
public:
  RecentPopularity() : J(4), K(6) {}
  RecentPopularity(int jay, int kay) : J(jay), K(kay) {}
  
  void add_observation(string X, string successor) {
    inode_sequences[X].push_back(successor);
    
    if (inode_sequences[X].size() > (unsigned)K)
      inode_sequences[X].pop_front();
  }

  string predict_successor(string X) {

    // is our known sequence big enough?
    if (inode_sequences[X].size() < (unsigned)K)
      return string();

    // can we make a prediction with confidence?
    set<string> checked_inodes;
    unsigned int index = 0;
    for (deque<string>::reverse_iterator iri = inode_sequences[X].rbegin();
	 iri != inode_sequences[X].rend();
	 iri++) {
      
      // dont even bother if weve already searched
      if (checked_inodes.count(*iri) == 0) {

	// are there enough unchecked inodes to even keep going?
	if (inode_sequences[X].size() - index >= (unsigned)J) {
	  int occurance = 0;
	  for (deque<string>::reverse_iterator ini = iri;
	       ini != inode_sequences[X].rend();
	       ini++) {
	    
	    // do we have a match?
	    if ((*ini) == (*iri))
	      occurance++;
	    if (occurance > J)
	      return (*iri);
	  }
	}
	else
	  return 0;
	
	// mark the inode as seen
	checked_inodes.insert(*iri);
      }

      index++;
    }

    // we cannot make a guess with confidnce
    return 0;
  }
};

#endif
