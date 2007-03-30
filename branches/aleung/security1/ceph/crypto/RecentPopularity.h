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
  map < inodeno_t, deque<inodeno_t> > inode_sequences;
public:
  RecentPopularity() : J(4), K(6) {}
  RecentPopularity(int jay, int kay) : J(jay), K(kay) {}
  RecentPopularity(map<inodeno_t, deque<inodeno_t> > sequence) : J(4), K(6),
								 inode_sequences(sequence) {}
  RecentPopularity(int jay, int kay, map<inodeno_t, deque<inodeno_t> > sequence) : J(jay), K(kay), inode_sequences(sequence) {}
  

  map<inodeno_t, deque<inodeno_t> >& get_sequence() { return inode_sequences; }
  void add_observation(inodeno_t X, inodeno_t successor) {
    inode_sequences[X].push_back(successor);
    
    if (inode_sequences[X].size() > (unsigned)K)
      inode_sequences[X].pop_front();
  }

  inodeno_t predict_successor(inodeno_t X) {

    //debug -- remove this at some point
    //for (deque<inodeno_t>::reverse_iterator test_it = inode_sequences[X].rbegin();
    //test_it != inode_sequences[X].rend();
    //test_it++) {
    //cout << *test_it << endl;
    //}

    // is our known sequence big enough?
    if (inode_sequences[X].size() < (unsigned)K)
      return inodeno_t();

    // can we make a prediction with confidence?
    set<inodeno_t> checked_inodes;
    unsigned int index = 0;
    for (deque<inodeno_t>::reverse_iterator iri = inode_sequences[X].rbegin();
	 iri != inode_sequences[X].rend();
	 iri++) {
      
      // dont even bother if weve already searched
      if (checked_inodes.count(*iri) == 0) {

	// are there enough unchecked inodes to even keep going?
	if (inode_sequences[X].size() - index >= (unsigned)J) {
	  int occurance = 0;
	  for (deque<inodeno_t>::reverse_iterator ini = iri;
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
