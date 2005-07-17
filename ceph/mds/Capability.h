#ifndef __CAPABILITY_H
#define __CAPABILITY_H

// definite caps
#define CAP_FILE_RDCACHE   1
#define CAP_FILE_RD        2
#define CAP_FILE_WR        4
#define CAP_FILE_WRBUFFER  8
#define CAP_INODE_STAT    16

// heuristics
#define CAP_FILE_DELAYFLUSH  32

class Capability {
  int wanted_caps;     // what the client wants
  int pending_caps;    // what we've sent them
  int confirmed_caps;  // what they've confirmed they've received

  Capability(int wants = 0) :
	wanted_caps(wants),
	pending_caps(0),
	confirmed_caps(0) { }
};





#endif
