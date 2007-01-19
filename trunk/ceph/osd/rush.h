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


//
//
//    rush.h
//
//    Classes and definitions for the RUSH algorithm.
//
// $Id$
//
//

#ifndef    _rush_h_
#define    _rush_h_

#define    RUSH_MAX_CLUSTERS    100

class RushRNG {
public:
  unsigned int    RandomInt ();
  double RandomDouble ();
  void    Seed (unsigned int a, unsigned int b);
  int    HyperGeometricWeighted (int nDraws, double yesWeighted,
                double totalWeighted, double weightOne);
  void    DrawKofN (int vals[], int nToDraw, int setSize);
  RushRNG();
private:
  unsigned int state1, state2;
};

class Rush {
public:
  void    GetServersByKey (int key, int nReplicas, int servers[]);
  int    AddCluster (int nServers, double weight);
  int    Clusters () {return (nClusters);}
  int    Servers () {return (totalServers);}
  Rush ();
private:
  int    DrawKofN (int *servers, int n, int clusterSize, RushRNG *g);
  int    nClusters;
  int    totalServers;
  int    clusterSize[RUSH_MAX_CLUSTERS];
  int    serversInPrevious[RUSH_MAX_CLUSTERS];
  double clusterWeight[RUSH_MAX_CLUSTERS];
  double totalWeightBefore[RUSH_MAX_CLUSTERS];
};

#endif    /* _rush_h_ */
