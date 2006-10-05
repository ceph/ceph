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
//    rush.cc
//
// $Id$
//

#include <stdlib.h>
#include <stdio.h>
#include <cassert>
#include "rush.h"


static
unsigned int
myhash (unsigned int n)
{
  unsigned int v = (n ^ 0xdead1234) * (884811920 * 3  + 1);
  return (v);
}

Rush::Rush ()
{
  nClusters = 0;
  totalServers = 0;
}

//----------------------------------------------------------------------
//
//    Rush::AddCluster
//
//    Add a cluster.  The number of servers in the cluster and
//    the weight of each server is passed.  The current number of
//    clusters is returned.
//
//----------------------------------------------------------------------
int
Rush::AddCluster (int nServers, double weight)
{
  clusterSize[nClusters] = nServers;
  clusterWeight[nClusters] = weight;
  if (nClusters == 0) {
    serversInPrevious[0] = 0;
    totalWeightBefore[0] = 0.0;
  } else {
    serversInPrevious[nClusters] = serversInPrevious[nClusters-1] +
      clusterSize[nClusters-1];
    totalWeightBefore[nClusters] =
      totalWeightBefore[nClusters-1] + (double)clusterSize[nClusters-1] *
      clusterWeight[nClusters-1];
  }
  nClusters += 1;
  totalServers += nServers;
#if 0
  for (int i = 0; i < nClusters; i++) {
    fprintf (stderr, "size=%-3d prev=%-3d weight=%-6.2f prevWeight=%-8.2f\n",
        clusterSize[i], serversInPrevious[i], clusterWeight[i],
        totalWeightBefore[i]);
  }
#endif
  return (nClusters);
}


//----------------------------------------------------------------------
//
//    Rush::GetServersByKey
//
//    This function returns a list of servers on which an object
//    should be placed.  The servers array must be large enough to
//    contain the list.
//
//----------------------------------------------------------------------
void
Rush::GetServersByKey (int key, int nReplicas, int servers[])
{
  int    replicasLeft = nReplicas;
  int    cluster;
  int    mustAssign, numberAssigned;
  int    i, toDraw;
  int    *srv = servers;
  double    myWeight;
  RushRNG    rng;

  // There may not be more replicas than servers!
  assert (nReplicas <= totalServers);
  
  for (cluster = nClusters-1; (cluster >= 0) && (replicasLeft > 0); cluster--) {
    if (serversInPrevious[cluster] < replicasLeft) {
      mustAssign = replicasLeft - serversInPrevious[cluster];
    } else {
      mustAssign = 0;
    }
    toDraw = replicasLeft - mustAssign;
    if (toDraw > (clusterSize[cluster] - mustAssign)) {
      toDraw = clusterSize[cluster] - mustAssign;
    }
    myWeight = (double)clusterSize[cluster] * clusterWeight[cluster];
    rng.Seed (myhash (key)^cluster, cluster^0xb90738);
    numberAssigned = mustAssign +
      rng.HyperGeometricWeighted (toDraw, myWeight,
                                  totalWeightBefore[cluster] + myWeight,
                                  clusterWeight[cluster]);
    if (numberAssigned > 0) {
      rng.Seed (myhash (key)^cluster ^ 11, cluster^0xfea937);
      rng.DrawKofN (srv, numberAssigned, clusterSize[cluster]);
      for (i = 0; i < numberAssigned; i++) {
        srv[i] += serversInPrevious[cluster];
      }
      replicasLeft -= numberAssigned;
      srv += numberAssigned;
    }
  }
}



//----------------------------------------------------------------------
//
//    RushRNG::HyperGeometricWeighted
//
//    Use an iterative method to generate a hypergeometric random
//    variable.  This approach guarantees that, if the number of draws
//    is reduced, the number of successes must be as well as long as
//    the seed for the RNG is the same.
//
//----------------------------------------------------------------------
int
RushRNG::HyperGeometricWeighted (int nDraws, double yesWeighted,
                 double totalWeighted, double weightOne)
{
  int    positives = 0, i;
  double    curRand;

  // If the weight is too small (or is negative), choose zero objects.
  if (weightOne <= 1e-9 || nDraws == 0) {
    return (0);
  }

  // Draw nDraws items from the "bag".  For each positive, subtract off
  // the weight of an object from the weight of positives remaining.  For
  // each draw, subtract off the weight of an object from the total weight
  // remaining.
  for (i = 0; i < nDraws; i++) {
    curRand = RandomDouble ();
    if (curRand < (yesWeighted / totalWeighted)) {
      positives += 1;
      yesWeighted -= weightOne;
    }
    totalWeighted -= weightOne;
  }
  return (positives);
}

//----------------------------------------------------------------------
//
//    RushRNG::DrawKofN
//
//----------------------------------------------------------------------
void
RushRNG::DrawKofN (int vals[], int nToDraw, int setSize)
{
  int    deck[setSize];
  int    i, pick;

  assert(nToDraw <= setSize);

  for (i = 0; i < setSize; i++) {
    deck[i] = i;
  }

  for (i = 0; i < nToDraw; i++) {
    pick = (int)(RandomDouble () * (double)(setSize - i));
    if (pick >= setSize-i) pick = setSize-i-1;  // in case
    //    assert(i >= 0 && i < nToDraw);
    //    assert(pick >= 0 && pick < setSize);
    vals[i] = deck[pick];
    deck[pick] = deck[setSize-i-1];
  }
}

#define    SEED_X 521288629
#define    SEED_Y 362436069
RushRNG::RushRNG ()
{
  Seed (0, 0);
}

void
RushRNG::Seed (unsigned int seed1, unsigned int seed2)
{
  state1 = ((seed1 == 0) ? SEED_X : seed1);
  state2 = ((seed2 == 0) ? SEED_Y : seed2);
}

unsigned int
RushRNG::RandomInt ()
{
  const unsigned int a = 18000;
  const unsigned int b = 18879;
  unsigned int    rndValue;

  state1 = a * (state1 & 0xffff) + (state1 >> 16);
  state2 = b * (state2 & 0xffff) + (state2 >> 16);
  rndValue = (state1 << 16) + (state2 & 0xffff);
  return (rndValue);
}

double
RushRNG::RandomDouble ()
{
  double    v;

  v = (double)RandomInt() / (65536.0*65536.0);
  return (v);
}
