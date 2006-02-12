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

//
//
//	rush.h
//
//	Classes and definitions for the RUSH algorithm.
//
// $Id$
//
//

#ifndef	_rush_h_
#define	_rush_h_

#define	RUSH_MAX_CLUSTERS	100

class RushRNG {
public:
  unsigned int	RandomInt ();
  double RandomDouble ();
  void	Seed (unsigned int a, unsigned int b);
  int	HyperGeometricWeighted (int nDraws, double yesWeighted,
				double totalWeighted, double weightOne);
  void	DrawKofN (int vals[], int nToDraw, int setSize);
  RushRNG();
private:
  unsigned int state1, state2;
};

class Rush {
public:
  void	GetServersByKey (int key, int nReplicas, int servers[]);
  int	AddCluster (int nServers, double weight);
  int	Clusters () {return (nClusters);}
  int	Servers () {return (totalServers);}
  Rush ();
private:
  int	DrawKofN (int *servers, int n, int clusterSize, RushRNG *g);
  int	nClusters;
  int	totalServers;
  int	clusterSize[RUSH_MAX_CLUSTERS];
  int	serversInPrevious[RUSH_MAX_CLUSTERS];
  double clusterWeight[RUSH_MAX_CLUSTERS];
  double totalWeightBefore[RUSH_MAX_CLUSTERS];
};

#endif	/* _rush_h_ */
