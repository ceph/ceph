// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "include/librados.h"

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sstream>
#include "common/Clock.h"
#include "include/utime.h"

int main (int argc, const char **argv) {
  Rados rados;
  rados_pool_t pool;
  if (rados.initialize(0, NULL) < 0) {
      cerr << "couldn't initialize rados!\n";
      exit(1);
    }

  //hacky arguments; fix this
  int concurrentios = atoi(argv[1]);
  int secondsToRun = double(atoi(argv[2]));
  int writeSize = atoi(argv[3]); //bytes
  int readOffResults = atoi(argv[4]);

  cout << "Maintaining " << concurrentios << " concurrent writes of " << writeSize
       << " bytes for at least " << secondsToRun << " seconds.\n";

  Rados::AioCompletion* completions[concurrentios];
  char* name[concurrentios];
  bufferlist* contents[concurrentios];
  char contentsChars[writeSize];
  double totalLatency = 0;
  utime_t maxLatency;
  utime_t startTimes[concurrentios];
  int writesMade = 0;
  int writesCompleted = 0;
  time_t initialTime;
  utime_t startTime;
  utime_t stopTime;

  time(&initialTime);
  stringstream initialTimeS("");
  initialTimeS << initialTime;
  const char* iTime = initialTimeS.str().c_str();
  maxLatency.set_from_double(0);
  //set up writes so I can start them together
  for (int i = 0; i<concurrentios; ++i) {
    name[i] = new char[128];
    contents[i] = new bufferlist();
    snprintf(name[i], 128, "Object %s:%d", iTime, i);
    snprintf(contentsChars, writeSize, "I'm the %dth object!", i);
    contents[i]->append(contentsChars, writeSize);
  }

  //set up the pool, get start time, and go!
  cout << "open pool result = " << rados.open_pool("data",&pool) << " pool = " << pool << std::endl;

  startTime = g_clock.now();

  for (int i = 0; i<concurrentios; ++i) {
    startTimes[i] = g_clock.now();
    rados.aio_write(pool, name[i], 0, *contents[i], writeSize, &completions[i]);
    ++writesMade;
  }
  cerr << "Finished writing first objects\n";

  //keep on adding new writes as old ones complete until we've passed minimum time
  int slot;
  bufferlist* newContents;
  char* newName;
  utime_t currentLatency;
  utime_t runtime;
  runtime.set_from_double(secondsToRun);
  stopTime = startTime + runtime;
  while( g_clock.now() < stopTime ) {
    slot = writesCompleted % concurrentios;
    //create new contents and name on the heap, and fill them
    newContents = new bufferlist();
    newName = new char[128];
    snprintf(newName, 128, "Object %s:%d", iTime, writesMade);
    snprintf(contentsChars, writeSize, "I'm the %dth object!", writesMade);
    newContents->append(contentsChars, writeSize);
    completions[slot]->wait_for_complete();
    currentLatency = g_clock.now() - startTimes[slot];
    totalLatency += currentLatency;
    if( currentLatency > maxLatency) maxLatency = currentLatency;
    ++writesCompleted;
    completions[slot]->release();
    //write new stuff to rados, then delete old stuff
    //and save locations of new stuff for later deletion
    startTimes[slot] = g_clock.now();
    rados.aio_write(pool, newName, 0, *newContents, writeSize, &completions[slot]);
    ++writesMade;
    delete name[slot];
    delete contents[slot];
    name[slot] = newName;
    contents[slot] = newContents;
  }
  
  cerr << "Waiting for last writes to finish\n";
  while (writesCompleted < writesMade) {
    slot = writesCompleted % concurrentios;
    completions[slot]->wait_for_complete();
    currentLatency = g_clock.now() - startTimes[slot];
    totalLatency += currentLatency;
    if (currentLatency > maxLatency) maxLatency = currentLatency;
    completions[slot]-> release();
    ++writesCompleted;
    delete name[slot];
    delete contents[slot];
  }

  utime_t timePassed = g_clock.now() - startTime;

  //check objects for consistency if requested
  int errors = 0;
  if (readOffResults) {
    char matchName[128];
    object_t oid;
    bufferlist actualContents;
    for (int i = 0; i < writesCompleted; ++i ) {
      snprintf(matchName, 128, "Object %s:%d", iTime, i);
      oid = object_t(matchName);
      snprintf(contentsChars, writeSize, "I'm the %dth object!", i);
      rados.read(pool, oid, 0, actualContents, writeSize);
      if (strcmp(contentsChars, actualContents.c_str()) != 0 ) {
	cerr << "Object " << matchName << " is not correct!";
	++errors;
      }
      actualContents.clear();
    }
  }

  char bw[20];
  double bandwidth = ((double)writesCompleted)*((double)writeSize)/(double)timePassed;
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec
  sprintf(bw, "%.3lf \n", bandwidth);

  double averageLatency = totalLatency / writesCompleted;

  cout << "Total time run:        " << timePassed << endl
       << "Total writes made:     " << writesCompleted << endl
       << "Write size:            " << writeSize << endl
       << "Bandwidth (MB/sec):    " << bw << endl
       << "Average Latency:       " << averageLatency << endl
       << "Max latency:           " << maxLatency << endl
       << "Time waiting for Rados:" << totalLatency/concurrentios << endl;

  if (readOffResults) {
    if (errors) cout << "WARNING: There were " << errors << " total errors in copying!\n";
    else cout << "No errors in copying!\n";
  }
  return 0;
}
