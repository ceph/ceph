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
#include <time.h>

int main (int argc, const char **argv) {
  Rados rados;
  rados_pool_t pool;
  if (rados.initialize(0, NULL) < 0) {
      cerr << "couldn't initialize rados!\n";
      exit(1);
    }

  //hacky arguments, fix this
  int concurrentios = atoi(argv[1]);
  int secondsToRun = double(atoi(argv[2]));
  int writeSize = atoi(argv[3]); //bytes
  bool readOffResults = true;

  cout << "Maintaining " << concurrentios << " concurrent writes of " << writeSize
       << " bytes for at least " << secondsToRun << " seconds.\n";

  Rados::AioCompletion* completions[concurrentios];
  char* name[concurrentios];
  bufferlist contents[concurrentios];
  char contentsChars[writeSize];
  int writesMade = 0;
  int writesCompleted = 0;
  time_t initialTime;
  time_t startTime;
  time_t currentTime;
  double timePassed;

  time(&initialTime);
  //set up writes so I can start them together
  for (int i = 0; i<concurrentios; ++i) {
    name[i] = new char[128];
    snprintf(name[i], 128, "Object %d:%d", initialTime, i);
    snprintf(contentsChars, writeSize, "I'm the %dth object!", i);
    contents[i] = bufferlist();
    contents[i].append(contentsChars, writeSize);
  }

  cout << "open pool result = " << rados.open_pool("data",&pool) << " pool = " << pool << std::endl;

  currentTime = time(&startTime);

  for (int i = 0; i<concurrentios; ++i) {
    /*    cerr << "About to write object " << writesMade << " with name " << name[i] << endl
	  << "         and contents " << contents[i].c_str() << endl; */
    rados.aio_write(pool, name[i], 0, contents[i], writeSize, &completions[i]);
    /*    cerr << "Writing object        " << writesMade << endl;
    cerr << "Object write complete? " << boolalpha << completions[i]->is_complete() << endl;
    cerr << "Completed object      " << writesMade << endl; */
    ++writesMade;
  }

  cerr << "Finished writing first objects\n";
  int slot;
  while(difftime(currentTime, startTime) < secondsToRun ) {
    slot = writesCompleted % concurrentios;
    //    cerr << "Waiting for " << writesCompleted << "th write to finish.\n";
    completions[slot]->wait_for_safe();
    ++writesCompleted;
    snprintf(name[slot], 128, "Object %d:%d", initialTime, writesMade);
    snprintf(contentsChars, writeSize, "I'm the %dth object!", writesMade);
    contents[slot].clear();
    contents[slot].append(contentsChars, writeSize);
    completions[slot]->release();
    rados.aio_write(pool, name[slot], 0, contents[slot], writeSize, &completions[slot]);
    ++writesMade;
    time(&currentTime);
  }
  
  cerr << "Waiting for last writes to finish\n";
  while (writesCompleted < writesMade) {
    //    cerr << "Currently completed " << writesCompleted << "/" << writesMade << endl;
    completions[writesCompleted % concurrentios]->wait_for_safe();
    ++writesCompleted;
    //    cerr << "Just completed write" << writesCompleted << "in slot " << writesCompleted%concurrentios << endl;
  }
  time(&currentTime);

  //check objects for consistency

  timePassed = difftime(currentTime, startTime);
  cout << "Total time run:        " << timePassed << endl
       << "Total writes made:     " << writesCompleted << endl
       << "Write size:            " << writeSize << endl
       << "Bandwidth (bytes/sec): " << ((double)writesCompleted)*writeSize/timePassed << endl;

  return 0;
}
