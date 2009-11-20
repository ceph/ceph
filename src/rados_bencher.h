// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "include/librados.h"
#include "config.h"
#include "common/common_init.h"
#include "common/Cond.h"
#include <iostream>
#include <fstream>

#include <stdlib.h>
#include <time.h>
#include <sstream>

Mutex dataLock("data mutex");

struct bench_data {
  bool done;
  int object_size;
  int trans_size;
  int in_flight;
  int started;
  int finished;
  double min_latency;
  double max_latency;
  double avg_latency;
  utime_t cur_latency;
  utime_t startTime;
};

static void *status_printer(void * data_store) {
  bench_data *data = (bench_data *) data_store;
  Cond cond;
  int i = 0;
  int previous_writes = 0;
  int cycleSinceChange = 0;
  double avg_bandwidth;
  double bandwidth;
  utime_t ONE_SECOND;
  ONE_SECOND.set_from_double(1.0);
  dataLock.Lock();
  while(!data->done) {
    if (i % 20 == 0) {
      if (i > 0)
	cout << "min lat: " << data->min_latency
	     << " max lat: " << data->max_latency
	     << " avg lat: " << data->avg_latency << std::endl;
      //I'm naughty and don't reset the fill
      cout << setfill(' ') 
	   << setw(5) << "sec" 
	   << setw(8) << "Cur ops"
	   << setw(10) << "started"
	   << setw(10) << "finished"
	   << setw(10) << "avg MB/s"
	   << setw(10) << "cur MB/s"
	   << setw(10) << "last lat"
	   << setw(10) << "avg lat" << std::endl;
    }
    bandwidth = (double)(data->finished - previous_writes)
      * (data->trans_size)
      / (1024*1024)
      / cycleSinceChange;
    avg_bandwidth = (double) (data->trans_size) * (data->finished)
      / (double)(g_clock.now() - data->startTime) / (1024*1024);
    if (previous_writes != data->finished) {
      previous_writes = data->finished;
      cycleSinceChange = 0;
      cout << setfill(' ') 
	   << setw(5) << i
	   << setw(8) << data->in_flight
	   << setw(10) << data->started
	   << setw(10) << data->finished
	   << setw(10) << avg_bandwidth
	   << setw(10) << bandwidth
	   << setw(10) << (double)data->cur_latency
	   << setw(10) << data->avg_latency << std::endl;
    }
    else {
      cout << setfill(' ')
	   << setw(5) << i
	   << setw(8) << data->in_flight
	   << setw(10) << data->started
	   << setw(10) << data->finished
	   << setw(10) << avg_bandwidth
	   << setw(10) << '0'
	   << setw(10) << '-'
	   << setw(10) << data->avg_latency << std::endl;
    }
    ++i;
    ++cycleSinceChange;
    cond.WaitInterval(dataLock, ONE_SECOND);
  }
  dataLock.Unlock();
  return NULL;
}
  
int aio_bench(Rados& rados, rados_pool_t pool, int secondsToRun,
	      int concurrentios, int writeSize, int readOffResults) {
  
  cout << "Maintaining " << concurrentios << " concurrent writes of "
       << writeSize << " bytes for at least "
       << secondsToRun << " seconds." << std::endl;
  
  dataLock.Lock();
  bench_data *data = new bench_data();
  data->done = false;
  data->trans_size = writeSize;
  data->started = 0;
  data->finished = 0;
  data->min_latency = 9999.0; // this better be higher than initial latency!
  data->max_latency = 0;
  data->avg_latency = 0;
  dataLock.Unlock();

  Rados::AioCompletion* completions[concurrentios];
  char* name[concurrentios];
  bufferlist* contents[concurrentios];
  char* contentsChars = new char[writeSize];
  double totalLatency = 0;
  utime_t startTimes[concurrentios];
  char bw[20];
  time_t initialTime;
  utime_t stopTime;

  //fill in contentsChars deterministically so we can check returns
  for (int i = 0; i < writeSize; ++i) {
    contentsChars[i] = i % sizeof(char);
  }
  
  time(&initialTime);
  stringstream initialTimeS("");
  initialTimeS << initialTime;
  char iTime[100];
  strcpy(iTime, initialTimeS.str().c_str());
  //set up writes so I can start them together
  for (int i = 0; i<concurrentios; ++i) {
    name[i] = new char[128];
    contents[i] = new bufferlist();
    snprintf(name[i], 128, "Object %d:%s", i, iTime);
    snprintf(contentsChars, writeSize, "I'm the %dth object!", i);
    contents[i]->append(contentsChars, writeSize);
  }
  
  //set up the pool, get start time, and go!
  cout << "open pool result = " << rados.open_pool("data",&pool) << " pool = " << pool << std::endl;

  
  pthread_t print_thread;
  
  pthread_create(&print_thread, NULL, status_printer, (void *)data);
  dataLock.Lock();
  data->startTime = g_clock.now();
  dataLock.Unlock();
  for (int i = 0; i<concurrentios; ++i) {
    startTimes[i] = g_clock.now();
    rados.aio_write(pool, name[i], 0, *contents[i], writeSize, &completions[i]);
    dataLock.Lock();
    ++data->started;
    ++data->in_flight;
    dataLock.Unlock();
  }
  
  //keep on adding new writes as old ones complete until we've passed minimum time
  int slot;
  bufferlist* newContents;
  char* newName;
  utime_t runtime;
  
  utime_t timePassed = g_clock.now() - data->startTime;
  //don't need locking for reads because other thread doesn't write
  
  runtime.set_from_double(secondsToRun);
  stopTime = data->startTime + runtime;
  while( g_clock.now() < stopTime ) {
    slot = data->finished % concurrentios;
    //create new contents and name on the heap, and fill them
    newContents = new bufferlist();
    newName = new char[128];
    snprintf(newName, 128, "Object %d:%s", data->started, iTime);
    snprintf(contentsChars, writeSize, "I'm the %dth object!", data->started);
    newContents->append(contentsChars, writeSize);
    completions[slot]->wait_for_safe();
    dataLock.Lock();
    data->cur_latency = g_clock.now() - startTimes[slot];
    totalLatency += data->cur_latency;
    if( data->cur_latency > data->max_latency) data->max_latency = data->cur_latency;
    if (data->cur_latency < data->min_latency) data->min_latency = data->cur_latency;
    ++data->finished;
    data->avg_latency = totalLatency / data->finished;
    --data->in_flight;
    dataLock.Unlock();
    completions[slot]->release();
    timePassed = g_clock.now() - data->startTime;
    
    //write new stuff to rados, then delete old stuff
    //and save locations of new stuff for later deletion
    startTimes[slot] = g_clock.now();
    rados.aio_write(pool, newName, 0, *newContents, writeSize, &completions[slot]);
    dataLock.Lock();
    ++data->started;
    ++data->in_flight;
    dataLock.Unlock();
    delete name[slot];
    delete contents[slot];
    name[slot] = newName;
    contents[slot] = newContents;
  }
  
  while (data->finished < data->started) {
    slot = data->finished % concurrentios;
    completions[slot]->wait_for_safe();
    dataLock.Lock();
    data->cur_latency = g_clock.now() - startTimes[slot];
    totalLatency += data->cur_latency;
    if (data->cur_latency > data->max_latency) data->max_latency = data->cur_latency;
    if (data->cur_latency < data->min_latency) data->min_latency = data->cur_latency;
    ++data->finished;
    data->avg_latency = totalLatency / data->finished;
    --data->in_flight;
    dataLock.Unlock();
    completions[slot]-> release();
    delete name[slot];
    delete contents[slot];
  }
  timePassed = g_clock.now() - data->startTime;
  dataLock.Lock();
  data->done = true;
  dataLock.Unlock();
  
  //check objects for consistency if requested
  int errors = 0;
  if (readOffResults) {
    char matchName[128];
    object_t oid;
    bufferlist actualContents;
    utime_t start_time;
    utime_t lat;
    double total_latency = 0;
    double avg_latency;
    double avg_bw;
    for (int i = 0; i < data->finished; ++i ) {
      snprintf(matchName, 128, "Object %d:%d", i, iTime);
      oid = object_t(matchName);
      snprintf(contentsChars, writeSize, "I'm the %dth object!", i);
      start_time = g_clock.now();
      rados.read(pool, oid, 0, actualContents, writeSize);
      lat = g_clock.now() - start_time;
      total_latency += (double) lat;
      if (strcmp(contentsChars, actualContents.c_str()) != 0 ) {
	cerr << "Object " << matchName << " is not correct!";
	++errors;
      }
      actualContents.clear();
    }
    avg_latency = total_latency / data->finished;
    avg_bw = data->finished * writeSize / (total_latency) / (1024 *1024);
    cout << "read avg latency: " << avg_latency
	 << " read avg bw: " << avg_bw << std::endl;
  }
  double bandwidth;
  bandwidth = ((double)data->finished)*((double)writeSize)/(double)timePassed;
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec
  sprintf(bw, "%.3lf \n", bandwidth);
  
  cout << "Total time run:        " << timePassed << std::endl
       << "Total writes made:     " << data->finished << std::endl
       << "Write size:            " << writeSize << std::endl
       << "Bandwidth (MB/sec):    " << bw << std::endl
       << "Average Latency:       " << data->avg_latency << std::endl
       << "Max latency:           " << data->max_latency << std::endl
       << "Min latency:           " << data->min_latency << std::endl;
  
  if (readOffResults) {
    if (errors) cout << "WARNING: There were " << errors << " total errors in copying!\n";
    else cout << "No errors in copying!\n";
  }
  
  pthread_join(print_thread, NULL);
  
  delete contentsChars;
  delete data;
  return 0;
}
