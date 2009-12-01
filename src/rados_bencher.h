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
  bool done; //is the benchmark is done
  int object_size; //the size of the objects
  int trans_size; //size of the write/read to perform
  // same as object_size for write tests
  int in_flight; //number of reads/writes being waited on
  int started;
  int finished;
  double min_latency;
  double max_latency;
  double avg_latency;
  utime_t cur_latency; //latency of last completed transaction
  utime_t start_time; //start time for benchmark
  char *object_contents; //pointer to the contents written to each object
};

int write_bench(Rados& rados, rados_pool_t pool,
		 int secondsToRun, int concurrentios, bench_data *data);
int seq_read_bench(Rados& rados, rados_pool_t pool,
		   int concurrentios, bench_data *data, int verify);
void *status_printer(void * data_store);
void sanitize_object_contents(bench_data *data, int length);
  
int aio_bench(Rados& rados, rados_pool_t pool, int secondsToRun,
	      int concurrentios, int writeSize, int sequentialTest) {

  char* contentsChars = new char[writeSize];
  int r = 0;
  
  dataLock.Lock();
  bench_data *data = new bench_data();
  data->done = false;
  data->object_size = writeSize;
  data->trans_size = writeSize; //just for now
  data->in_flight = 0;
  data->started = 0;
  data->finished = 0;
  data->min_latency = 9999.0; // this better be higher than initial latency!
  data->max_latency = 0;
  data->avg_latency = 0;
  data->object_contents = contentsChars;
  dataLock.Unlock();


  //fill in contentsChars deterministically so we can check returns
  sanitize_object_contents(data, writeSize);
  //set up the pool
  cout << "open pool result = " << rados.open_pool("data",&pool) << " pool = " << pool << std::endl;
  
  r = write_bench(rados, pool, secondsToRun, concurrentios, data);
  if (r != 0) goto out;

  //check objects for consistency if requested
  if (sequentialTest) {
    r = seq_read_bench(rados, pool, concurrentios, data, 1);
  }
  
 out:
  delete contentsChars;
  delete data;
  return r;
}

int write_bench(Rados& rados, rados_pool_t pool,
		 int secondsToRun, int concurrentios, bench_data *data) {
  cout << "Maintaining " << concurrentios << " concurrent writes of "
       << data->object_size << " bytes for at least "
       << secondsToRun << " seconds." << std::endl;
  
  Rados::AioCompletion* completions[concurrentios];
  char* name[concurrentios];
  bufferlist* contents[concurrentios];
  double total_latency = 0;
  utime_t start_times[concurrentios];
  utime_t stopTime;
  int r = 0;

  //set up writes so I can start them together
  for (int i = 0; i<concurrentios; ++i) {
    name[i] = new char[128];
    contents[i] = new bufferlist();
    snprintf(name[i], 128, "Object %d", i);
    snprintf(data->object_contents, data->object_size, "I'm the %dth object!", i);
    contents[i]->append(data->object_contents, data->object_size);
  }

  pthread_t print_thread;
  
  pthread_create(&print_thread, NULL, status_printer, (void *)data);
  dataLock.Lock();
  data->start_time = g_clock.now();
  dataLock.Unlock();
  for (int i = 0; i<concurrentios; ++i) {
    start_times[i] = g_clock.now();
    r = rados.aio_write(pool, name[i], 0, *contents[i], data->object_size, &completions[i]);
    if (r < 0) { //naughty, doesn't clean up heap
	dataLock.Unlock();
	return -5; //EIO
    }
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
  
  utime_t timePassed = g_clock.now() - data->start_time;
  //don't need locking for reads because other thread doesn't write
  
  runtime.set_from_double(secondsToRun);
  stopTime = data->start_time + runtime;
  while( g_clock.now() < stopTime ) {
    slot = data->finished % concurrentios;
    //create new contents and name on the heap, and fill them
    newContents = new bufferlist();
    newName = new char[128];
    snprintf(newName, 128, "Object %d", data->started);
    snprintf(data->object_contents, data->object_size, "I'm the %dth object!", data->started);
    newContents->append(data->object_contents, data->object_size);
    completions[slot]->wait_for_safe();
    dataLock.Lock();
    r = completions[slot]->get_return_value();
    if (r != 0) {
      dataLock.Unlock();
      return r;
    }
    data->cur_latency = g_clock.now() - start_times[slot];
    total_latency += data->cur_latency;
    if( data->cur_latency > data->max_latency) data->max_latency = data->cur_latency;
    if (data->cur_latency < data->min_latency) data->min_latency = data->cur_latency;
    ++data->finished;
    data->avg_latency = total_latency / data->finished;
    --data->in_flight;
    dataLock.Unlock();
    completions[slot]->release();
    timePassed = g_clock.now() - data->start_time;
    
    //write new stuff to rados, then delete old stuff
    //and save locations of new stuff for later deletion
    start_times[slot] = g_clock.now();
    r = rados.aio_write(pool, newName, 0, *newContents, data->object_size, &completions[slot]);
    if (r < 0) //naughty; doesn't clean up heap space.
      return r;
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
    data->cur_latency = g_clock.now() - start_times[slot];
    total_latency += data->cur_latency;
    if (data->cur_latency > data->max_latency) data->max_latency = data->cur_latency;
    if (data->cur_latency < data->min_latency) data->min_latency = data->cur_latency;
    ++data->finished;
    data->avg_latency = total_latency / data->finished;
    --data->in_flight;
    dataLock.Unlock();
    completions[slot]-> release();
    delete name[slot];
    delete contents[slot];
  }

  timePassed = g_clock.now() - data->start_time;
  dataLock.Lock();
  data->done = true;
  dataLock.Unlock();

  pthread_join(print_thread, NULL);

  double bandwidth;
  bandwidth = ((double)data->finished)*((double)data->object_size)/(double)timePassed;
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec
  char bw[20];
  sprintf(bw, "%.3lf \n", bandwidth);
  
  cout << "Total time run:        " << timePassed << std::endl
       << "Total writes made:     " << data->finished << std::endl
       << "Write size:            " << data->object_size << std::endl
       << "Bandwidth (MB/sec):    " << bw << std::endl
       << "Average Latency:       " << data->avg_latency << std::endl
       << "Max latency:           " << data->max_latency << std::endl
       << "Min latency:           " << data->min_latency << std::endl;
  return 0;
}

int seq_read_bench(Rados& rados, rados_pool_t pool, bench_data *data) {
  int errors = 0;
  char matchName[128];
  object_t oid;
  bufferlist actualContents;
  utime_t start_time;
  utime_t last_start;
  double total_latency = 0;
  double avg_latency;
  double avg_bw;
  int r = 0;
  sanitize_object_contents(data, 128);
  start_time = g_clock.now();
  for (int i = 0; i < data->finished; ++i ) {
    snprintf(matchName, 128, "Object %d", i);
    oid = object_t(matchName);
    snprintf(data->object_contents, data->object_size, "I'm the %dth object!", i);
    last_start = g_clock.now();
    r = rados.read(pool, oid, 0, actualContents, data->object_size);
    if (r != data->object_size) {
      if (r < 0) return r;
      else return -5; //EIO
    }
    total_latency += (double) g_clock.now() - last_start;
    if (memcmp(data->object_contents, actualContents.c_str(), data->object_size) != 0 ) {
      cerr << "Object " << matchName << " is not correct!";
      ++errors;
    }
    actualContents.clear();
  }
  last_start = g_clock.now();
  avg_latency = total_latency / data->finished;
  avg_bw = data->finished * data->object_size /
    (double)(last_start - start_time) / (1024 *1024);
  cout << "read avg latency: " << avg_latency
       << " read avg bw: " << avg_bw << std::endl;
  
  if (errors) cout << "WARNING: There were " << errors << " total errors in copying!\n";
  else cout << "No errors in copying!\n";
  return 0;
}



void *status_printer(void * data_store) {
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
      / (double)(g_clock.now() - data->start_time) / (1024*1024);
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

inline void sanitize_object_contents (bench_data *data, int length) {
  for (int i = 0; i < length; ++i) {
    data->object_contents[i] = i % sizeof(char);
  }
}
