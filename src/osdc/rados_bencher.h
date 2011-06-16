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
 * Series of functions to test your rados installation. Notice
 * that this code is not terribly robust -- for instance, if you
 * try and bench on a pool you don't have permission to access
 * it will just loop forever.
 */
#include "include/rados/librados.hpp"
#include "common/config.h"
#include "common/common_init.h"
#include "common/Cond.h"
#include <iostream>
#include <fstream>

#include <stdlib.h>
#include <time.h>
#include <sstream>

Mutex dataLock("data mutex");
const int OP_WRITE     = 1;
const int OP_SEQ_READ  = 2;
const int OP_RAND_READ = 3;
const char *BENCH_DATA = "benchmark_write_data";

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

void generate_object_name(char *s, int objnum, int pid = 0)
{
  char hostname[30];
  gethostname(hostname, sizeof(hostname)-1);
  hostname[sizeof(hostname)-1] = 0;
  if (pid) {
    snprintf(s, sizeof(hostname), "%s_%d_object%d", hostname, pid, objnum);
  } else {
    snprintf(s, sizeof(hostname), "%s_%d_object%d", hostname, getpid(), objnum);
  }
}

int write_bench(librados::Rados& rados, librados::IoCtx& io_ctx,
		 int secondsToRun, int concurrentios, bench_data *data);
int seq_read_bench(librados::Rados& rados, librados::IoCtx& io_ctx,
		   int secondsToRun, int concurrentios, bench_data *data,
		   int writePid);
void *status_printer(void * data_store);
void sanitize_object_contents(bench_data *data, int length);

int aio_bench(librados::Rados& rados, librados::IoCtx &io_ctx, int operation,
	      int secondsToRun, int concurrentios, int op_size) {
  int object_size = op_size;
  int num_objects = 0;
  char* contentsChars = new char[op_size];
  int r = 0;
  int prevPid = 0;

  //get data from previous write run, if available
  if (operation != OP_WRITE) {
    bufferlist object_data;
    r = io_ctx.read(BENCH_DATA, object_data, sizeof(int)*3, 0);
    if (r <= 0) {
      delete[] contentsChars;
      if (r == -2)
	cerr << "Must write data before running a read benchmark!" << std::endl;
      return r;
    }
    bufferlist::iterator p = object_data.begin();
    ::decode(object_size, p);
    ::decode(num_objects, p);
    ::decode(prevPid, p);
  } else {
    object_size = op_size;
  }

  dataLock.Lock();
  bench_data *data = new bench_data();
  data->done = false;
  data->object_size = object_size;
  data->trans_size = op_size;
  data->in_flight = 0;
  data->started = 0;
  data->finished = num_objects;
  data->min_latency = 9999.0; // this better be higher than initial latency!
  data->max_latency = 0;
  data->avg_latency = 0;
  data->object_contents = contentsChars;
  dataLock.Unlock();

  //fill in contentsChars deterministically so we can check returns
  sanitize_object_contents(data, data->object_size);

  if (OP_WRITE == operation) {
    r = write_bench(rados, io_ctx, secondsToRun, concurrentios, data);
    if (r != 0) goto out;
  }
  else if (OP_SEQ_READ == operation) {
    r = seq_read_bench(rados, io_ctx, secondsToRun, concurrentios, data, prevPid);
    if (r != 0) goto out;
  }
  else if (OP_RAND_READ == operation) {
    cerr << "Random test not implemented yet!" << std::endl;
    r = -1;
  }

 out:
  delete[] contentsChars;
  delete data;
  return r;
}

void _aio_cb(void *cb, void *arg) {
  dataLock.Lock();
  Cond *cond = (Cond *) arg;
  cond->Signal();
  dataLock.Unlock();
}

int write_bench(librados::Rados& rados, librados::IoCtx& io_ctx,
		 int secondsToRun, int concurrentios, bench_data *data) {
  cout << "Maintaining " << concurrentios << " concurrent writes of "
       << data->object_size << " bytes for at least "
       << secondsToRun << " seconds." << std::endl;

  librados::AioCompletion* completions[concurrentios];
  char* name[concurrentios];
  bufferlist* contents[concurrentios];
  double total_latency = 0;
  utime_t start_times[concurrentios];
  utime_t stopTime;
  int r = 0;
  bufferlist b_write;
  Cond cond;
  utime_t runtime;
  utime_t timePassed;

  //set up writes so I can start them together
  for (int i = 0; i<concurrentios; ++i) {
    name[i] = new char[128];
    contents[i] = new bufferlist();
    generate_object_name(name[i], i);
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
    completions[i] = rados.aio_create_completion((void *) &cond, 0,
						 &_aio_cb);
    r = io_ctx.aio_write(name[i], completions[i], *contents[i], data->object_size, 0);
    if (r < 0) { //naughty, doesn't clean up heap
      goto ERR;
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

  //don't need locking for reads because other thread doesn't write

  runtime.set_from_double(secondsToRun);
  stopTime = data->start_time + runtime;
  while( g_clock.now() < stopTime ) {
    dataLock.Lock();
    while (1) {
      for (slot = 0; slot < concurrentios; ++slot) {
	if (completions[slot]->is_safe()) {
	  break;
	}
      }
      if (slot < concurrentios) {
	break;
      }
      cond.Wait(dataLock);
    }
    dataLock.Unlock();
    //create new contents and name on the heap, and fill them
    newContents = new bufferlist();
    newName = new char[128];
    generate_object_name(newName, data->started);
    snprintf(data->object_contents, data->object_size, "I'm the %dth object!", data->started);
    newContents->append(data->object_contents, data->object_size);
    completions[slot]->wait_for_safe();
    dataLock.Lock();
    r = completions[slot]->get_return_value();
    if (r != 0) {
      dataLock.Unlock();
      goto ERR;
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
    completions[slot] = 0;
    timePassed = g_clock.now() - data->start_time;

    //write new stuff to rados, then delete old stuff
    //and save locations of new stuff for later deletion
    start_times[slot] = g_clock.now();
    completions[slot] = rados.aio_create_completion((void *) &cond, 0, &_aio_cb);
    r = io_ctx.aio_write(newName, completions[slot], *newContents, data->object_size, 0);
    if (r < 0) {//naughty; doesn't clean up heap space.
      goto ERR;
    }
    dataLock.Lock();
    ++data->started;
    ++data->in_flight;
    dataLock.Unlock();
    delete[] name[slot];
    delete contents[slot];
    name[slot] = newName;
    contents[slot] = newContents;
  }

  while (data->finished < data->started) {
    slot = data->finished % concurrentios;
    completions[slot]->wait_for_safe();
    dataLock.Lock();
    r = completions[slot]->get_return_value();
    if (r != 0) {
      dataLock.Unlock();
      goto ERR;
    }
    data->cur_latency = g_clock.now() - start_times[slot];
    total_latency += data->cur_latency;
    if (data->cur_latency > data->max_latency) data->max_latency = data->cur_latency;
    if (data->cur_latency < data->min_latency) data->min_latency = data->cur_latency;
    ++data->finished;
    data->avg_latency = total_latency / data->finished;
    --data->in_flight;
    dataLock.Unlock();
    completions[slot]->release();
    completions[slot] = 0;
    delete[] name[slot];
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
  snprintf(bw, sizeof(bw), "%.3lf \n", bandwidth);

  cout << "Total time run:        " << timePassed << std::endl
       << "Total writes made:     " << data->finished << std::endl
       << "Write size:            " << data->object_size << std::endl
       << "Bandwidth (MB/sec):    " << bw << std::endl
       << "Average Latency:       " << data->avg_latency << std::endl
       << "Max latency:           " << data->max_latency << std::endl
       << "Min latency:           " << data->min_latency << std::endl;

  //write object size/number data for read benchmarks
  int written_objects[2];
  written_objects[0] = data->object_size;
  written_objects[1] = data->finished;
  ::encode(data->object_size, b_write);
  ::encode(data->finished, b_write);
  ::encode(getpid(), b_write);
  io_ctx.write(BENCH_DATA, b_write, sizeof(int)*3, 0);
  return 0;

 ERR:
  dataLock.Lock();
  data->done = 1;
  dataLock.Unlock();
  pthread_join(print_thread, NULL);
  return -5;
}

int seq_read_bench(librados::Rados& rados, librados::IoCtx& io_ctx, int seconds_to_run,
		   int concurrentios, bench_data *write_data, int pid) {
  bench_data *data = new bench_data();
  data->done = false;
  data->object_size = write_data->object_size;
  data->trans_size = data->object_size;
  data->in_flight= 0;
  data->started = 0;
  data->finished = 0;
  data->min_latency = 9999.0;
  data->max_latency = 0;
  data->avg_latency = 0;
  data->object_contents = write_data->object_contents;

  Cond cond;
  librados::AioCompletion* completions[concurrentios];
  char* name[concurrentios];
  bufferlist* contents[concurrentios];
  int index[concurrentios];
  int errors = 0;
  utime_t start_time;
  utime_t start_times[concurrentios];
  utime_t time_to_run;
  time_to_run.set_from_double(seconds_to_run);
  double total_latency = 0;
  int r = 0;
  utime_t runtime;
  sanitize_object_contents(data, 128); //clean it up once; subsequent
  //changes will be safe because string length monotonically increases

  //set up initial reads
  for (int i = 0; i < concurrentios; ++i) {
    name[i] = new char[128];
    generate_object_name(name[i], i, pid);
    contents[i] = new bufferlist();
  }

  pthread_t print_thread;
  pthread_create(&print_thread, NULL, status_printer, (void *)data);

  dataLock.Lock();
  data->start_time = g_clock.now();
  dataLock.Unlock();
  utime_t finish_time = data->start_time + time_to_run;
  //start initial reads
  for (int i = 0; i < concurrentios; ++i) {
    index[i] = i;
    start_times[i] = g_clock.now();
    completions[i] = rados.aio_create_completion((void *) &cond, &_aio_cb, 0);
    r = io_ctx.aio_read(name[i], completions[i], contents[i], data->object_size, 0);
    if (r < 0) { //naughty, doesn't clean up heap -- oh, or handle the print thread!
      cerr << "r = " << r << std::endl;
      goto ERR;
    }
    dataLock.Lock();
    ++data->started;
    ++data->in_flight;
    dataLock.Unlock();
  }

  //keep on adding new reads as old ones complete
  int slot;
  char* newName;
  bufferlist *cur_contents;

  while (seconds_to_run && (g_clock.now() < finish_time) &&
      write_data->finished > data->started) {
    dataLock.Lock();
    while (1) {
      for (slot = 0; slot < concurrentios; ++slot) {
	if (completions[slot]->is_complete()) {
	  break;
	}
      }
      if (slot < concurrentios) {
	break;
      }
      cond.Wait(dataLock);
    }
    dataLock.Unlock();
    newName = new char[128];
    generate_object_name(newName, data->started, pid);
    int current_index = index[slot];
    index[slot] = data->started;
    completions[slot]->wait_for_complete();
    dataLock.Lock();
    r = completions[slot]->get_return_value();
    if (r != 0) {
      cerr << "read got " << r << std::endl;
      dataLock.Unlock();
      goto ERR;
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
    completions[slot] = 0;
    cur_contents = contents[slot];

    //start new read and check data if requested
    start_times[slot] = g_clock.now();
    contents[slot] = new bufferlist();
    completions[slot] = rados.aio_create_completion((void *) &cond, &_aio_cb, 0);
    r = io_ctx.aio_read(newName, completions[slot], contents[slot], data->object_size, 0);
    if (r < 0) {
      goto ERR;
    }
    dataLock.Lock();
    ++data->started;
    ++data->in_flight;
    snprintf(data->object_contents, data->object_size, "I'm the %dth object!", current_index);
    dataLock.Unlock();
    if (memcmp(data->object_contents, cur_contents->c_str(), data->object_size) != 0) {
      cerr << name[slot] << " is not correct!" << std::endl;
      ++errors;
    }
    delete name[slot];
    name[slot] = newName;
    delete cur_contents;
  }

  //wait for final reads to complete
  while (data->finished < data->started) {
    slot = data->finished % concurrentios;
    completions[slot]->wait_for_complete();
    dataLock.Lock();
    r = completions[slot]->get_return_value();
    if (r != 0) {
      cerr << "read got " << r << std::endl;
      dataLock.Unlock();
      goto ERR;
    }
    data->cur_latency = g_clock.now() - start_times[slot];
    total_latency += data->cur_latency;
    if (data->cur_latency > data->max_latency) data->max_latency = data->cur_latency;
    if (data->cur_latency < data->min_latency) data->min_latency = data->cur_latency;
    ++data->finished;
    data->avg_latency = total_latency / data->finished;
    --data->in_flight;
    completions[slot]-> release();
    completions[slot] = 0;
    snprintf(data->object_contents, data->object_size, "I'm the %dth object!", index[slot]);
    dataLock.Unlock();
    if (memcmp(data->object_contents, contents[slot]->c_str(), data->object_size) != 0) {
      cerr << name[slot] << " is not correct!" << std::endl;
      ++errors;
    }
    delete name[slot];
    delete contents[slot];
  }

  runtime = g_clock.now() - data->start_time;
  dataLock.Lock();
  data->done = true;
  dataLock.Unlock();

  pthread_join(print_thread, NULL);

  double bandwidth;
  bandwidth = ((double)data->finished)*((double)data->object_size)/(double)runtime;
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec
  char bw[20];
  snprintf(bw, sizeof(bw), "%.3lf \n", bandwidth);

  cout << "Total time run:        " << runtime << std::endl
       << "Total reads made:     " << data->finished << std::endl
       << "Read size:            " << data->object_size << std::endl
       << "Bandwidth (MB/sec):    " << bw << std::endl
       << "Average Latency:       " << data->avg_latency << std::endl
       << "Max latency:           " << data->max_latency << std::endl
       << "Min latency:           " << data->min_latency << std::endl;

  delete data;
  return 0;

 ERR:
  dataLock.Lock();
  data->done = 1;
  dataLock.Unlock();
  pthread_join(print_thread, NULL);
  return -5;
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
