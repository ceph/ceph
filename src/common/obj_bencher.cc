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
#include "common/Cond.h"
#include "obj_bencher.h"

#include <iostream>
#include <fstream>

#include <cerrno>

#include <stdlib.h>
#include <time.h>
#include <sstream>


const char *BENCH_METADATA = "benchmark_write_metadata";
const std::string BENCH_PREFIX = "benchmark_data";

static std::string generate_object_prefix(int pid = 0) {
  char hostname[30];
  gethostname(hostname, sizeof(hostname)-1);
  hostname[sizeof(hostname)-1] = 0;

  if (!pid)
    pid = getpid();

  std::ostringstream oss;
  oss << BENCH_PREFIX << "_" << hostname << "_" << pid;
  return oss.str();
}

static std::string generate_object_name(int objnum, int pid = 0)
{
  std::ostringstream oss;
  oss << generate_object_prefix(pid) << "_object" << objnum;
  return oss.str();
}

static void sanitize_object_contents (bench_data *data, int length) {
  memset(data->object_contents, 'z', length);
}

ostream& ObjBencher::out(ostream& os, utime_t& t)
{
  if (show_time)
    return t.localtime(os) << " ";
  else
    return os << " ";
}

ostream& ObjBencher::out(ostream& os)
{
  utime_t cur_time = ceph_clock_now(g_ceph_context);
  return out(os, cur_time);
}

void *ObjBencher::status_printer(void *_bencher) {
  ObjBencher *bencher = (ObjBencher *)_bencher;
  bench_data& data = bencher->data;
  Cond cond;
  int i = 0;
  int previous_writes = 0;
  int cycleSinceChange = 0;
  double avg_bandwidth;
  double bandwidth;
  utime_t ONE_SECOND;
  ONE_SECOND.set_from_double(1.0);
  bencher->lock.Lock();
  while(!data.done) {
    utime_t cur_time = ceph_clock_now(g_ceph_context);

    if (i % 20 == 0) {
      if (i > 0)
	cur_time.localtime(cout) << "min lat: " << data.min_latency
	     << " max lat: " << data.max_latency
	     << " avg lat: " << data.avg_latency << std::endl;
      //I'm naughty and don't reset the fill
      bencher->out(cout, cur_time) << setfill(' ')
	   << setw(5) << "sec"
	   << setw(8) << "Cur ops"
	   << setw(10) << "started"
	   << setw(10) << "finished"
	   << setw(10) << "avg MB/s"
	   << setw(10) << "cur MB/s"
	   << setw(10) << "last lat"
	   << setw(10) << "avg lat" << std::endl;
    }
    bandwidth = (double)(data.finished - previous_writes)
      * (data.trans_size)
      / (1024*1024)
      / cycleSinceChange;

    if (!isnan(bandwidth)) {
      if (bandwidth > data.idata.max_bandwidth)
        data.idata.max_bandwidth = bandwidth;
      if (bandwidth < data.idata.min_bandwidth)
        data.idata.min_bandwidth = bandwidth;

      data.history.bandwidth.push_back(bandwidth);
    }

    avg_bandwidth = (double) (data.trans_size) * (data.finished)
      / (double)(cur_time - data.start_time) / (1024*1024);
    if (previous_writes != data.finished) {
      previous_writes = data.finished;
      cycleSinceChange = 0;
      bencher->out(cout, cur_time) << setfill(' ')
	   << setw(5) << i
	   << setw(8) << data.in_flight
	   << setw(10) << data.started
	   << setw(10) << data.finished
	   << setw(10) << avg_bandwidth
	   << setw(10) << bandwidth
	   << setw(10) << (double)data.cur_latency
	   << setw(10) << data.avg_latency << std::endl;
    }
    else {
      bencher->out(cout, cur_time) << setfill(' ')
	   << setw(5) << i
	   << setw(8) << data.in_flight
	   << setw(10) << data.started
	   << setw(10) << data.finished
	   << setw(10) << avg_bandwidth
	   << setw(10) << '0'
	   << setw(10) << '-'
	   << setw(10) << data.avg_latency << std::endl;
    }
    ++i;
    ++cycleSinceChange;
    cond.WaitInterval(g_ceph_context, bencher->lock, ONE_SECOND);
  }
  bencher->lock.Unlock();
  return NULL;
}

int ObjBencher::aio_bench(int operation, int secondsToRun, int concurrentios, int op_size, bool cleanup) {
  int object_size = op_size;
  int num_objects = 0;
  char* contentsChars = new char[op_size];
  int r = 0;
  int prevPid = 0;

  //get data from previous write run, if available
  if (operation != OP_WRITE) {
    r = fetch_bench_metadata(&object_size, &num_objects, &prevPid);
    if (r <= 0) {
      delete[] contentsChars;
      if (r == -ENOENT)
	cerr << "Must write data before running a read benchmark!" << std::endl;
      return r;
    }
  } else {
    object_size = op_size;
  }

  lock.Lock();
  data.done = false;
  data.object_size = object_size;
  data.trans_size = op_size;
  data.in_flight = 0;
  data.started = 0;
  data.finished = num_objects;
  data.min_latency = 9999.0; // this better be higher than initial latency!
  data.max_latency = 0;
  data.avg_latency = 0;
  data.idata.min_bandwidth = 99999999.0;
  data.idata.max_bandwidth = 0;
  data.object_contents = contentsChars;
  lock.Unlock();

  //fill in contentsChars deterministically so we can check returns
  sanitize_object_contents(&data, data.object_size);

  if (OP_WRITE == operation) {
    r = write_bench(secondsToRun, concurrentios);
    if (r != 0) goto out;
  }
  else if (OP_SEQ_READ == operation) {
    r = seq_read_bench(secondsToRun, num_objects, concurrentios, prevPid);
    if (r != 0) goto out;
  }
  else if (OP_RAND_READ == operation) {
    cerr << "Random test not implemented yet!" << std::endl;
    r = -1;
  }

  if (OP_WRITE == operation && cleanup) {
    r = fetch_bench_metadata(&object_size, &num_objects, &prevPid);
    if (r <= 0) {
      if (r == -ENOENT)
	cerr << "Should never happen: bench metadata missing for current run!" << std::endl;
      goto out;
    }
 
    r = clean_up(num_objects, prevPid);
    if (r != 0) goto out;

    r = sync_remove(BENCH_METADATA);
  }

 out:
  delete[] contentsChars;
  return r;
}

struct lock_cond {
  lock_cond(Mutex *_lock) : lock(_lock) {}
  Mutex *lock;
  Cond cond;
};

void _aio_cb(void *cb, void *arg) {
  struct lock_cond *lc = (struct lock_cond *)arg;
  lc->lock->Lock();
  lc->cond.Signal();
  lc->lock->Unlock();
}

static double vec_stddev(vector<double>& v)
{
  double mean = 0;

  if (v.size() < 2)
    return 0;

  vector<double>::iterator iter;
  for (iter = v.begin(); iter != v.end(); ++iter) {
    mean += *iter;
  }

  mean /= v.size();

  double stddev = 0;
  for (iter = v.begin(); iter != v.end(); ++iter) {
    double dev = *iter - mean;
    dev *= dev;
    stddev += dev;
  }
  stddev /= (v.size() - 1);
  return sqrt(stddev);
}

int ObjBencher::fetch_bench_metadata(int* object_size, int* num_objects, int* prevPid) {
  int r = 0;
  bufferlist object_data;

  r = sync_read(BENCH_METADATA, object_data, sizeof(int)*3);
  if (r <= 0) {
    return r;
  }
  bufferlist::iterator p = object_data.begin();
  ::decode(*object_size, p);
  ::decode(*num_objects, p);
  ::decode(*prevPid, p);

  return 1;
}

int ObjBencher::write_bench(int secondsToRun, int concurrentios) {
  out(cout) << "Maintaining " << concurrentios << " concurrent writes of "
       << data.object_size << " bytes for at least "
       << secondsToRun << " seconds." << std::endl;

  std::string prefix = generate_object_prefix();
  out(cout) << "Object prefix: " << prefix << std::endl;

  std::string name[concurrentios];
  std::string newName;
  bufferlist* contents[concurrentios];
  double total_latency = 0;
  utime_t start_times[concurrentios];
  utime_t stopTime;
  int r = 0;
  bufferlist b_write;
  lock_cond lc(&lock);
  utime_t runtime;
  utime_t timePassed;

  r = completions_init(concurrentios);

  //set up writes so I can start them together
  for (int i = 0; i<concurrentios; ++i) {
    name[i] = generate_object_name(i);
    contents[i] = new bufferlist();
    snprintf(data.object_contents, data.object_size, "I'm the %16dth object!", i);
    contents[i]->append(data.object_contents, data.object_size);
  }

  pthread_t print_thread;

  pthread_create(&print_thread, NULL, ObjBencher::status_printer, (void *)this);
  lock.Lock();
  data.start_time = ceph_clock_now(g_ceph_context);
  lock.Unlock();
  for (int i = 0; i<concurrentios; ++i) {
    start_times[i] = ceph_clock_now(g_ceph_context);
    r = create_completion(i, _aio_cb, (void *)&lc);
    if (r < 0)
      goto ERR;
    r = aio_write(name[i], i, *contents[i], data.object_size);
    if (r < 0) { //naughty, doesn't clean up heap
      goto ERR;
    }
    lock.Lock();
    ++data.started;
    ++data.in_flight;
    lock.Unlock();
  }

  //keep on adding new writes as old ones complete until we've passed minimum time
  int slot;
  bufferlist* newContents;

  //don't need locking for reads because other thread doesn't write

  runtime.set_from_double(secondsToRun);
  stopTime = data.start_time + runtime;
  slot = 0;
  while( ceph_clock_now(g_ceph_context) < stopTime ) {
    lock.Lock();
    bool found = false;
    while (1) {
      int old_slot = slot;
      do {
	if (completion_is_done(slot)) {
          found = true;
	  break;
	}
        slot++;
        if (slot == concurrentios) {
          slot = 0;
        }
      } while (slot != old_slot);
      if (found)
        break;
      lc.cond.Wait(lock);
    }
    lock.Unlock();
    //create new contents and name on the heap, and fill them
    newContents = new bufferlist();
    newName = generate_object_name(data.started);
    snprintf(data.object_contents, data.object_size, "I'm the %16dth object!", data.started);
    newContents->append(data.object_contents, data.object_size);
    completion_wait(slot);
    lock.Lock();
    r = completion_ret(slot);
    if (r != 0) {
      lock.Unlock();
      goto ERR;
    }
    data.cur_latency = ceph_clock_now(g_ceph_context) - start_times[slot];
    data.history.latency.push_back(data.cur_latency);
    total_latency += data.cur_latency;
    if( data.cur_latency > data.max_latency) data.max_latency = data.cur_latency;
    if (data.cur_latency < data.min_latency) data.min_latency = data.cur_latency;
    ++data.finished;
    data.avg_latency = total_latency / data.finished;
    --data.in_flight;
    lock.Unlock();
    release_completion(slot);
    timePassed = ceph_clock_now(g_ceph_context) - data.start_time;

    //write new stuff to backend, then delete old stuff
    //and save locations of new stuff for later deletion
    start_times[slot] = ceph_clock_now(g_ceph_context);
    r = create_completion(slot, _aio_cb, &lc);
    if (r < 0)
      goto ERR;
    r = aio_write(newName, slot, *newContents, data.object_size);
    if (r < 0) {//naughty; doesn't clean up heap space.
      goto ERR;
    }
    lock.Lock();
    ++data.started;
    ++data.in_flight;
    lock.Unlock();
    delete contents[slot];
    name[slot] = newName;
    contents[slot] = newContents;
  }

  while (data.finished < data.started) {
    slot = data.finished % concurrentios;
    completion_wait(slot);
    lock.Lock();
    r = completion_ret(slot);
    if (r != 0) {
      lock.Unlock();
      goto ERR;
    }
    data.cur_latency = ceph_clock_now(g_ceph_context) - start_times[slot];
    data.history.latency.push_back(data.cur_latency);
    total_latency += data.cur_latency;
    if (data.cur_latency > data.max_latency) data.max_latency = data.cur_latency;
    if (data.cur_latency < data.min_latency) data.min_latency = data.cur_latency;
    ++data.finished;
    data.avg_latency = total_latency / data.finished;
    --data.in_flight;
    lock.Unlock();
    release_completion(slot);
    delete contents[slot];
  }

  timePassed = ceph_clock_now(g_ceph_context) - data.start_time;
  lock.Lock();
  data.done = true;
  lock.Unlock();

  pthread_join(print_thread, NULL);

  double bandwidth;
  bandwidth = ((double)data.finished)*((double)data.object_size)/(double)timePassed;
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec
  char bw[20];
  snprintf(bw, sizeof(bw), "%.3lf \n", bandwidth);

  out(cout) << "Total time run:         " << timePassed << std::endl
       << "Total writes made:      " << data.finished << std::endl
       << "Write size:             " << data.object_size << std::endl
       << "Bandwidth (MB/sec):     " << bw << std::endl
       << "Stddev Bandwidth:       " << vec_stddev(data.history.bandwidth) << std::endl
       << "Max bandwidth (MB/sec): " << data.idata.max_bandwidth << std::endl
       << "Min bandwidth (MB/sec): " << data.idata.min_bandwidth << std::endl
       << "Average Latency:        " << data.avg_latency << std::endl
       << "Stddev Latency:         " << vec_stddev(data.history.latency) << std::endl
       << "Max latency:            " << data.max_latency << std::endl
       << "Min latency:            " << data.min_latency << std::endl;

  //write object size/number data for read benchmarks
  ::encode(data.object_size, b_write);
  ::encode(data.finished, b_write);
  ::encode(getpid(), b_write);
  sync_write(BENCH_METADATA, b_write, sizeof(int)*3);

  completions_done();

  return 0;

 ERR:
  lock.Lock();
  data.done = 1;
  lock.Unlock();
  pthread_join(print_thread, NULL);
  return -5;
}

int ObjBencher::seq_read_bench(int seconds_to_run, int num_objects, int concurrentios, int pid) {
  data.finished = 0;

  lock_cond lc(&lock);
  std::string name[concurrentios];
  std::string newName;
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
  sanitize_object_contents(&data, data.object_size); //clean it up once; subsequent
  //changes will be safe because string length should remain the same

  r = completions_init(concurrentios);
  if (r < 0)
    return r;

  //set up initial reads
  for (int i = 0; i < concurrentios; ++i) {
    name[i] = generate_object_name(i, pid);
    contents[i] = new bufferlist();
  }

  pthread_t print_thread;
  pthread_create(&print_thread, NULL, status_printer, (void *)this);

  lock.Lock();
  data.start_time = ceph_clock_now(g_ceph_context);
  lock.Unlock();
  utime_t finish_time = data.start_time + time_to_run;
  //start initial reads
  for (int i = 0; i < concurrentios; ++i) {
    index[i] = i;
    start_times[i] = ceph_clock_now(g_ceph_context);
    create_completion(i, _aio_cb, (void *)&lc);
    r = aio_read(name[i], i, contents[i], data.object_size);
    if (r < 0) { //naughty, doesn't clean up heap -- oh, or handle the print thread!
      cerr << "r = " << r << std::endl;
      goto ERR;
    }
    lock.Lock();
    ++data.started;
    ++data.in_flight;
    lock.Unlock();
  }

  //keep on adding new reads as old ones complete
  int slot;
  bufferlist *cur_contents;

  slot = 0;
  while (seconds_to_run && (ceph_clock_now(g_ceph_context) < finish_time) &&
      num_objects > data.started) {
    lock.Lock();
    int old_slot = slot;
    bool found = false;
    while (1) {
      do {
	if (completion_is_done(slot)) {
          found = true;
	  break;
	}
        slot++;
        if (slot == concurrentios) {
          slot = 0;
        }
      } while (slot != old_slot);
      if (found) {
	break;
      }
      lc.cond.Wait(lock);
    }
    lock.Unlock();
    newName = generate_object_name(data.started, pid);
    int current_index = index[slot];
    index[slot] = data.started;
    completion_wait(slot);
    lock.Lock();
    r = completion_ret(slot);
    if (r != 0) {
      cerr << "read got " << r << std::endl;
      lock.Unlock();
      goto ERR;
    }
    data.cur_latency = ceph_clock_now(g_ceph_context) - start_times[slot];
    total_latency += data.cur_latency;
    if( data.cur_latency > data.max_latency) data.max_latency = data.cur_latency;
    if (data.cur_latency < data.min_latency) data.min_latency = data.cur_latency;
    ++data.finished;
    data.avg_latency = total_latency / data.finished;
    --data.in_flight;
    lock.Unlock();
    release_completion(slot);
    cur_contents = contents[slot];

    //start new read and check data if requested
    start_times[slot] = ceph_clock_now(g_ceph_context);
    contents[slot] = new bufferlist();
    create_completion(slot, _aio_cb, (void *)&lc);
    r = aio_read(newName, slot, contents[slot], data.object_size);
    if (r < 0) {
      goto ERR;
    }
    lock.Lock();
    ++data.started;
    ++data.in_flight;
    snprintf(data.object_contents, data.object_size, "I'm the %16dth object!", current_index);
    lock.Unlock();
    if (memcmp(data.object_contents, cur_contents->c_str(), data.object_size) != 0) {
      cerr << name[slot] << " is not correct!" << std::endl;
      ++errors;
    }
    name[slot] = newName;
    delete cur_contents;
  }

  //wait for final reads to complete
  while (data.finished < data.started) {
    slot = data.finished % concurrentios;
    completion_wait(slot);
    lock.Lock();
    r = completion_ret(slot);
    if (r != 0) {
      cerr << "read got " << r << std::endl;
      lock.Unlock();
      goto ERR;
    }
    data.cur_latency = ceph_clock_now(g_ceph_context) - start_times[slot];
    total_latency += data.cur_latency;
    if (data.cur_latency > data.max_latency) data.max_latency = data.cur_latency;
    if (data.cur_latency < data.min_latency) data.min_latency = data.cur_latency;
    ++data.finished;
    data.avg_latency = total_latency / data.finished;
    --data.in_flight;
    release_completion(slot);
    snprintf(data.object_contents, data.object_size, "I'm the %16dth object!", index[slot]);
    lock.Unlock();
    if (memcmp(data.object_contents, contents[slot]->c_str(), data.object_size) != 0) {
      cerr << name[slot] << " is not correct!" << std::endl;
      ++errors;
    }
    delete contents[slot];
  }

  runtime = ceph_clock_now(g_ceph_context) - data.start_time;
  lock.Lock();
  data.done = true;
  lock.Unlock();

  pthread_join(print_thread, NULL);

  double bandwidth;
  bandwidth = ((double)data.finished)*((double)data.object_size)/(double)runtime;
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec
  char bw[20];
  snprintf(bw, sizeof(bw), "%.3lf \n", bandwidth);

  out(cout) << "Total time run:        " << runtime << std::endl
       << "Total reads made:     " << data.finished << std::endl
       << "Read size:            " << data.object_size << std::endl
       << "Bandwidth (MB/sec):    " << bw << std::endl
       << "Average Latency:       " << data.avg_latency << std::endl
       << "Max latency:           " << data.max_latency << std::endl
       << "Min latency:           " << data.min_latency << std::endl;

  completions_done();

  return 0;

 ERR:
  lock.Lock();
  data.done = 1;
  lock.Unlock();
  pthread_join(print_thread, NULL);
  return -5;
}

int ObjBencher::clean_up(int num_objects, int prevPid) {
  int r = 0;

  for (int i = 0; i < num_objects; ++i) {
      std::string name = generate_object_name(i, prevPid);
      r = sync_remove(name);

      if (r < 0) {
          return r;
      }
  }

  return 0;
}


