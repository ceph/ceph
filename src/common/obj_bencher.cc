// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
#include "include/compat.h"
#include <pthread.h>
#include "common/Cond.h"
#include "obj_bencher.h"

const std::string BENCH_LASTRUN_METADATA = "benchmark_last_metadata";
const std::string BENCH_PREFIX = "benchmark_data";
const std::string BENCH_OBJ_NAME = BENCH_PREFIX + "_%s_%d_object%d";

static char cached_hostname[30] = {0};
int cached_pid = 0;

static std::string generate_object_prefix_nopid() {
  if (cached_hostname[0] == 0) {
    gethostname(cached_hostname, sizeof(cached_hostname)-1);
    cached_hostname[sizeof(cached_hostname)-1] = 0;
  }

  std::ostringstream oss;
  oss << BENCH_PREFIX << "_" << cached_hostname;
  return oss.str();
}

static std::string generate_object_prefix(int pid = 0) {
  if (pid)
    cached_pid = pid;
  else if (!cached_pid)
    cached_pid = getpid();

  std::ostringstream oss;
  oss << generate_object_prefix_nopid() << "_" << cached_pid;
  return oss.str();
}

// this is 8x faster than previous impl based on chained, deduped functions call
static std::string generate_object_name_fast(int objnum, int pid = 0)
{
  if (cached_hostname[0] == 0) {
	gethostname(cached_hostname, sizeof(cached_hostname)-1);
	cached_hostname[sizeof(cached_hostname)-1] = 0;
  }

  if (pid)
	cached_pid = pid;
  else if (!cached_pid)
	cached_pid = getpid();

  char name[512];
  int n = snprintf(&name[0], sizeof(name),  BENCH_OBJ_NAME.c_str(), cached_hostname, cached_pid, objnum);
  ceph_assert(n > 0 && n < (int)sizeof(name));
  return std::string(&name[0], (size_t)n);
}

static void sanitize_object_contents (bench_data *data, size_t length) {
  memset(data->object_contents, 'z', length);
}

ostream& ObjBencher::out(ostream& os, utime_t& t)
{
  if (show_time)
    return t.localtime(os) << " ";
  else
    return os;
}

ostream& ObjBencher::out(ostream& os)
{
  utime_t cur_time = ceph_clock_now();
  return out(os, cur_time);
}

void *ObjBencher::status_printer(void *_bencher) {
  ObjBencher *bencher = static_cast<ObjBencher *>(_bencher);
  bench_data& data = bencher->data;
  Formatter *formatter = bencher->formatter;
  ostream *outstream = bencher->outstream;
  Cond cond;
  int i = 0;
  int previous_writes = 0;
  int cycleSinceChange = 0;
  double bandwidth;
  int iops = 0;
  mono_clock::duration ONE_SECOND = std::chrono::seconds(1);
  bencher->lock.lock();
  if (formatter)
    formatter->open_array_section("datas");
  while(!data.done) {
    mono_time cur_time = mono_clock::now();
    utime_t t = ceph_clock_now();

    if (i % 20 == 0 && !formatter) {
      if (i > 0)
        t.localtime(cout)
          << " min lat: " << data.min_latency
          << " max lat: " << data.max_latency
          << " avg lat: " << data.avg_latency << std::endl;
      //I'm naughty and don't reset the fill
      bencher->out(cout, t) << setfill(' ')
          << setw(5) << "sec"
          << setw(8) << "Cur ops"
          << setw(10) << "started"
          << setw(10) << "finished"
          << setw(10) << "avg MB/s"
          << setw(10) << "cur MB/s"
          << setw(12) << "last lat(s)"
          << setw(12) << "avg lat(s)" << std::endl;
    }
    if (cycleSinceChange)
      bandwidth = (double)(data.finished - previous_writes)
        * (data.op_size)
        / (1024*1024)
        / cycleSinceChange;
    else
      bandwidth = -1;

    if (!std::isnan(bandwidth) && bandwidth > -1) {
      if (bandwidth > data.idata.max_bandwidth)
        data.idata.max_bandwidth = bandwidth;
      if (bandwidth < data.idata.min_bandwidth)
        data.idata.min_bandwidth = bandwidth;

      ++data.idata.bandwidth_cycles;
      double delta = bandwidth - data.idata.avg_bandwidth;
      data.idata.avg_bandwidth += delta / data.idata.bandwidth_cycles;
      data.idata.bandwidth_diff_sum += delta * (bandwidth - data.idata.avg_bandwidth);
    }

    if (cycleSinceChange)
      iops = (double)(data.finished - previous_writes)
        / cycleSinceChange;
    else
      iops = -1;

    if (!std::isnan(iops) && iops > -1) {
      if (iops > data.idata.max_iops)
        data.idata.max_iops = iops;
      if (iops < data.idata.min_iops)
        data.idata.min_iops = iops;

      ++data.idata.iops_cycles;
      double delta = iops - data.idata.avg_iops;
      data.idata.avg_iops += delta / data.idata.iops_cycles;
      data.idata.iops_diff_sum += delta * (iops - data.idata.avg_iops);
    }
    
    if (formatter)
      formatter->open_object_section("data");

    // elapsed will be in seconds, by default
    std::chrono::duration<double> elapsed = cur_time - data.start_time;
    double avg_bandwidth = (double) (data.op_size) * (data.finished)
      / elapsed.count() / (1024*1024);
    if (previous_writes != data.finished) {
      previous_writes = data.finished;
      cycleSinceChange = 0;
      if (!formatter) {
        bencher->out(cout, t)
	  << setfill(' ')
          << setw(5) << i
	  << ' ' << setw(7) << data.in_flight
          << ' ' << setw(9) << data.started
          << ' ' << setw(9) << data.finished
          << ' ' << setw(9) << avg_bandwidth
          << ' ' << setw(9) << bandwidth
          << ' ' << setw(11) << (double)data.cur_latency.count()
          << ' ' << setw(11) << data.avg_latency << std::endl;
      } else {
        formatter->dump_format("sec", "%d", i);
        formatter->dump_format("cur_ops", "%d", data.in_flight);
        formatter->dump_format("started", "%d", data.started);
        formatter->dump_format("finished", "%d", data.finished);
        formatter->dump_format("avg_bw", "%f", avg_bandwidth);
        formatter->dump_format("cur_bw", "%f", bandwidth);
        formatter->dump_format("last_lat", "%f", (double)data.cur_latency.count());
        formatter->dump_format("avg_lat", "%f", data.avg_latency);
      }
    }
    else {
      if (!formatter) {
        bencher->out(cout, t)
	  << setfill(' ')
          << setw(5) << i
	  << ' ' << setw(7) << data.in_flight
          << ' ' << setw(9) << data.started
          << ' ' << setw(9) << data.finished
          << ' ' << setw(9) << avg_bandwidth
	  << ' ' << setw(9) << '0'
          << ' ' << setw(11) << '-'
          << ' '<< setw(11) << data.avg_latency << std::endl;
      } else {
        formatter->dump_format("sec", "%d", i);
        formatter->dump_format("cur_ops", "%d", data.in_flight);
        formatter->dump_format("started", "%d", data.started);
        formatter->dump_format("finished", "%d", data.finished);
        formatter->dump_format("avg_bw", "%f", avg_bandwidth);
        formatter->dump_format("cur_bw", "%f", 0);
        formatter->dump_format("last_lat", "%f", 0);
        formatter->dump_format("avg_lat", "%f", data.avg_latency);
      }
    }
    if (formatter) {
      formatter->close_section(); // data
      formatter->flush(*outstream);
    }
    ++i;
    ++cycleSinceChange;
    cond.WaitInterval(bencher->lock, ONE_SECOND);
  }
  if (formatter)
    formatter->close_section(); //datas
  if (iops < 0) {
    std::chrono::duration<double> runtime = mono_clock::now() - data.start_time;
    data.idata.min_iops = data.idata.max_iops = data.finished / runtime.count();
  }
  bencher->lock.unlock();
  return NULL;
}

int ObjBencher::aio_bench(
  int operation, int secondsToRun,
  int concurrentios,
  uint64_t op_size, uint64_t object_size,
  unsigned max_objects,
  bool cleanup, bool hints,
  const std::string& run_name, bool reuse_bench, bool no_verify) {

  if (concurrentios <= 0)
    return -EINVAL;

  int num_objects = 0;
  int r = 0;
  int prev_pid = 0;
  std::chrono::duration<double> timePassed;

  // default metadata object is used if user does not specify one
  const std::string run_name_meta = (run_name.empty() ? BENCH_LASTRUN_METADATA : run_name);

  //get data from previous write run, if available
  if (operation != OP_WRITE || reuse_bench) {
    uint64_t prev_op_size, prev_object_size;
    r = fetch_bench_metadata(run_name_meta, &prev_op_size, &prev_object_size,
			     &num_objects, &prev_pid);
    if (r < 0) {
      if (r == -ENOENT) {
        if (reuse_bench)
          cerr << "Must write data before using reuse_bench for a write benchmark!" << std::endl;
        else
          cerr << "Must write data before running a read benchmark!" << std::endl;
      }
      return r;
    }
    object_size = prev_object_size;   
    op_size = prev_op_size;           
  }

  char* contentsChars = new char[op_size];
  lock.lock();
  data.done = false;
  data.hints = hints;
  data.object_size = object_size;
  data.op_size = op_size;
  data.in_flight = 0;
  data.started = 0;
  data.finished = 0;
  data.min_latency = 9999.0; // this better be higher than initial latency!
  data.max_latency = 0;
  data.avg_latency = 0;
  data.latency_diff_sum = 0;
  data.object_contents = contentsChars;
  lock.unlock();

  //fill in contentsChars deterministically so we can check returns
  sanitize_object_contents(&data, data.op_size);

  if (formatter)
    formatter->open_object_section("bench");

  if (OP_WRITE == operation) {
    r = write_bench(secondsToRun, concurrentios, run_name_meta, max_objects, prev_pid);
    if (r != 0) goto out;
  }
  else if (OP_SEQ_READ == operation) {
    r = seq_read_bench(secondsToRun, num_objects, concurrentios, prev_pid, no_verify);
    if (r != 0) goto out;
  }
  else if (OP_RAND_READ == operation) {
    r = rand_read_bench(secondsToRun, num_objects, concurrentios, prev_pid, no_verify);
    if (r != 0) goto out;
  }

  if (OP_WRITE == operation && cleanup) {
    r = fetch_bench_metadata(run_name_meta, &op_size, &object_size,
                            &num_objects, &prev_pid);
    if (r < 0) {
      if (r == -ENOENT)
        cerr << "Should never happen: bench metadata missing for current run!" << std::endl;
      goto out;
    }

    data.start_time = mono_clock::now();
    out(cout) << "Cleaning up (deleting benchmark objects)" << std::endl;

    r = clean_up(num_objects, prev_pid, concurrentios);
    if (r != 0) goto out;

    timePassed = mono_clock::now() - data.start_time;
    out(cout) << "Clean up completed and total clean up time :" << timePassed.count() << std::endl;

    // lastrun file
    r = sync_remove(run_name_meta);
    if (r != 0) goto out;
  }

 out:
  if (formatter) {
    formatter->close_section(); // bench
    formatter->flush(*outstream);
    *outstream << std::endl;
  }
  delete[] contentsChars;
  return r;
}

struct lock_cond {
  explicit lock_cond(Mutex *_lock) : lock(_lock) {}
  Mutex *lock;
  Cond cond;
};

void _aio_cb(void *cb, void *arg) {
  struct lock_cond *lc = (struct lock_cond *)arg;
  lc->lock->lock();
  lc->cond.Signal();
  lc->lock->unlock();
}

int ObjBencher::fetch_bench_metadata(const std::string& metadata_file,
				     uint64_t *op_size, uint64_t* object_size,
				     int* num_objects, int* prevPid) {
  int r = 0;
  bufferlist object_data;

  r = sync_read(metadata_file, object_data,
		sizeof(int) * 2 + sizeof(size_t) * 2);
  if (r <= 0) {
    // treat an empty file as a file that does not exist
    if (r == 0) {
      r = -ENOENT;
    }
    return r;
  }
  auto p = object_data.cbegin();
  decode(*object_size, p);
  decode(*num_objects, p);
  decode(*prevPid, p);
  if (!p.end()) {
    decode(*op_size, p);
  } else {
    *op_size = *object_size;
  }

  return 0;
}

int ObjBencher::write_bench(int secondsToRun,
			    int concurrentios, const string& run_name_meta,
			    unsigned max_objects, int prev_pid) {
  if (concurrentios <= 0) 
    return -EINVAL;
  
  if (!formatter) {
    out(cout) << "Maintaining " << concurrentios << " concurrent writes of "
	      << data.op_size << " bytes to objects of size "
	      << data.object_size << " for up to "
	      << secondsToRun << " seconds or "
	      << max_objects << " objects"
	      << std::endl;
  } else {
    formatter->dump_format("concurrent_ios", "%d", concurrentios);
    formatter->dump_format("object_size", "%d", data.object_size);
    formatter->dump_format("op_size", "%d", data.op_size);
    formatter->dump_format("seconds_to_run", "%d", secondsToRun);
    formatter->dump_format("max_objects", "%d", max_objects);
  }
  bufferlist* newContents = 0;

  std::string prefix = prev_pid ? generate_object_prefix(prev_pid) : generate_object_prefix();
  if (!formatter)
    out(cout) << "Object prefix: " << prefix << std::endl;
  else
    formatter->dump_string("object_prefix", prefix);

  std::vector<string> name(concurrentios);
  std::string newName;
  unique_ptr<bufferlist> contents[concurrentios];
  int r = 0;
  bufferlist b_write;
  lock_cond lc(&lock);
  double total_latency = 0;
  std::vector<mono_time> start_times(concurrentios);
  mono_time stopTime;
  std::chrono::duration<double> timePassed;

  unsigned writes_per_object = 1;
  if (data.op_size)
    writes_per_object = data.object_size / data.op_size;

  r = completions_init(concurrentios);

  //set up writes so I can start them together
  for (int i = 0; i<concurrentios; ++i) {
    name[i] = generate_object_name_fast(i / writes_per_object);
    contents[i] = std::make_unique<bufferlist>();
    snprintf(data.object_contents, data.op_size, "I'm the %16dth op!", i);
    contents[i]->append(data.object_contents, data.op_size);
  }

  pthread_t print_thread;

  pthread_create(&print_thread, NULL, ObjBencher::status_printer, (void *)this);
  ceph_pthread_setname(print_thread, "write_stat");
  lock.lock();
  data.finished = 0;
  data.start_time = mono_clock::now();
  lock.unlock();
  for (int i = 0; i<concurrentios; ++i) {
    start_times[i] = mono_clock::now();
    r = create_completion(i, _aio_cb, (void *)&lc);
    if (r < 0)
      goto ERR;
    r = aio_write(name[i], i, *contents[i], data.op_size,
		  data.op_size * (i % writes_per_object));
    if (r < 0) {
      goto ERR;
    }
    lock.lock();
    ++data.started;
    ++data.in_flight;
    lock.unlock();
  }

  //keep on adding new writes as old ones complete until we've passed minimum time
  int slot;
  int num_objects;

  //don't need locking for reads because other thread doesn't write

  stopTime = data.start_time + std::chrono::seconds(secondsToRun);
  slot = 0;
  lock.lock();
  while (secondsToRun && mono_clock::now() < stopTime) {
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
    lock.unlock();
    //create new contents and name on the heap, and fill them
    newName = generate_object_name_fast(data.started / writes_per_object);
    newContents = contents[slot].get();
    snprintf(newContents->c_str(), data.op_size, "I'm the %16dth op!", data.started);
    // we wrote to buffer, going around internal crc cache, so invalidate it now.
    newContents->invalidate_crc();

    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r != 0) {
      lock.unlock();
      goto ERR;
    }
    data.cur_latency = mono_clock::now() - start_times[slot];
    total_latency += data.cur_latency.count();
    if( data.cur_latency.count() > data.max_latency)
      data.max_latency = data.cur_latency.count();
    if (data.cur_latency.count() < data.min_latency)
      data.min_latency = data.cur_latency.count();
    ++data.finished;
    double delta = data.cur_latency.count() - data.avg_latency;
    data.avg_latency = total_latency / data.finished;
    data.latency_diff_sum += delta * (data.cur_latency.count() - data.avg_latency);
    --data.in_flight;
    lock.unlock();
    release_completion(slot);

    //write new stuff to backend
    start_times[slot] = mono_clock::now();
    r = create_completion(slot, _aio_cb, &lc);
    if (r < 0)
      goto ERR;
    r = aio_write(newName, slot, *newContents, data.op_size,
		  data.op_size * (data.started % writes_per_object));
    if (r < 0) {
      goto ERR;
    }
    name[slot] = newName;
    lock.lock();
    ++data.started;
    ++data.in_flight;
    if (data.op_size) {
      if (max_objects &&
	  data.started >= (int)((data.object_size * max_objects + data.op_size - 1) /
			       data.op_size))
        break;
    }
  }
  lock.unlock();

  while (data.finished < data.started) {
    slot = data.finished % concurrentios;
    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r != 0) {
      lock.unlock();
      goto ERR;
    }
    data.cur_latency = mono_clock::now() - start_times[slot];
    total_latency += data.cur_latency.count();
    if (data.cur_latency.count() > data.max_latency)
      data.max_latency = data.cur_latency.count();
    if (data.cur_latency.count() < data.min_latency)
      data.min_latency = data.cur_latency.count();
    ++data.finished;
    double delta = data.cur_latency.count() - data.avg_latency;
    data.avg_latency = total_latency / data.finished;
    data.latency_diff_sum += delta * (data.cur_latency.count() - data.avg_latency);
    --data.in_flight;
    lock.unlock();
    release_completion(slot);
  }

  timePassed = mono_clock::now() - data.start_time;
  lock.lock();
  data.done = true;
  lock.unlock();

  pthread_join(print_thread, NULL);

  double bandwidth;
  bandwidth = ((double)data.finished)*((double)data.op_size) /
       timePassed.count();
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec

  double bandwidth_stddev;
  double iops_stddev;
  double latency_stddev;
  if (data.idata.bandwidth_cycles > 1) {
    bandwidth_stddev = std::sqrt(data.idata.bandwidth_diff_sum / (data.idata.bandwidth_cycles - 1));
  } else {
    bandwidth_stddev = 0;
  }
  if (data.idata.iops_cycles > 1) {
    iops_stddev = std::sqrt(data.idata.iops_diff_sum / (data.idata.iops_cycles - 1));
  } else {
    iops_stddev = 0;
  }
  if (data.finished > 1) {
    latency_stddev = std::sqrt(data.latency_diff_sum / (data.finished - 1));
  } else {
    latency_stddev = 0;
  }

  if (!formatter) {
    out(cout) << "Total time run:         " << timePassed.count() << std::endl
       << "Total writes made:      " << data.finished << std::endl
       << "Write size:             " << data.op_size << std::endl
       << "Object size:            " << data.object_size << std::endl      
       << "Bandwidth (MB/sec):     " << setprecision(6) << bandwidth << std::endl
       << "Stddev Bandwidth:       " << bandwidth_stddev << std::endl
       << "Max bandwidth (MB/sec): " << data.idata.max_bandwidth << std::endl
       << "Min bandwidth (MB/sec): " << data.idata.min_bandwidth << std::endl
       << "Average IOPS:           " << (int)(data.finished/timePassed.count()) << std::endl
       << "Stddev IOPS:            " << iops_stddev << std::endl
       << "Max IOPS:               " << data.idata.max_iops << std::endl
       << "Min IOPS:               " << data.idata.min_iops << std::endl
       << "Average Latency(s):     " << data.avg_latency << std::endl
       << "Stddev Latency(s):      " << latency_stddev << std::endl
       << "Max latency(s):         " << data.max_latency << std::endl
       << "Min latency(s):         " << data.min_latency << std::endl;
  } else {
    formatter->dump_format("total_time_run", "%f", timePassed.count());
    formatter->dump_format("total_writes_made", "%d", data.finished);
    formatter->dump_format("write_size", "%d", data.op_size);
    formatter->dump_format("object_size", "%d", data.object_size);
    formatter->dump_format("bandwidth", "%f", bandwidth);
    formatter->dump_format("stddev_bandwidth", "%f", bandwidth_stddev);
    formatter->dump_format("max_bandwidth", "%f", data.idata.max_bandwidth);
    formatter->dump_format("min_bandwidth", "%f", data.idata.min_bandwidth);
    formatter->dump_format("average_iops", "%d", (int)(data.finished/timePassed.count()));
    formatter->dump_format("stddev_iops", "%d", iops_stddev);
    formatter->dump_format("max_iops", "%d", data.idata.max_iops);
    formatter->dump_format("min_iops", "%d", data.idata.min_iops);
    formatter->dump_format("average_latency", "%f", data.avg_latency);
    formatter->dump_format("stddev_latency", "%f", latency_stddev);
    formatter->dump_format("max_latency", "%f", data.max_latency);
    formatter->dump_format("min_latency", "%f", data.min_latency);
  }
  //write object size/number data for read benchmarks
  encode(data.object_size, b_write);
  num_objects = (data.finished + writes_per_object - 1) / writes_per_object;
  encode(num_objects, b_write);
  encode(prev_pid ? prev_pid : getpid(),  b_write);
  encode(data.op_size, b_write);

  // persist meta-data for further cleanup or read
  sync_write(run_name_meta, b_write, sizeof(int)*3);

  completions_done();

  return 0;

 ERR:
  lock.lock();
  data.done = 1;
  lock.unlock();
  pthread_join(print_thread, NULL);
  return r;
}

int ObjBencher::seq_read_bench(int seconds_to_run, int num_objects, int concurrentios, int pid, bool no_verify) {
  lock_cond lc(&lock);

  if (concurrentios <= 0) 
    return -EINVAL;

  std::vector<string> name(concurrentios);
  std::string newName;
  unique_ptr<bufferlist> contents[concurrentios];
  int index[concurrentios];
  int errors = 0;
  double total_latency = 0;
  int r = 0;
  std::vector<mono_time> start_times(concurrentios);
  mono_clock::duration time_to_run = std::chrono::seconds(seconds_to_run);
  std::chrono::duration<double> timePassed;
  sanitize_object_contents(&data, data.op_size); //clean it up once; subsequent
  //changes will be safe because string length should remain the same

  unsigned reads_per_object = 1;
  if (data.op_size)
    reads_per_object = data.object_size / data.op_size;

  r = completions_init(concurrentios);
  if (r < 0)
    return r;

  //set up initial reads
  for (int i = 0; i < concurrentios; ++i) {
    name[i] = generate_object_name_fast(i / reads_per_object, pid);
    contents[i] = std::make_unique<bufferlist>();
  }

  lock.lock();
  data.finished = 0;
  data.start_time = mono_clock::now();
  lock.unlock();

  pthread_t print_thread;
  pthread_create(&print_thread, NULL, status_printer, (void *)this);
  ceph_pthread_setname(print_thread, "seq_read_stat");

  mono_time finish_time = data.start_time + time_to_run;
  //start initial reads
  for (int i = 0; i < concurrentios; ++i) {
    index[i] = i;
    start_times[i] = mono_clock::now();
    create_completion(i, _aio_cb, (void *)&lc);
    r = aio_read(name[i], i, contents[i].get(), data.op_size,
		 data.op_size * (i % reads_per_object));
    if (r < 0) {
      cerr << "r = " << r << std::endl;
      goto ERR;
    }
    lock.lock();
    ++data.started;
    ++data.in_flight;
    lock.unlock();
  }

  //keep on adding new reads as old ones complete
  int slot;
  bufferlist *cur_contents;

  slot = 0;
  while ((seconds_to_run && mono_clock::now() < finish_time) &&
	 num_objects > data.started) {
    lock.lock();
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

    // calculate latency here, so memcmp doesn't inflate it
    data.cur_latency = mono_clock::now() - start_times[slot];

    cur_contents = contents[slot].get();
    int current_index = index[slot];
    
    // invalidate internal crc cache
    cur_contents->invalidate_crc();
  
    if (!no_verify) {
      snprintf(data.object_contents, data.op_size, "I'm the %16dth op!", current_index);
      if ( (cur_contents->length() != data.op_size) || 
           (memcmp(data.object_contents, cur_contents->c_str(), data.op_size) != 0) ) {
        cerr << name[slot] << " is not correct!" << std::endl;
        ++errors;
      }
    }

    newName = generate_object_name_fast(data.started / reads_per_object, pid);
    index[slot] = data.started;
    lock.unlock();
    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r < 0) {
      cerr << "read got " << r << std::endl;
      lock.unlock();
      goto ERR;
    }
    total_latency += data.cur_latency.count();
    if (data.cur_latency.count() > data.max_latency)
      data.max_latency = data.cur_latency.count();
    if (data.cur_latency.count() < data.min_latency)
      data.min_latency = data.cur_latency.count();
    ++data.finished;
    data.avg_latency = total_latency / data.finished;
    --data.in_flight;
    lock.unlock();
    release_completion(slot);

    //start new read and check data if requested
    start_times[slot] = mono_clock::now();
    create_completion(slot, _aio_cb, (void *)&lc);
    r = aio_read(newName, slot, contents[slot].get(), data.op_size,
		 data.op_size * (data.started % reads_per_object));
    if (r < 0) {
      goto ERR;
    }
    lock.lock();
    ++data.started;
    ++data.in_flight;
    lock.unlock();
    name[slot] = newName;
  }

  //wait for final reads to complete
  while (data.finished < data.started) {
    slot = data.finished % concurrentios;
    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r < 0) {
      cerr << "read got " << r << std::endl;
      lock.unlock();
      goto ERR;
    }
    data.cur_latency = mono_clock::now() - start_times[slot];
    total_latency += data.cur_latency.count();
    if (data.cur_latency.count() > data.max_latency)
      data.max_latency = data.cur_latency.count();
    if (data.cur_latency.count() < data.min_latency)
      data.min_latency = data.cur_latency.count();
    ++data.finished;
    data.avg_latency = total_latency / data.finished;
    --data.in_flight;
    release_completion(slot);
    if (!no_verify) {
      snprintf(data.object_contents, data.op_size, "I'm the %16dth op!", index[slot]);
      lock.unlock();
      if ((contents[slot]->length() != data.op_size) || 
         (memcmp(data.object_contents, contents[slot]->c_str(), data.op_size) != 0)) {
        cerr << name[slot] << " is not correct!" << std::endl;
        ++errors;
      }
    } else {
        lock.unlock();
    }
  }

  timePassed = mono_clock::now() - data.start_time;
  lock.lock();
  data.done = true;
  lock.unlock();

  pthread_join(print_thread, NULL);

  double bandwidth;
  bandwidth = ((double)data.finished)*((double)data.op_size)/timePassed.count();
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec
  
  double iops_stddev;
  if (data.idata.iops_cycles > 1) {
    iops_stddev = std::sqrt(data.idata.iops_diff_sum / (data.idata.iops_cycles - 1));
  } else {
    iops_stddev = 0;
  }

  if (!formatter) {
    out(cout) << "Total time run:       " << timePassed.count() << std::endl
       << "Total reads made:     " << data.finished << std::endl
       << "Read size:            " << data.op_size << std::endl
       << "Object size:          " << data.object_size << std::endl
       << "Bandwidth (MB/sec):   " << setprecision(6) << bandwidth << std::endl
       << "Average IOPS:         " << (int)(data.finished/timePassed.count()) << std::endl
       << "Stddev IOPS:          " << iops_stddev << std::endl
       << "Max IOPS:             " << data.idata.max_iops << std::endl
       << "Min IOPS:             " << data.idata.min_iops << std::endl
       << "Average Latency(s):   " << data.avg_latency << std::endl
       << "Max latency(s):       " << data.max_latency << std::endl
       << "Min latency(s):       " << data.min_latency << std::endl;
  } else {
    formatter->dump_format("total_time_run", "%f", timePassed.count());
    formatter->dump_format("total_reads_made", "%d", data.finished);
    formatter->dump_format("read_size", "%d", data.op_size);
    formatter->dump_format("object_size", "%d", data.object_size);
    formatter->dump_format("bandwidth", "%f", bandwidth);
    formatter->dump_format("average_iops", "%d", (int)(data.finished/timePassed.count()));
    formatter->dump_format("stddev_iops", "%f", iops_stddev);
    formatter->dump_format("max_iops", "%d", data.idata.max_iops);
    formatter->dump_format("min_iops", "%d", data.idata.min_iops);
    formatter->dump_format("average_latency", "%f", data.avg_latency);
    formatter->dump_format("max_latency", "%f", data.max_latency);
    formatter->dump_format("min_latency", "%f", data.min_latency);
  }

  completions_done();

  return (errors > 0 ? -EIO : 0);

 ERR:
  lock.lock();
  data.done = 1;
  lock.unlock();
  pthread_join(print_thread, NULL);
  return r;
}

int ObjBencher::rand_read_bench(int seconds_to_run, int num_objects, int concurrentios, int pid, bool no_verify)
{
  lock_cond lc(&lock);

  if (concurrentios <= 0)
    return -EINVAL;

  std::vector<string> name(concurrentios);
  std::string newName;
  unique_ptr<bufferlist> contents[concurrentios];
  int index[concurrentios];
  int errors = 0;
  int r = 0;
  double total_latency = 0;
  std::vector<mono_time> start_times(concurrentios);
  mono_clock::duration time_to_run = std::chrono::seconds(seconds_to_run);
  std::chrono::duration<double> timePassed;
  sanitize_object_contents(&data, data.op_size); //clean it up once; subsequent
  //changes will be safe because string length should remain the same

  unsigned reads_per_object = 1;
  if (data.op_size)
    reads_per_object = data.object_size / data.op_size;

  srand (time(NULL));

  r = completions_init(concurrentios);
  if (r < 0)
    return r;

  //set up initial reads
  for (int i = 0; i < concurrentios; ++i) {
    name[i] = generate_object_name_fast(i / reads_per_object, pid);
    contents[i] = std::make_unique<bufferlist>();
  }

  lock.lock();
  data.finished = 0;
  data.start_time = mono_clock::now();
  lock.unlock();

  pthread_t print_thread;
  pthread_create(&print_thread, NULL, status_printer, (void *)this);
  ceph_pthread_setname(print_thread, "rand_read_stat");

  mono_time finish_time = data.start_time + time_to_run;
  //start initial reads
  for (int i = 0; i < concurrentios; ++i) {
    index[i] = i;
    start_times[i] = mono_clock::now();
    create_completion(i, _aio_cb, (void *)&lc);
    r = aio_read(name[i], i, contents[i].get(), data.op_size,
		 data.op_size * (i % reads_per_object));
    if (r < 0) {
      cerr << "r = " << r << std::endl;
      goto ERR;
    }
    lock.lock();
    ++data.started;
    ++data.in_flight;
    lock.unlock();
  }

  //keep on adding new reads as old ones complete
  int slot;
  bufferlist *cur_contents;
  int rand_id;

  slot = 0;
  while ((seconds_to_run && mono_clock::now() < finish_time)) {
    lock.lock();
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

    // calculate latency here, so memcmp doesn't inflate it
    data.cur_latency = mono_clock::now() - start_times[slot];

    lock.unlock();

    int current_index = index[slot];
    cur_contents = contents[slot].get();
    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r < 0) {
      cerr << "read got " << r << std::endl;
      lock.unlock();
      goto ERR;
    }

    total_latency += data.cur_latency.count();
    if (data.cur_latency.count() > data.max_latency)
      data.max_latency = data.cur_latency.count();
    if (data.cur_latency.count() < data.min_latency)
      data.min_latency = data.cur_latency.count();
    ++data.finished;
    data.avg_latency = total_latency / data.finished;
    --data.in_flight;
    lock.unlock();
    
    if (!no_verify) {
      snprintf(data.object_contents, data.op_size, "I'm the %16dth op!", current_index);
      if ((cur_contents->length() != data.op_size) || 
          (memcmp(data.object_contents, cur_contents->c_str(), data.op_size) != 0)) {
        cerr << name[slot] << " is not correct!" << std::endl;
        ++errors;
      }
    } 

    rand_id = rand() % num_objects;
    newName = generate_object_name_fast(rand_id / reads_per_object, pid);
    index[slot] = rand_id;
    release_completion(slot);

    // invalidate internal crc cache
    cur_contents->invalidate_crc();

    //start new read and check data if requested
    start_times[slot] = mono_clock::now();
    create_completion(slot, _aio_cb, (void *)&lc);
    r = aio_read(newName, slot, contents[slot].get(), data.op_size,
		 data.op_size * (rand_id % reads_per_object));
    if (r < 0) {
      goto ERR;
    }
    lock.lock();
    ++data.started;
    ++data.in_flight;
    lock.unlock();
    name[slot] = newName;
  }


  //wait for final reads to complete
  while (data.finished < data.started) {
    slot = data.finished % concurrentios;
    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r < 0) {
      cerr << "read got " << r << std::endl;
      lock.unlock();
      goto ERR;
    }
    data.cur_latency = mono_clock::now() - start_times[slot];
    total_latency += data.cur_latency.count();
    if (data.cur_latency.count() > data.max_latency)
      data.max_latency = data.cur_latency.count();
    if (data.cur_latency.count() < data.min_latency)
      data.min_latency = data.cur_latency.count();
    ++data.finished;
    data.avg_latency = total_latency / data.finished;
    --data.in_flight;
    release_completion(slot);
    if (!no_verify) {
      snprintf(data.object_contents, data.op_size, "I'm the %16dth op!", index[slot]);
      lock.unlock();
      if ((contents[slot]->length() != data.op_size) || 
          (memcmp(data.object_contents, contents[slot]->c_str(), data.op_size) != 0)) {
        cerr << name[slot] << " is not correct!" << std::endl;
        ++errors;
      }
    } else {
        lock.unlock();
    }
  }

  timePassed = mono_clock::now() - data.start_time;
  lock.lock();
  data.done = true;
  lock.unlock();

  pthread_join(print_thread, NULL);

  double bandwidth;
  bandwidth = ((double)data.finished)*((double)data.op_size)/timePassed.count();
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec
  
  double iops_stddev;
  if (data.idata.iops_cycles > 1) {
    iops_stddev = std::sqrt(data.idata.iops_diff_sum / (data.idata.iops_cycles - 1));
  } else {
    iops_stddev = 0;
  }

  if (!formatter) {
    out(cout) << "Total time run:       " << timePassed.count() << std::endl
       << "Total reads made:     " << data.finished << std::endl
       << "Read size:            " << data.op_size << std::endl
       << "Object size:          " << data.object_size << std::endl
       << "Bandwidth (MB/sec):   " << setprecision(6) << bandwidth << std::endl
       << "Average IOPS:         " << (int)(data.finished/timePassed.count()) << std::endl
       << "Stddev IOPS:          " << iops_stddev << std::endl
       << "Max IOPS:             " << data.idata.max_iops << std::endl
       << "Min IOPS:             " << data.idata.min_iops << std::endl
       << "Average Latency(s):   " << data.avg_latency << std::endl
       << "Max latency(s):       " << data.max_latency << std::endl
       << "Min latency(s):       " << data.min_latency << std::endl;
  } else {
    formatter->dump_format("total_time_run", "%f", timePassed.count());
    formatter->dump_format("total_reads_made", "%d", data.finished);
    formatter->dump_format("read_size", "%d", data.op_size);
    formatter->dump_format("object_size", "%d", data.object_size);
    formatter->dump_format("bandwidth", "%f", bandwidth);
    formatter->dump_format("average_iops", "%d", (int)(data.finished/timePassed.count()));
    formatter->dump_format("stddev_iops", "%f", iops_stddev);
    formatter->dump_format("max_iops", "%d", data.idata.max_iops);
    formatter->dump_format("min_iops", "%d", data.idata.min_iops);
    formatter->dump_format("average_latency", "%f", data.avg_latency);
    formatter->dump_format("max_latency", "%f", data.max_latency);
    formatter->dump_format("min_latency", "%f", data.min_latency);
  }
  completions_done();

  return (errors > 0 ? -EIO : 0);

 ERR:
  lock.lock();
  data.done = 1;
  lock.unlock();
  pthread_join(print_thread, NULL);
  return r;
}

int ObjBencher::clean_up(const std::string& orig_prefix, int concurrentios, const std::string& run_name) {
  int r = 0;
  uint64_t op_size, object_size;
  int num_objects;
  int prevPid;

  // default meta object if user does not specify one
  const std::string run_name_meta = (run_name.empty() ? BENCH_LASTRUN_METADATA : run_name);
  const std::string prefix = (orig_prefix.empty() ? generate_object_prefix_nopid() : orig_prefix);

  if (prefix.substr(0, BENCH_PREFIX.length()) != BENCH_PREFIX) {
    cerr << "Specified --prefix invalid, it must begin with \"" << BENCH_PREFIX << "\"" << std::endl;
    return -EINVAL;
  }

  std::list<Object> unfiltered_objects;
  std::set<std::string> meta_namespaces, all_namespaces;

  // If caller set all_nspaces this will be searching
  // across multiple namespaces.
  while (true) {
    bool objects_remain = get_objects(&unfiltered_objects, 20);
    if (!objects_remain)
      break;

    std::list<Object>::const_iterator i = unfiltered_objects.begin();
    for ( ; i != unfiltered_objects.end(); ++i) {
      if (i->first == run_name_meta) {
        meta_namespaces.insert(i->second);
      }
      if (i->first.substr(0, prefix.length()) == prefix) {
        all_namespaces.insert(i->second);
      }
    }
  }

  std::set<std::string>::const_iterator i = all_namespaces.begin();
  for ( ; i != all_namespaces.end(); ++i) {
    set_namespace(*i);

    // if no metadata file found we should try to do a linear search on the prefix
    if (meta_namespaces.find(*i) == meta_namespaces.end()) {
      int r = clean_up_slow(prefix, concurrentios);
      if (r < 0) {
        cerr << "clean_up_slow error r= " << r << std::endl;
        return r;
      }
      continue;
    }

    r = fetch_bench_metadata(run_name_meta, &op_size, &object_size, &num_objects, &prevPid);
    if (r < 0) {
      return r;
    }

    r = clean_up(num_objects, prevPid, concurrentios);
    if (r != 0) return r;

    r = sync_remove(run_name_meta);
    if (r != 0) return r;
  }

  return 0;
}

int ObjBencher::clean_up(int num_objects, int prevPid, int concurrentios) {
  lock_cond lc(&lock);
  
  if (concurrentios <= 0) 
    return -EINVAL;

  std::vector<string> name(concurrentios);
  std::string newName;
  int r = 0;
  int slot = 0;

  lock.lock();
  data.done = false;
  data.in_flight = 0;
  data.started = 0;
  data.finished = 0;
  lock.unlock();

  // don't start more completions than files
  if (num_objects == 0) {
    return 0;
  } else if (num_objects < concurrentios) {
    concurrentios = num_objects;
  }

  r = completions_init(concurrentios);
  if (r < 0)
    return r;

  //set up initial removes
  for (int i = 0; i < concurrentios; ++i) {
    name[i] = generate_object_name_fast(i, prevPid);
  }

  //start initial removes
  for (int i = 0; i < concurrentios; ++i) {
    create_completion(i, _aio_cb, (void *)&lc);
    r = aio_remove(name[i], i);
    if (r < 0) { //naughty, doesn't clean up heap
      cerr << "r = " << r << std::endl;
      goto ERR;
    }
    lock.lock();
    ++data.started;
    ++data.in_flight;
    lock.unlock();
  }

  //keep on adding new removes as old ones complete
  while (data.started < num_objects) {
    lock.lock();
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
    lock.unlock();
    newName = generate_object_name_fast(data.started, prevPid);
    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r != 0 && r != -ENOENT) { // file does not exist
      cerr << "remove got " << r << std::endl;
      lock.unlock();
      goto ERR;
    }
    ++data.finished;
    --data.in_flight;
    lock.unlock();
    release_completion(slot);

    //start new remove and check data if requested
    create_completion(slot, _aio_cb, (void *)&lc);
    r = aio_remove(newName, slot);
    if (r < 0) {
      goto ERR;
    }
    lock.lock();
    ++data.started;
    ++data.in_flight;
    lock.unlock();
    name[slot] = newName;
  }

  //wait for final removes to complete
  while (data.finished < data.started) {
    slot = data.finished % concurrentios;
    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r != 0 && r != -ENOENT) { // file does not exist
      cerr << "remove got " << r << std::endl;
      lock.unlock();
      goto ERR;
    }
    ++data.finished;
    --data.in_flight;
    release_completion(slot);
    lock.unlock();
  }

  lock.lock();
  data.done = true;
  lock.unlock();

  completions_done();

  out(cout) << "Removed " << data.finished << " object" << (data.finished != 1 ? "s" : "") << std::endl;

  return 0;

 ERR:
  lock.lock();
  data.done = 1;
  lock.unlock();
  return r;
}

/**
 * Return objects from the datastore which match a prefix.
 *
 * Clears the list and populates it with any objects which match the
 * prefix. The list is guaranteed to have at least one item when the
 * function returns true.
 *
 * @param prefix the prefix to match against
 * @param objects [out] return list of objects
 * @returns true if there are any objects in the store which match
 * the prefix, false if there are no more
 */
bool ObjBencher::more_objects_matching_prefix(const std::string& prefix, std::list<Object>* objects) {
  std::list<Object> unfiltered_objects;

  objects->clear();

  while (objects->empty()) {
    bool objects_remain = get_objects(&unfiltered_objects, 20);
    if (!objects_remain)
      return false;

    std::list<Object>::const_iterator i = unfiltered_objects.begin();
    for ( ; i != unfiltered_objects.end(); ++i) {
      if (i->first.substr(0, prefix.length()) == prefix) {
        objects->push_back(*i);
      }
    }
  }

  return true;
}

int ObjBencher::clean_up_slow(const std::string& prefix, int concurrentios) {
  lock_cond lc(&lock);

  if (concurrentios <= 0) 
    return -EINVAL;

  std::vector<Object> name(concurrentios);
  Object newName;
  int r = 0;
  int slot = 0;
  std::list<Object> objects;
  bool objects_remain = true;

  lock.lock();
  data.done = false;
  data.in_flight = 0;
  data.started = 0;
  data.finished = 0;
  lock.unlock();

  out(cout) << "Warning: using slow linear search" << std::endl;

  r = completions_init(concurrentios);
  if (r < 0)
    return r;

  //set up initial removes
  for (int i = 0; i < concurrentios; ++i) {
    if (objects.empty()) {
      // if there are fewer objects than concurrent ios, don't generate extras
      bool objects_found = more_objects_matching_prefix(prefix, &objects);
      if (!objects_found) {
        concurrentios = i;
        objects_remain = false;
        break;
      }
    }

    name[i] = objects.front();
    objects.pop_front();
  }

  //start initial removes
  for (int i = 0; i < concurrentios; ++i) {
    create_completion(i, _aio_cb, (void *)&lc);
    set_namespace(name[i].second);
    r = aio_remove(name[i].first, i);
    if (r < 0) { //naughty, doesn't clean up heap
      cerr << "r = " << r << std::endl;
      goto ERR;
    }
    lock.lock();
    ++data.started;
    ++data.in_flight;
    lock.unlock();
  }

  //keep on adding new removes as old ones complete
  while (objects_remain) {
    lock.lock();
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
    lock.unlock();

    // get more objects if necessary
    if (objects.empty()) {
      objects_remain = more_objects_matching_prefix(prefix, &objects);
      // quit if there are no more
      if (!objects_remain) {
        break;
      }
    }

    // get the next object
    newName = objects.front();
    objects.pop_front();

    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r != 0 && r != -ENOENT) { // file does not exist
      cerr << "remove got " << r << std::endl;
      lock.unlock();
      goto ERR;
    }
    ++data.finished;
    --data.in_flight;
    lock.unlock();
    release_completion(slot);

    //start new remove and check data if requested
    create_completion(slot, _aio_cb, (void *)&lc);
    set_namespace(newName.second);
    r = aio_remove(newName.first, slot);
    if (r < 0) {
      goto ERR;
    }
    lock.lock();
    ++data.started;
    ++data.in_flight;
    lock.unlock();
    name[slot] = newName;
  }

  //wait for final removes to complete
  while (data.finished < data.started) {
    slot = data.finished % concurrentios;
    completion_wait(slot);
    lock.lock();
    r = completion_ret(slot);
    if (r != 0 && r != -ENOENT) { // file does not exist
      cerr << "remove got " << r << std::endl;
      lock.unlock();
      goto ERR;
    }
    ++data.finished;
    --data.in_flight;
    release_completion(slot);
    lock.unlock();
  }

  lock.lock();
  data.done = true;
  lock.unlock();

  completions_done();

  out(cout) << "Removed " << data.finished << " object" << (data.finished != 1 ? "s" : "") << std::endl;

  return 0;

 ERR:
  lock.lock();
  data.done = 1;
  lock.unlock();
  return -EIO;
}
