/*
 * Generate latency statistics for a configurable number of write
 * operations of configurable size.
 *
 *  Created on: May 21, 2012
 *      Author: Eleanor Cawthon
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/utime.h"
#include "global/global_context.h"
#include "common/ceph_argparse.h"
#include "test/omap_bench.h"

#include <string>
#include <iostream>
#include <cassert>
#include <climits>
#include <cmath>

using namespace std;
using ceph::bufferlist;

int OmapBench::setup(int argc, const char** argv) {
  //parse key_value_store_bench args
  vector<const char*> args;
  argv_to_vec(argc,argv,args);
  for (unsigned i = 0; i < args.size(); i++) {
    if(i < args.size() - 1) {
      if (strcmp(args[i], "-t") == 0) {
	threads = atoi(args[i+1]);
      } else if (strcmp(args[i], "-o") == 0) {
	objects = atoi(args[i+1]);
      } else if (strcmp(args[i], "--entries") == 0) {
	entries_per_omap = atoi(args[i+1]);
      } else if (strcmp(args[i], "--keysize") == 0) {
	key_size = atoi(args[i+1]);
      } else if (strcmp(args[i], "--valsize") == 0) {
	value_size = atoi(args[i+1]);
      } else if (strcmp(args[i], "--inc") == 0) {
	increment = atoi(args[i+1]);
      } else if (strcmp(args[i], "--omaptype") == 0) {
	if(strcmp("rand",args[i+1]) == 0) {
	  omap_generator = OmapBench::generate_non_uniform_omap;
	}
	else if (strcmp("uniform", args[i+1]) == 0) {
	  omap_generator = OmapBench::generate_uniform_omap;
	}
      } else if (strcmp(args[i], "--name") == 0) {
	rados_id = args[i+1];
      }
    } else if (strcmp(args[i], "--help") == 0) {
      cout << "\nUsage: ostorebench [options]\n"
	   << "Generate latency statistics for a configurable number of "
	   << "key value pair operations of\n"
	   << "configurable size.\n\n"
	   << "OPTIONS\n"
	   << "	-t              number of threads to use (default "<<threads;
      cout << ")\n"
	   << "	-o              number of objects to write (default "<<objects;
      cout << ")\n"
	   << "	--entries       number of entries per (default "
	   << entries_per_omap;
      cout <<")\n"
	   << "	--keysize       number of characters per key "
	   << "(default "<<key_size;
      cout << ")\n"
      	   << "	--valsize       number of characters per value "
      	   << "(default "<<value_size;
      cout << ")\n"
      	   << "	--inc           specify the increment to use in the displayed "
      	   << "histogram (default "<<increment;
      cout << ")\n"
      	   << "	--omaptype      specify how omaps should be generated - "
      	   << "rand for random sizes between\n"
      	   << "                        0 and max size, uniform for all sizes"
      	   << " to be specified size.\n"
      	   << "                        (default "<<value_size;
      cout <<"\n  --name          the rados id to use (default "<<rados_id;
      cout<<")\n";
      exit(1);
    }
  }
  int r = rados.init(rados_id.c_str());
  if (r < 0) {
    cout << "error during init" << std::endl;
    return r;
  }
  r = rados.conf_parse_argv(argc, argv);
  if (r < 0) {
    cout << "error during parsing args" << std::endl;
    return r;
  }
  r = rados.conf_parse_env(NULL);
  if (r < 0) {
    cout << "error during parsing env" << std::endl;
    return r;
  }
  r = rados.conf_read_file(NULL);
  if (r < 0) {
    cout << "error during read file" << std::endl;
    return r;
  }
  r = rados.connect();
  if (r < 0) {
    cout << "error during connect" << std::endl;
    return r;
  }
  r = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (r < 0) {
    cout << "error creating io ctx" << std::endl;
    rados.shutdown();
    return r;
  }
  return 0;
}

//Writer functions
Writer::Writer(OmapBench *omap_bench) : ob(omap_bench) {
  stringstream name;
  ob->data_lock.Lock();
  name << omap_bench->prefix << ++(ob->data.started_ops);
  ob->data_lock.Unlock();
  oid = name.str();
}
void Writer::start_time() {
  begin_time = ceph_clock_now(g_ceph_context);
}
void Writer::stop_time() {
  end_time = ceph_clock_now(g_ceph_context);
}
double Writer::get_time() {
  return (end_time - begin_time) * 1000;
}
string Writer::get_oid() {
  return oid;
}
std::map<std::string, bufferlist> & Writer::get_omap() {
  return omap;
}

//AioWriter functions
AioWriter::AioWriter(OmapBench *ob) : Writer(ob) {
  aioc = NULL;
}
AioWriter::~AioWriter() {
  if(aioc) aioc->release();
}
librados::AioCompletion * AioWriter::get_aioc() {
  return aioc;
}
void AioWriter::set_aioc(librados::callback_t complete,
    librados::callback_t safe) {
  aioc = ob->rados.aio_create_completion(this, complete, safe);
}


//Helper methods
void OmapBench::aio_is_safe(rados_completion_t c, void *arg) {
  AioWriter *aiow = reinterpret_cast<AioWriter *>(arg);
  aiow->stop_time();
  Mutex * data_lock = &aiow->ob->data_lock;
  Mutex * thread_is_free_lock = &aiow->ob->thread_is_free_lock;
  Cond * thread_is_free = &aiow->ob->thread_is_free;
  int &busythreads_count = aiow->ob->busythreads_count;
  o_bench_data &data = aiow->ob->data;
  int INCREMENT = aiow->ob->increment;
  int err = aiow->get_aioc()->get_return_value();
  if (err < 0) {
    cout << "error writing AioCompletion";
    return;
  }
  double time = aiow->get_time();
  delete aiow;
  data_lock->Lock();
  data.avg_latency = (data.avg_latency * data.completed_ops + time)
      / (data.completed_ops + 1);
  data.completed_ops++;
  if (time < data.min_latency) {
    data.min_latency = time;
  }
  if (time > data.max_latency) {
    data.max_latency = time;
  }
  data.total_latency += time;
  ++(data.freq_map[time / INCREMENT]);
  if(data.freq_map[time/INCREMENT] > data.mode.second) {
    data.mode.first = time/INCREMENT;
    data.mode.second = data.freq_map[time/INCREMENT];
  }
  data_lock->Unlock();

  thread_is_free_lock->Lock();
  busythreads_count--;
  thread_is_free->Signal();
  thread_is_free_lock->Unlock();
}

string OmapBench::random_string(int len) {
  string ret;
  string alphanum = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  for (int i = 0; i < len; ++i) {
    ret.push_back(alphanum[rand() % (alphanum.size() - 1)]);
  }

  return ret;
}

int OmapBench::run() {
  return (((OmapBench *)this)->*OmapBench::test)(omap_generator);
}

int OmapBench::print_written_omap() {
  for (int i = 1; i <= objects; i++) {
    int err = 0;
    librados::ObjectReadOperation key_read;
    set<string> out_keys;
    map<string, bufferlist> out_vals;
    std::stringstream objstrm;
    objstrm << prefix;
    objstrm << i;
    cout << "\nPrinting omap for "<<objstrm.str() << std::endl;
    key_read.omap_get_keys("", LONG_MAX, &out_keys, &err);
    io_ctx.operate(objstrm.str(), &key_read, NULL);
    if (err < 0) {
      cout << "error " << err;
      cout << " getting omap key set " << std::endl;
      return err;
    }

    librados::ObjectReadOperation val_read;
    val_read.omap_get_vals_by_keys(out_keys, &out_vals, &err);
    if (err < 0) {
      cout << "error " << err;
      cout << " getting omap value set " << std::endl;
      return err;
    }
    io_ctx.operate(objstrm.str(), &val_read, NULL);

    for (set<string>::iterator iter = out_keys.begin();
	iter != out_keys.end(); ++iter) {
      cout << *iter << "\t" << (out_vals)[*iter] << std::endl;
    }
  }
  return 0;
}

void OmapBench::print_results() {
  cout << "========================================================";
  cout << "\nNumber of kvmaps written:\t" << objects;
  cout << "\nNumber of ops at once:\t" << threads;
  cout << "\nEntries per kvmap:\t\t" << entries_per_omap;
  cout << "\nCharacters per key:\t" << key_size;
  cout << "\nCharacters per val:\t" << value_size;
  cout << std::endl;
  cout << std::endl;
  cout << "Average latency:\t" << data.avg_latency;
  cout << "ms\nMinimum latency:\t" << data.min_latency;
  cout << "ms\nMaximum latency:\t" << data.max_latency;
  cout << "ms\nMode latency:\t\t"<<"between "<<data.mode.first * increment;
  cout << " and " <<data.mode.first * increment + increment;
  cout << "ms\nTotal latency:\t\t" << data.total_latency;
  cout << "ms"<<std::endl;
  cout << std::endl;
  cout << "Histogram:" << std::endl;
  for(int i = floor(data.min_latency / increment); i <
      ceil(data.max_latency / increment); i++) {
    cout << ">= "<< i * increment;
    cout << "ms";
    int spaces;
    if (i == 0) spaces = 4;
    else spaces = 3 - floor(log10(i));
    for (int j = 0; j < spaces; j++) {
      cout << " ";
    }
    cout << "[";
    for(int j = 0; j < ((data.freq_map)[i])*45/(data.mode.second); j++) {
      cout << "*";
    }
    cout << std::endl;
  }
  cout << "\n========================================================"
       << std::endl;
}

int OmapBench::write_omap_asynchronously(AioWriter *aiow,
    const std::map<std::string,bufferlist> &omap) {
  librados::ObjectWriteOperation owo;
  owo.create(false);
  owo.omap_clear();
  owo.omap_set(omap);
  aiow->start_time();
  int err = io_ctx.aio_operate(aiow->get_oid(), aiow->get_aioc(), &owo);
  if (err < 0) {
    cout << "writing omap failed with code "<<err;
    cout << std::endl;
    return err;
  }
  return 0;
}

//Omap Generators
int OmapBench::generate_uniform_omap(const int omap_entries, const int key_size,
    const int value_size, std::map<std::string,bufferlist> * out_omap) {
  bufferlist bl;

  //setup omap
  for (int i = 0; i < omap_entries; i++) {
    bufferlist omap_val;
    omap_val.append(random_string(value_size));
    string key = random_string(key_size);
    (*out_omap)[key]= omap_val;
  }
  return 0;
}

int OmapBench::generate_non_uniform_omap(const int omap_entries,
    const int key_size, const int value_size,
    std::map<std::string,bufferlist> * out_omap) {
  bufferlist bl;

  int num_entries = rand() % omap_entries + 1;
  int key_len = rand() % key_size +1;
  int val_len = rand() % value_size +1;

  //setup omap
  for (int i = 0; i < num_entries; i++) {
    bufferlist omap_val;
    omap_val.append(random_string(val_len));
    string key = random_string(key_len);
    (*out_omap)[key] = omap_val;
  }
  return 0;
}

int OmapBench::generate_small_non_random_omap(const int omap_entries,
    const int key_size, const int value_size,
    std::map<std::string,bufferlist> * out_omap) {
  bufferlist bl;
  stringstream key;

  //setup omap
  for (int i = 0; i < omap_entries; i++) {
    bufferlist omap_val;
    omap_val.append("Value ");
    omap_val.append(i);
    key << "Key " << i;
    (*out_omap)[key.str()]= omap_val;
  }
  return 0;
}

//tests
int OmapBench::test_write_objects_in_parallel(omap_generator_t omap_gen) {
  comp = NULL;
  AioWriter *this_aio_writer;

  Mutex::Locker l(thread_is_free_lock);
  for (int i = 0; i < objects; i++) {
    assert(busythreads_count <= threads);
    //wait for a writer to be free
    if (busythreads_count == threads) {
      int err = thread_is_free.Wait(thread_is_free_lock);
      assert(busythreads_count < threads);
      if (err < 0) {
	return err;
      }
    }

    //set up the write
    this_aio_writer = new AioWriter(this);
    this_aio_writer->set_aioc(NULL,safe);

    //perform the write
    busythreads_count++;
    int err = omap_gen(entries_per_omap, key_size, value_size,
	& this_aio_writer->get_omap());
    if (err < 0) {
      return err;
    }
    err = OmapBench::write_omap_asynchronously(this_aio_writer,
	(this_aio_writer->get_omap()));


    if (err < 0) {
      return err;
    }
  }
  while(busythreads_count > 0) {
    thread_is_free.Wait(thread_is_free_lock);
  }

  return 0;
}

/**
 * runs the specified test with the specified parameters and generates
 * a histogram of latencies
 */
int main(int argc, const char** argv) {
  OmapBench ob;
  int err = ob.setup(argc, argv);
  if (err<0) {
    cout << "error during setup: "<<err;
    cout << std::endl;
    exit(1);
  }
  err = ob.run();
  if (err < 0) {
    cout << "writing objects failed with code " << err;
    cout << std::endl;
    return err;
  }

  ob.print_results();

  //uncomment to show omaps
  /*err = ob.return print_written_omap();
  if (err < 0) {
    cout << "printing omaps failed with code " << err;
    cout << std::endl;
    return err;
  }
  */
  return 0;

}
