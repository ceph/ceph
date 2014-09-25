/*
 * KvStoreBench.cc
 *
 *  Created on: Aug 23, 2012
 *      Author: eleanor
 */

#include "test/kv_store_bench.h"
#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree_async.h"
#include "include/rados/librados.hpp"
#include "test/omap_bench.h"
#include "common/ceph_argparse.h"


#include <string>
#include <climits>
#include <iostream>
#include <sstream>
#include <cmath>

KvStoreBench::KvStoreBench()
: entries(30),
  ops(100),
  clients(5),
  key_size(5),
  val_size(7),
  max_ops_in_flight(8),
  clear_first(false),
  k(2),
  cache_size(10),
  cache_refresh(1),
  client_name("admin"),
  verbose(false),
  kvs(NULL),
  data_lock("data lock"),
  ops_in_flight(0),
  ops_in_flight_lock("KvStoreBench::ops_in_flight_lock"),
  rados_id("admin"),
  pool_name("rbd"),
  io_ctx_ready(false)
{
  probs[25] = 'i';
  probs[50] = 'u';
  probs[75] = 'd';
  probs[100] = 'r';
}

KvStoreBench::~KvStoreBench()
{
  if (io_ctx_ready) {
    librados::ObjectWriteOperation owo;
    owo.remove();
    io_ctx.operate(client_name + ".done-setting", &owo);
  }
  delete kvs;
}

int KvStoreBench::setup(int argc, const char** argv) {
  vector<const char*> args;
  argv_to_vec(argc,argv,args);
  srand(time(NULL));

  stringstream help;
  help
      << "Usage: KvStoreBench [options]\n"
      << "Generate latency and throughput statistics for the key value store\n"
      << "\n"
      << "There are two sets of options - workload options affect the kind of\n"
      << "test to run, while algorithm options affect how the key value\n"
      << "store handles the workload.\n"
      << "\n"
      << "There are about entries / k objects in the store to begin with.\n"
      << "Higher k values reduce the likelihood of splits and the likelihood\n"
      << "multiple writers simultaneously faling to write because an object \n"
      << "is full, but having a high k also means there will be more object\n"
      << "contention.\n"
      << "\n"
      << "WORKLOAD OPTIONS\n"
      << "   --name <client name>                          client name (default admin)\n"
      << "   --entries <number>                            number of key/value pairs to store initially\n"
      << "                                                 (default " << entries << ")\n"
      << "   --ops <number>                                number of operations to run\n"
      << "   --keysize <number>                            number of characters per key (default " << key_size << ")\n"
      << "   --valsize <number>                            number of characters per value (default " << val_size << ")\n"
      << "   -t <number>                                   number of operations in flight concurrently\n"
      << "                                                 (default " << max_ops_in_flight << ")\n"
      << "   --clients <number>                            tells this instance how many total clients are. Note that\n"
      << "                                                 changing this does not change the number of clients."
      << "   -d <insert> <update> <delete> <read>          percent (1-100) of operations that should be of each type\n"
      << "                                                 (default 25 25 25 25)\n"
      << "   -r <number>                                   random seed to use (default time(0))\n"
      << "ALGORITHM OPTIONS\n"
      << "   --kval                                        k, where each object has a number of entries\n"
      << "                                                 >= k and <= 2k.\n"
      << "   --cache-size                                  number of index entries to keep in cache\n"
      << "                                                 (default " << cache_size << ")\n"
      << "   --cache-refresh                               percent (1-100) of cache-size to read each \n"
      << "                                                 time the index is read\n"
      << "OTHER OPTIONS\n"
      << "   --verbosity-on                                display debug output\n"
      << "   --clear-first                                 delete all existing objects in the pool before running tests\n";
  for (unsigned i = 0; i < args.size(); i++) {
    if(i < args.size() - 1) {
      if (strcmp(args[i], "--ops") == 0) {
	ops = atoi(args[i+1]);
      } else if (strcmp(args[i], "--entries") == 0) {
	entries = atoi(args[i+1]);
      } else if (strcmp(args[i], "--kval") == 0) {
	k = atoi(args[i+1]);
      } else if (strcmp(args[i], "--keysize") == 0) {
	key_size = atoi(args[i+1]);
      } else if (strcmp(args[i], "--valsize") == 0) {
	val_size = atoi(args[i+1]);
      } else if (strcmp(args[i], "--cache-size") == 0) {
	cache_size = atoi(args[i+1]);
      } else if (strcmp(args[i], "--cache-refresh") == 0) {
	cache_refresh = 100 / atoi(args[i+1]);
      } else if (strcmp(args[i], "-t") == 0) {
	max_ops_in_flight = atoi(args[i+1]);
      } else if (strcmp(args[i], "--clients") == 0) {
	clients = atoi(args[i+1]);
      } else if (strcmp(args[i], "-d") == 0) {
	if (i + 4 >= args.size()) {
	  cout << "Invalid arguments after -d: there must be 4 of them."
	      << std::endl;
	  continue;
	} else {
	  probs.clear();
	  int sum = atoi(args[i + 1]);
	  probs[sum] = 'i';
	  sum += atoi(args[i + 2]);
	  probs[sum] = 'u';
	  sum += atoi(args[i + 3]);
	  probs[sum] = 'd';
	  sum += atoi(args[i + 4]);
	  probs[sum] = 'r';
	  if (sum != 100) {
	    cout << "Invalid arguments after -d: they must add to 100."
		<< std::endl;
	  }
	}
      } else if (strcmp(args[i], "--name") == 0) {
	client_name = args[i+1];
      } else if (strcmp(args[i], "-r") == 0) {
	srand(atoi(args[i+1]));
      }
    } else if (strcmp(args[i], "--verbosity-on") == 0) {
      verbose = true;
    } else if (strcmp(args[i], "--clear-first") == 0) {
      clear_first = true;
    } else if (strcmp(args[i], "--help") == 0) {
      cout << help.str() << std::endl;
      exit(1);
    }
  }

  KvFlatBtreeAsync * kvba = new KvFlatBtreeAsync(k, client_name, cache_size,
      cache_refresh, verbose);
  kvs = kvba;

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
    cout << "error during connect: " << r << std::endl;
    return r;
  }
  r = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (r < 0) {
    cout << "error creating io ctx" << std::endl;
    rados.shutdown();
    return r;
  }
  io_ctx_ready = true;

  if (clear_first) {
    librados::NObjectIterator it;
    for (it = io_ctx.nobjects_begin(); it != io_ctx.nobjects_end(); ++it) {
      librados::ObjectWriteOperation rm;
      rm.remove();
      io_ctx.operate(it->get_oid(), &rm);
    }
  }

  int err = kvs->setup(argc, argv);
  if (err < 0 && err != -17) {
    cout << "error during setup of kvs: " << err << std::endl;
    return err;
  }

  return 0;
}

string KvStoreBench::random_string(int len) {
  string ret;
  string alphanum = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  for (int i = 0; i < len; ++i) {
    ret.push_back(alphanum[rand() % (alphanum.size() - 1)]);
  }

  return ret;
}

pair<string, bufferlist> KvStoreBench::rand_distr(bool new_elem) {
  pair<string, bufferlist> ret;
  if (new_elem) {
    ret = make_pair(random_string(key_size),
	KvFlatBtreeAsync::to_bl(random_string(val_size)));
    key_set.insert(ret.first);
  } else {
    if (key_set.size() == 0) {
      return make_pair("",KvFlatBtreeAsync::to_bl(""));
    }
    string get_string = random_string(key_size);
    std::set<string>::iterator it = key_set.lower_bound(get_string);
    if (it == key_set.end()) {
      ret.first = *(key_set.rbegin());
    } else {
      ret.first = *it;
    }
    ret.second = KvFlatBtreeAsync::to_bl(random_string(val_size));
  }
  return ret;
}

int KvStoreBench::test_random_insertions() {
  int err;
  if (entries == 0) {
    return 0;
  }
  stringstream prev_ss;
  prev_ss << (atoi(client_name.c_str()) - 1);
  string prev_rid = prev_ss.str();
  stringstream last_ss;
  if (client_name.size() > 1) {
    last_ss << client_name.substr(0,client_name.size() - 2);
  }
  last_ss << clients - 1;
  string last_rid = client_name == "admin" ? "admin" : last_ss.str();

  map<string, bufferlist> big_map;
  for (int i = 0; i < entries; i++) {
    bufferlist bfr;
    bfr.append(random_string(7));
    big_map[random_string(5)] = bfr;
  }

  uint64_t uint;
  time_t t;
  if (client_name[client_name.size() - 1] != '0' && client_name != "admin") {
    do {
      librados::ObjectReadOperation oro;
      oro.stat(&uint, &t, &err);
      err = io_ctx.operate(prev_rid + ".done-setting", &oro, NULL);
      if (verbose) cout << "reading " << prev_rid << ": err = " << err
	  << std::endl;
    } while (err != 0);
    cout << "detected " << prev_rid << ".done-setting" << std::endl;
  }

  cout << "testing random insertions";
  err = kvs->set_many(big_map);
  if (err < 0) {
    cout << "error setting things" << std::endl;
    return err;
  }

  librados::ObjectWriteOperation owo;
  owo.create(true);
  io_ctx.operate(client_name + ".done-setting", &owo);
  cout << "created " << client_name + ".done-setting. waiting for "
      << last_rid << ".done-setting" << std::endl;

  do {
    librados::ObjectReadOperation oro;
    oro.stat(&uint, &t, &err);
    err = io_ctx.operate(last_rid + ".done-setting", &oro, NULL);
  } while (err != 0);
  cout << "detected " << last_rid << ".done-setting" << std::endl;

  return err;
}

void KvStoreBench::aio_callback_timed(int * err, void *arg) {
  timed_args *args = reinterpret_cast<timed_args *>(arg);
  Mutex * ops_in_flight_lock = &args->kvsb->ops_in_flight_lock;
  Mutex * data_lock = &args->kvsb->data_lock;
  Cond * op_avail = &args->kvsb->op_avail;
  int *ops_in_flight = &args->kvsb->ops_in_flight;
  if (*err < 0 && *err != -61) {
    cerr << "Error during " << args->op << " operation: " << *err << std::endl;
  }

  args->sw.stop_time();
  double time = args->sw.get_time();
  args->sw.clear();

  data_lock->Lock();
  //latency
  args->kvsb->data.latency_jf.open_object_section("latency");
  args->kvsb->data.latency_jf.dump_float(string(1, args->op).c_str(),
      time);
  args->kvsb->data.latency_jf.close_section();

  //throughput
  args->kvsb->data.throughput_jf.open_object_section("throughput");
  args->kvsb->data.throughput_jf.dump_unsigned(string(1, args->op).c_str(),
      ceph_clock_now(g_ceph_context));
  args->kvsb->data.throughput_jf.close_section();

  data_lock->Unlock();

  ops_in_flight_lock->Lock();
  (*ops_in_flight)--;
  op_avail->Signal();
  ops_in_flight_lock->Unlock();

  delete args;
}

int KvStoreBench::test_teuthology_aio(next_gen_t distr,
    const map<int, char> &probs)
{
  int err = 0;
  cout << "inserting initial entries..." << std::endl;
  err = test_random_insertions();
  if (err < 0) {
    return err;
  }
  cout << "finished inserting initial entries. Waiting 10 seconds for everyone"
      << " to catch up..." << std::endl;

  sleep(10);

  cout << "done waiting. Starting random operations..." << std::endl;

  Mutex::Locker l(ops_in_flight_lock);
  for (int i = 0; i < ops; i++) {
    assert(ops_in_flight <= max_ops_in_flight);
    if (ops_in_flight == max_ops_in_flight) {
      int err = op_avail.Wait(ops_in_flight_lock);
      if (err < 0) {
	assert(false);
	return err;
      }
      assert(ops_in_flight < max_ops_in_flight);
    }
    cout << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t" << i + 1 << " / "
	<< ops << std::endl;
    timed_args * cb_args = new timed_args(this);
    pair<string, bufferlist> kv;
    int random = (rand() % 100);
    cb_args->op = probs.lower_bound(random)->second;
    switch (cb_args->op) {
    case 'i':
      kv = (((KvStoreBench *)this)->*distr)(true);
      if (kv.first == "") {
	i--;
	delete cb_args;
	continue;
      }
      ops_in_flight++;
      cb_args->sw.start_time();
      kvs->aio_set(kv.first, kv.second, false, aio_callback_timed,
	  cb_args, &cb_args->err);
      break;
    case 'u':
      kv = (((KvStoreBench *)this)->*distr)(false);
      if (kv.first == "") {
	i--;
	delete cb_args;
	continue;
      }
      ops_in_flight++;
      cb_args->sw.start_time();
      kvs->aio_set(kv.first, kv.second, true, aio_callback_timed,
	  cb_args, &cb_args->err);
      break;
    case 'd':
      kv = (((KvStoreBench *)this)->*distr)(false);
      if (kv.first == "") {
	i--;
	delete cb_args;
	continue;
      }
      key_set.erase(kv.first);
      ops_in_flight++;
      cb_args->sw.start_time();
      kvs->aio_remove(kv.first, aio_callback_timed, cb_args, &cb_args->err);
      break;
    case 'r':
      kv = (((KvStoreBench *)this)->*distr)(false);
      if (kv.first == "") {
	i--;
	delete cb_args;
	continue;
      }
      bufferlist val;
      ops_in_flight++;
      cb_args->sw.start_time();
      kvs->aio_get(kv.first, &cb_args->val, aio_callback_timed,
	  cb_args, &cb_args->err);
      break;
    }

    delete cb_args;
  }

  while(ops_in_flight > 0) {
    op_avail.Wait(ops_in_flight_lock);
  }

  print_time_data();
  return err;
}

int KvStoreBench::test_teuthology_sync(next_gen_t distr,
    const map<int, char> &probs)
{
  int err = 0;
  err = test_random_insertions();
  if (err < 0) {
    return err;
  }
  sleep(10);
  for (int i = 0; i < ops; i++) {
    StopWatch sw;
    pair<char, double> d;
    cout << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t" << i + 1 << " / "
	<< ops << std::endl;
    pair<string, bufferlist> kv;
    int random = (rand() % 100);
    d.first = probs.lower_bound(random)->second;
    switch (d.first) {
    case 'i':
      kv = (((KvStoreBench *)this)->*distr)(true);
      if (kv.first == "") {
	i--;
	continue;
      }
      sw.start_time();
      err = kvs->set(kv.first, kv.second, true);
      sw.stop_time();
      if (err < 0) {
	cout << "Error setting " << kv << ": " << err << std::endl;
	return err;
      }
      break;
    case 'u':
      kv = (((KvStoreBench *)this)->*distr)(false);
      if (kv.first == "") {
	i--;
	continue;
      }
      sw.start_time();
      err = kvs->set(kv.first, kv.second, true);
      sw.stop_time();
      if (err < 0 && err != -61) {
	cout << "Error updating " << kv << ": " << err << std::endl;
	return err;
      }
      break;
    case 'd':
      kv = (((KvStoreBench *)this)->*distr)(false);
      if (kv.first == "") {
	i--;
	continue;
      }
      key_set.erase(kv.first);
      sw.start_time();
      err = kvs->remove(kv.first);
      sw.stop_time();
      if (err < 0 && err != -61) {
	cout << "Error removing " << kv << ": " << err << std::endl;
	return err;
      }
      break;
    case 'r':
      kv = (((KvStoreBench *)this)->*distr)(false);
      if (kv.first == "") {
	i--;
	continue;
      }
      bufferlist val;
      sw.start_time();
      err = kvs->get(kv.first, &kv.second);
      sw.stop_time();
      if (err < 0 && err != -61) {
	cout << "Error getting " << kv << ": " << err << std::endl;
	return err;
      }
      break;
    }

    double time = sw.get_time();
    d.second = time;
    sw.clear();
    //latency
    data.latency_jf.open_object_section("latency");
    data.latency_jf.dump_float(string(1, d.first).c_str(),
        time);
    data.latency_jf.close_section();
  }

  print_time_data();
  return err;
}

void KvStoreBench::print_time_data() {
  cout << "========================================================\n";
  cout << "latency:" << std::endl;
  data.latency_jf.flush(cout);
  cout << std::endl;
  cout << "throughput:" << std::endl;
  data.throughput_jf.flush(cout);
  cout << "\n========================================================"
       << std::endl;
}

int KvStoreBench::teuthology_tests() {
  int err = 0;
  if (max_ops_in_flight > 1) {
    test_teuthology_aio(&KvStoreBench::rand_distr, probs);
  } else {
    err = test_teuthology_sync(&KvStoreBench::rand_distr, probs);
  }
  return err;
}

int main(int argc, const char** argv) {
  KvStoreBench kvsb;
  int err = kvsb.setup(argc, argv);
  if (err == 0) cout << "setup successful" << std::endl;
  else{
    cout << "error " << err << std::endl;
    return err;
  }
  err = kvsb.teuthology_tests();
  if (err < 0) return err;
  return 0;
};
