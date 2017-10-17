// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <iostream>
#include <vector>
#include <map>
#include <ctime>
#include <chrono>

#include <cfloat>

#include <getopt.h>

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"

using namespace std;

void usage (char *bin) {
  cout << "usage: " << bin
       << " [-p pool_name] [-n object_count] [-s omap_size] [-t time_limit]"
       << endl
       << "where: " << endl
       << "pool_name = RADOS pool to use (must already exist)" << endl
       << "object_count = number of objects to write before terminating" << endl
       << "omap_size = number of omap key/value pairs per object" << endl
       << "time_limit = time limit in seconds before exiting" << endl;
}

int main (int argc, char **argv)
{
  int c = 0;
  static struct option long_options[] = {
    {"help", no_argument, 0, 'h' }
  };

  string pool_name = "testpool";
  int omap_size = 100000;
  int objects = 1000000;
  time_t time_limit = 0;

  while (c != -1) {
    c = getopt_long(argc, argv, "hn:p:s:t:", long_options, &optind);
    switch(c) {
    case 'h':
      usage(argv[0]);
      exit(0);
      break;

    case 'n':
      objects = atoi(optarg);
      break;

    case 'p':
      pool_name = optarg;
      break;

    case 's':
      omap_size = atoi(optarg);
      break;

    case 't':
      time_limit = atoi(optarg);

    case '?':
    case -1:
      break;

    default:
      usage(argv[0]);
      exit(1);
    }
  }

  librados::Rados rados;
  librados::IoCtx ioctx;

  vector<const char *> args;
  int r;

  r = rados.init("admin");
  if (r < 0) {
    cerr << "error " << r << " in init." << endl;
    return -r;
  }
 
  r = rados.conf_parse_argv(argc, (const char **)argv);
  if (r < 0) {
    cerr << "couldn't parse command line arguments: " << r << endl;
    return -r;
  }

  r = rados.conf_read_file(NULL);
  if (r < 0) {
    cerr << "error reading configuration file: " << r << endl;
    return -r;
  }

  rados.conf_parse_env(NULL);
  if (r < 0) {
    cerr << "error parsing config environment variables: " << r << endl;
    return -r;
  }

  rados.connect();
  if (r < 0) {
    cerr << "error " << r << " while connecting." << endl;
    return -r;
  }

  rados.ioctx_create(pool_name.c_str(), ioctx);
  if (r < 0) {
    cerr << "error " << r << " creating ioctx, check pool " << pool_name << endl;
    return -r;
  }

  map<string, bufferlist> omap;
  vector<map<string, bufferlist> > omap_vec;
  librados::bufferlist bl;

  for (int i=0; i<omap_size; i++) {
    omap.insert(make_pair(to_string(i), bl));
    if (i > 0 && !(i % omap_size)) {
      omap_vec.push_back(omap);
      omap.clear();
    }
  }

  if (!omap.empty()) {
    omap_vec.push_back(omap);
  }  

  time_t begin = time(NULL);
  time_t iter = begin;
  time_t cur;

  cout << "prefix = " << to_string(begin) << endl;

  using sample = pair<chrono::high_resolution_clock::time_point, double>;
  vector<sample> samples, latest;
  auto by_time = [](const sample& a, const sample &b) {
    return a.first < b.first;
  };
  auto by_latency = [](const sample& a, const sample &b) {
    return a.second < b.second;
  };

  for (int i=0, last=0; i<objects; i++) {
    string object_name = to_string(begin) + "." + to_string(i);
    for (auto m : omap_vec) {
      chrono::high_resolution_clock::time_point pre =
	chrono::high_resolution_clock::now();
      r = ioctx.omap_set(object_name, m);
      chrono::high_resolution_clock::time_point post =
	chrono::high_resolution_clock::now();

      if (r) cerr << "omap_set returned " << r << endl;

      chrono::duration<double, std::milli> elapsed = post - pre;
      double latency = elapsed.count();
      latest.push_back(make_pair(post, latency));
    }
    cur = time(NULL);
    if (cur - iter >= 10) {
      sort(latest.begin(), latest.end(), by_latency);
      cout << "avg: " << i << " obj in " << cur - begin << " sec ("
	   << (float)i/(cur-begin) << " obj, "
	   << (float)(i*omap_size)/(cur-begin) << " omap entries/sec)"
	   << endl
	   << "cur: " << i-last << " obj in " << cur - iter << " sec ("
	   << (float)(i-last)/(cur-iter) << " obj, "
	   << (float)((i-last)*omap_size)/(cur-iter) << " omap entries/sec)"
	   << endl
	   << "lat: min = " << latest[0].second
	   << " max = " << latest[latest.size() - 1].second
	   << " avg = " << accumulate(latest.begin(),
				      latest.end(), 0.0,
				      [](double a, sample b) {
					return a + b.second;
				      }) / latest.size()
	   << endl;

      samples.insert(samples.end(), latest.begin(), latest.end());
      latest.clear();
      sort(samples.begin(), samples.end(), by_latency);

      int n = samples.size();

      if (n >= 100) {
	cout << "n: " << n
	     << " 80% = " << samples[n * .8].second
	     << " 90% = " << samples[n * .9].second
	     << " 99% = " << samples[n * .99].second
	     << " max = " << samples[samples.size() - 1].second
	     << endl;
      } else {
	cout << "waiting for " << 100 - n << " more samples" << endl;
      }

      if (n > 1024) {
	sort(samples.begin(), samples.end(), by_time);
	samples.erase(samples.begin(), samples.begin() + (n - 1024));
      }
      iter = cur;
      last = i;
    }
    if (time_limit && (cur - begin) > time_limit) {
      break;
    }
  }

  rados.shutdown();
  return 0;
}
