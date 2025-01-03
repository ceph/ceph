// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream> // for std::cout

#include "include/types.h"
#include "common/Thread.h"
#include "common/debug.h"
#include "common/Clock.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "log/Log.h"

#define dout_context g_ceph_context

using namespace std;

struct T : public Thread {
  int num;
  set<int> myset;
  map<int,string> mymap;
  explicit T(int n) : num(n) {
    myset.insert(123);
    myset.insert(456);
    mymap[1] = "foo";
    mymap[10] = "bar";
  }

  void *entry() override {
    while (num-- > 0)
      generic_dout(0) << "this is a typical log line.  set "
		      << myset << " and map " << mymap << dendl;
    return 0;
  }
};

void usage(const char *name) {
  cout << name << " <threads> <lines>\n"
       << "\t threads: the number of threads for this test.\n"
       << "\t lines: the number of log entries per thread.\n";
}

int main(int argc, const char **argv)
{
  if (argc < 3) {
    usage(argv[0]);
    return EXIT_FAILURE;
  }

  int threads = atoi(argv[1]);
  int num = atoi(argv[2]);

  cout << threads << " threads, " << num << " lines per thread" << std::endl;

  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  utime_t start = ceph_clock_now();

  list<T*> ls;
  for (int i=0; i<threads; i++) {
    T *t = new T(num);
    t->create("t");
    ls.push_back(t);
  }

  for (int i=0; i<threads; i++) {
    T *t = ls.front();
    ls.pop_front();
    t->join();
    delete t;
  }

  utime_t t = ceph_clock_now();
  t -= start;
  cout << " flushing.. " << t << " so far ..." << std::endl;

  g_ceph_context->_log->flush();

  utime_t end = ceph_clock_now();
  utime_t dur = end - start;

  cout << dur << std::endl;
  return 0;
}
