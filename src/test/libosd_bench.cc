// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <thread>

#include "libcephd/ceph_osd.h"
#include "common/ceph_argparse.h"
#include "common/ceph_time.h"

#define dout_subsys ceph_subsys_osd

static void usage()
{
  std::cerr << "usage: libosd_bench [flags]\n"
      "	 --volume\n"
      "	       name of the volume\n"
      "	 --threads\n"
      "	       number of threads to carry out this workload\n" << std::endl;
  generic_server_usage();
}

void benchmark_thread(struct libosd *osd, uint8_t *volume, int count)
{
  // do 'count' synchronous reads on a nonexistent object
  char buffer[16];
  for (int i = 0; i < count; i++) {
    int r = osd->read("foo", volume, 0, sizeof(buffer), buffer,
                      LIBOSD_READ_FLAGS_NONE, nullptr, nullptr);
    if (r != 0)
      std::cerr << "libosd_read() failed with " << r << std::endl;
  }
}

void benchmark(struct libosd *osd, uint8_t *uuid, int nthreads, int count)
{
  auto fn = [osd, uuid, count]() {
    benchmark_thread(osd, uuid, count);
  };

  auto t1 = ceph::mono_clock::now();
  std::cout << "time started " << t1 << std::endl;

  // start threads
  std::vector<std::thread> threads;
  for (int i = 0; i < nthreads; i++)
    threads.emplace_back(fn);

  // join threads
  for (auto &t : threads)
    t.join();

  auto t2 = ceph::mono_clock::now();
  std::cout << "time finished " << t2 << std::endl;

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  std::cout << "count " << count << std::endl;
  std::cout << "duration " << duration.count() << "ms" << std::endl;
  std::cout << "iops " << 1000000LL * count / duration.count() << std::endl;
}

int main(int argc, const char *argv[])
{
  const struct libosd_init_args init_args {
    .id = 0,
    .config = nullptr,
    .cluster = nullptr,
    .callbacks = nullptr,
    .argv = argv,
    .argc = argc,
    .user = nullptr,
  };

  // default values
  int threads = 1;
  int count = 1;
  std::string volume;

  // command-line arguments
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  string val;
  for (auto i = args.begin(); i != args.end();) {
    if (ceph_argparse_double_dash(args, i))
      break;

    if (ceph_argparse_witharg(args, i, &val, "--threads", (char*)NULL)) {
      threads = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--count", (char*)NULL)) {
      count = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--volume", (char*)NULL)) {
      volume = val;
    } else
      ++i;
  }

  if (threads < 1 || threads > 256) {
    std::cerr << "Invalid value for threads " << threads << std::endl;
    usage();
    return EXIT_FAILURE;
  }
  if (count < 1) {
    std::cerr << "Invalid value for count " << count << std::endl;
    usage();
    return EXIT_FAILURE;
  }
  if (volume.empty()) {
    std::cerr << "Missing argument --volume" << std::endl;
    usage();
    return EXIT_FAILURE;
  }

  std::cout << "threads " << threads << std::endl;
  std::cout << "count " << count << std::endl;
  std::cout << "volume " << volume << std::endl;

  // start osd
  struct libosd* osd = libosd_init(&init_args);
  if (osd == nullptr) {
    std::cerr << "libosd_init() failed" << std::endl;
    return EXIT_FAILURE;
  }

  uint8_t uuid[16];
  int r = osd->get_volume(volume.c_str(), uuid);
  if (r != 0) {
    std::cerr << "libosd_get_volume() failed with " << r << std::endl;
    return EXIT_FAILURE;
  }

  benchmark(osd, uuid, threads, count);

  // shutdown and cleanup
  osd->shutdown();
  osd->join();
  libosd_cleanup(osd);
  std::cout << "libosd_cleanup() finished" << std::endl;
  return EXIT_SUCCESS;
}
