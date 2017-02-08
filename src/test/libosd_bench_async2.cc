// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>

#include "libcephd/ceph_osd.h"
#include "common/ceph_argparse.h"
#include "common/likely.h"
#include "common/ceph_time.h"

#define dout_subsys ceph_subsys_osd

static void usage()
{
  std::cerr << "usage: libosd_bench_async2 [flags]\n"
      "	 --volume\n"
      "	       name of the volume\n"
      "	 --threads\n"
      "	       number of threads to carry out this workload\n"
      "	 --count\n"
      "	       per-thread request total (done when reached)\n"
      "        count MUST be a multiple of BATCH!\n"
      "	 --depth\n"
      "	       per-thread submit depth\n"
	    << std::endl;
  generic_server_usage();
}

struct ReadR
{
  std::mutex* mtx;
  std::condition_variable* cv;
  struct libosd* osd;
  uint8_t* volume;
  char* obj;
  std::atomic<int>* count;
  std::atomic<int>* batch;
  int *next;
  int depth;
  int lowat;
  int inst;
  int times_queued;
  char buffer[16];

  ReadR() {}
};

extern "C" {
  static void io_completion(int result, uint64_t length, int flags,
			    void *user_data)
{
  ReadR* rio = static_cast<struct ReadR*>(user_data);

#ifdef VERBOSE_PRINTS
  std::cout << "io_completion " << rio
	    << " times_queued " << rio->times_queued
	    << " count " << (*(rio->count)).load()
	    << std::endl;
#endif

  /* done? */
  int count = --(*(rio->count));
  if (count == 0) {
    std::unique_lock<std::mutex> lk(*(rio->mtx));
    *(rio->next) = -1;
    rio->cv->notify_one();
    return;
  }

  if (count < rio->depth)
    return;

  int batch = ++(*(rio->batch));
  int next = batch % rio->depth;
  if (unlikely(next == 0)) {
    std::unique_lock<std::mutex> lk(*(rio->mtx));
    rio->cv->notify_one();
  }
} /* io_completion */

} /* extern "C" */

void benchmark_thread(struct libosd *osd, uint8_t *volume, int depth,
		      int count, int ix)
{
  int r = 0;

  vector<ReadR> rios;
  rios.reserve(depth);

  std::mutex mtx;
  std::condition_variable cv;
  std::unique_lock<std::mutex> lk(mtx);
  std::atomic<int> cnt{count};
  std::atomic<int> batch_cnt{0};
  int next{0};

  std::string obj = "foo_" + std::to_string(ix);

  std::cout << "obj is: " << obj << std::endl;

  /* queue reqs up to depth */
  for (int i = 0; i < depth; ++i) {
    ReadR rio;
    rio.mtx = &mtx;
    rio.cv = &cv;
    rio.osd = osd;
    rio.volume = volume;
    rio.obj = const_cast<char*>(obj.c_str());
    rio.count = &cnt;
    rio.batch = &batch_cnt;
    rio.next = &next;
    rio.depth = depth;
    rio.inst = i;
    rio.times_queued = 1; /* below */
    rios.emplace_back(rio);

    ReadR* riop = &(rios[i]);
    r = osd->read(riop->obj, riop->volume, 0, 
		  sizeof(riop->buffer), riop->buffer,
		  LIBOSD_READ_FLAGS_NONE,
		  io_completion, riop);
    if (r != 0)
      std::cerr << "benchmark_thread libosd_read() failed with "
		<< r << std::endl;
  }

  /* wait till done */
  do {
    cv.wait(lk);

    if (next == -1)
      break;

    /* queue another batch */
    for (int i = 0; i < depth; ++i) {
      ReadR* rio = &(rios[i]);
      ++rio->times_queued;
      int r = rio->osd->read(rio->obj, rio->volume, 0, 
			     sizeof(rio->buffer), rio->buffer,
			     LIBOSD_READ_FLAGS_NONE,
			     io_completion, rio);
      if (r != 0)
	std::cerr << "io_completion libosd_read() failed with "
		  << r << std::endl;
    }
  } while (1);
}

void benchmark(struct libosd *osd, uint8_t *uuid, int nthreads,
	       int depth, int count)
{
  auto t1 = ceph::mono_clock::now();
  std::cout << "time started " << t1 << std::endl;

  // start threads
  std::vector<std::thread> threads;
  for (int i = 0; i < nthreads; i++) {
    auto fn = [osd, uuid, depth, count, i]() {
      benchmark_thread(osd, uuid, depth, count, i);
    };
    threads.emplace_back(fn);
  }

  // join threads
  for (auto &t : threads)
    t.join();

  auto t2 = ceph::mono_clock::now();
  std::cout << "time finished " << t2 << std::endl;

  auto duration =
    std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

  std::cout << "count " << count << std::endl;
  std::cout << "duration " << duration.count() << "ms" << std::endl;
  std::cout << "iops " << 1000000LL * count / duration.count()
	    << std::endl;
}

int main(int argc, const char *argv[])
{
  struct libosd_init_args init_args {
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
  int depth = 1000;
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

    if (ceph_argparse_witharg(args, i, &val,
			      "--threads", (char*)NULL)) {
      threads = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val,
			      "--id", (char*)NULL)) {
      init_args.id = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--depth",
				     (char*)NULL)) {
      depth = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--count",
				     (char*)NULL)) {
      count = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--volume",
				     (char*)NULL)) {
      volume = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--conf",
				     (char*)NULL)) {
      init_args.config = strdup(val.c_str());
    } else
      ++i;
  }

  if (threads < 1 || threads > 256) {
    std::cerr << "Invalid value for threads " << threads << std::endl;
    usage();
    return EXIT_FAILURE;
  }
  if (depth < 1) {
    std::cerr << "Invalid value for depth " << depth << std::endl;
    usage();
    return EXIT_FAILURE;
  }
  if (count < 1) {
    std::cerr << "Invalid value for count " << count << std::endl;
    usage();
    return EXIT_FAILURE;
  }
  if (count < depth) {
    std::cerr << "Must have count > depth" << std::endl;
    usage();
    return EXIT_FAILURE;
  }
  if ((count % depth) != 0) {
    std::cerr << "Must have count % depth == 0!" << std::endl;
    usage();
    return EXIT_FAILURE;
  }

  if (volume.empty()) {
    std::cerr << "Missing argument --volume" << std::endl;
    usage();
    return EXIT_FAILURE;
  }

  std::cout << "threads " << threads << std::endl;
  std::cout << "depth " << depth << std::endl;
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

  benchmark(osd, uuid, threads, depth, count);

  // shutdown and cleanup
  osd->shutdown();
  osd->join();

  std::this_thread::sleep_for(std::chrono::seconds(10));

  libosd_cleanup(osd);
  std::cout << "libosd_cleanup() finished" << std::endl;
  return EXIT_SUCCESS;
}
