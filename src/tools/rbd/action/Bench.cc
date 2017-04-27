// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include <iostream>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_sum.hpp>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace bench {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

enum io_type_t {
  IO_TYPE_READ = 0,
  IO_TYPE_WRITE,

  IO_TYPE_NUM,
};

struct IOType {};
struct Size {};
struct IOPattern {};

void validate(boost::any& v, const std::vector<std::string>& values,
              Size *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  std::string parse_error;
  uint64_t size = strict_sistrtoll(s.c_str(), &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(size);
}

void validate(boost::any& v, const std::vector<std::string>& values,
              IOPattern *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  if (s == "rand") {
    v = boost::any(true);
  } else if (s == "seq") {
    v = boost::any(false);
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
}

io_type_t get_io_type(string io_type_string) {
  if (io_type_string == "read")
    return IO_TYPE_READ;
  else if (io_type_string == "write")
    return IO_TYPE_WRITE;
  else
    return IO_TYPE_NUM;
}

void validate(boost::any& v, const std::vector<std::string>& values,
              IOType *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  io_type_t io_type = get_io_type(s);
  if (io_type >= IO_TYPE_NUM)
    throw po::validation_error(po::validation_error::invalid_option_value);
  else
    v = boost::any(io_type);
}

} // anonymous namespace

static void rbd_bencher_completion(void *c, void *pc);
struct rbd_bencher;

struct bencher_completer {
  rbd_bencher *bencher;
  bufferlist *bl;

public:
  bencher_completer(rbd_bencher *bencher, bufferlist *bl)
    : bencher(bencher), bl(bl)
  { }

  ~bencher_completer()
  {
    if (bl)
      delete bl;
  }
};

struct rbd_bencher {
  librbd::Image *image;
  Mutex lock;
  Cond cond;
  int in_flight;
  io_type_t io_type;
  uint64_t io_size;
  bufferlist write_bl;

  explicit rbd_bencher(librbd::Image *i, io_type_t io_type, uint64_t io_size)
    : image(i),
      lock("rbd_bencher::lock"),
      in_flight(0),
      io_type(io_type),
      io_size(io_size)
  {
    if (io_type == IO_TYPE_WRITE) {
      bufferptr bp(io_size);
      memset(bp.c_str(), rand() & 0xff, io_size);
      write_bl.push_back(bp);
    }
  }


  bool start_io(int max, uint64_t off, uint64_t len, int op_flags)
  {
    {
      Mutex::Locker l(lock);
      if (in_flight >= max)
        return false;
      in_flight++;
    }

    librbd::RBD::AioCompletion *c;
    if (io_type == IO_TYPE_READ) {
      bufferlist *read_bl = new bufferlist();
      c = new librbd::RBD::AioCompletion((void *)(new bencher_completer(this, read_bl)),
					 rbd_bencher_completion);
      image->aio_read2(off, len, *read_bl, c, op_flags);
    } else if (io_type == IO_TYPE_WRITE) {
      c = new librbd::RBD::AioCompletion((void *)(new bencher_completer(this, NULL)),
					 rbd_bencher_completion);
      image->aio_write2(off, len, write_bl, c, op_flags);
    } else {
      assert(0 == "Invalid io_type");
    }
    //cout << "start " << c << " at " << off << "~" << len << std::endl;
    return true;
  }

  void wait_for(int max) {
    Mutex::Locker l(lock);
    while (in_flight > max) {
      utime_t dur;
      dur.set_from_double(.2);
      cond.WaitInterval(lock, dur);
    }
  }

};

void rbd_bencher_completion(void *vc, void *pc)
{
  librbd::RBD::AioCompletion *c = (librbd::RBD::AioCompletion *)vc;
  bencher_completer *bc = static_cast<bencher_completer *>(pc);
  rbd_bencher *b = bc->bencher;
  //cout << "complete " << c << std::endl;
  int ret = c->get_return_value();
  if (b->io_type == IO_TYPE_WRITE && ret != 0) {
    cout << "write error: " << cpp_strerror(ret) << std::endl;
    exit(ret < 0 ? -ret : ret);
  } else if (b->io_type == IO_TYPE_READ && (unsigned int)ret != b->io_size) {
    cout << "read error: " << cpp_strerror(ret) << std::endl;
    exit(ret < 0 ? -ret : ret);
  }
  b->lock.Lock();
  b->in_flight--;
  b->cond.Signal();
  b->lock.Unlock();
  c->release();
  delete bc;
}

int do_bench(librbd::Image& image, io_type_t io_type,
		   uint64_t io_size, uint64_t io_threads,
		   uint64_t io_bytes, bool random)
{
  uint64_t size = 0;
  image.size(&size);
  if (io_size > size) {
    std::cerr << "rbd: io-size " << prettybyte_t(io_size) << " "
              << "larger than image size " << prettybyte_t(size) << std::endl;
    return -EINVAL;
  }

  if (io_size > std::numeric_limits<uint32_t>::max()) {
    std::cerr << "rbd: io-size should be less than 4G" << std::endl;
    return -EINVAL;
  }

  rbd_bencher b(&image, io_type, io_size);

  std::cout << "bench "
       << " type " << (io_type == IO_TYPE_READ ? "read" : "write")
       << " io_size " << io_size
       << " io_threads " << io_threads
       << " bytes " << io_bytes
       << " pattern " << (random ? "random" : "sequential")
       << std::endl;

  srand(time(NULL) % (unsigned long) -1);

  utime_t start = ceph_clock_now();
  utime_t last;
  unsigned ios = 0;

  vector<uint64_t> thread_offset;
  uint64_t i;
  uint64_t start_pos;

  // disturb all thread's offset, used by seq IO
  for (i = 0; i < io_threads; i++) {
    start_pos = (rand() % (size / io_size)) * io_size;
    thread_offset.push_back(start_pos);
  }

  const int WINDOW_SIZE = 5;
  typedef boost::accumulators::accumulator_set<
    double, boost::accumulators::stats<
      boost::accumulators::tag::rolling_sum> > RollingSum;

  RollingSum time_acc(
    boost::accumulators::tag::rolling_window::window_size = WINDOW_SIZE);
  RollingSum ios_acc(
    boost::accumulators::tag::rolling_window::window_size = WINDOW_SIZE);
  RollingSum off_acc(
    boost::accumulators::tag::rolling_window::window_size = WINDOW_SIZE);
  uint64_t cur_ios = 0;
  uint64_t cur_off = 0;

  int op_flags;
  if  (random) {
    op_flags = LIBRADOS_OP_FLAG_FADVISE_RANDOM;
  } else {
    op_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL;
  }

  printf("  SEC       OPS   OPS/SEC   BYTES/SEC\n");
  uint64_t off;
  for (off = 0; off < io_bytes; ) {
    b.wait_for(io_threads - 1);
    i = 0;
    while (i < io_threads && off < io_bytes) {
      if (random) {
        thread_offset[i] = (rand() % (size / io_size)) * io_size;
      } else {
        thread_offset[i] += io_size;
        if (thread_offset[i] + io_size > size)
          thread_offset[i] = 0;
      }

      if (!b.start_io(io_threads, thread_offset[i], io_size, op_flags))
        break;

      ++i;
      ++ios;
      off += io_size;

      ++cur_ios;
      cur_off += io_size;
    }

    utime_t now = ceph_clock_now();
    utime_t elapsed = now - start;
    if (last.is_zero()) {
      last = elapsed;
    } else if (elapsed.sec() != last.sec()) {
      time_acc(elapsed - last);
      ios_acc(static_cast<double>(cur_ios));
      off_acc(static_cast<double>(cur_off));
      cur_ios = 0;
      cur_off = 0;

      double time_sum = boost::accumulators::rolling_sum(time_acc);
      printf("%5d  %8d  %8.2lf  %8.2lf\n",
             (int)elapsed,
             (int)(ios - io_threads),
             boost::accumulators::rolling_sum(ios_acc) / time_sum,
             boost::accumulators::rolling_sum(off_acc) / time_sum);
      last = elapsed;
    }
  }
  b.wait_for(0);
  int r = image.flush();
  if (r < 0) {
    std::cerr << "Error flushing data at the end: " << cpp_strerror(r)
              << std::endl;
  }

  utime_t now = ceph_clock_now();
  double elapsed = now - start;

  printf("elapsed: %5d  ops: %8d  ops/sec: %8.2lf  bytes/sec: %8.2lf\n",
         (int)elapsed, ios, (double)ios / elapsed, (double)off / elapsed);

  return 0;
}

void add_bench_common_options(po::options_description *positional,
			      po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);

  options->add_options()
    ("io-size", po::value<Size>(), "IO size (in B/K/M/G/T) [default: 4K]")
    ("io-threads", po::value<uint32_t>(), "ios in flight [default: 16]")
    ("io-total", po::value<Size>(), "total size for IO (in B/K/M/G/T) [default: 1G]")
    ("io-pattern", po::value<IOPattern>(), "IO pattern (rand or seq) [default: seq]");
}

void get_arguments_for_write(po::options_description *positional,
                             po::options_description *options) {
  add_bench_common_options(positional, options);
}

void get_arguments_for_bench(po::options_description *positional,
                             po::options_description *options) {
  add_bench_common_options(positional, options);

  options->add_options()
    ("io-type", po::value<IOType>()->required(), "IO type (read or write)");
}

int bench_execute(const po::variables_map &vm, io_type_t bench_io_type) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  utils::SnapshotPresence snap_presence = utils::SNAPSHOT_PRESENCE_NONE;
  if (bench_io_type == IO_TYPE_READ)
    snap_presence = utils::SNAPSHOT_PRESENCE_PERMITTED;

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, snap_presence, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  uint64_t bench_io_size;
  if (vm.count("io-size")) {
    bench_io_size = vm["io-size"].as<uint64_t>();
  } else {
    bench_io_size = 4096;
  }
  if (bench_io_size == 0) {
    std::cerr << "rbd: --io-size should be greater than zero." << std::endl;
    return -EINVAL;
  }

  uint32_t bench_io_threads;
  if (vm.count("io-threads")) {
    bench_io_threads = vm["io-threads"].as<uint32_t>();
  } else {
    bench_io_threads = 16;
  }
  if (bench_io_threads == 0) {
    std::cerr << "rbd: --io-threads should be greater than zero." << std::endl;
    return -EINVAL;
  }

  uint64_t bench_bytes;
  if (vm.count("io-total")) {
    bench_bytes = vm["io-total"].as<uint64_t>();
  } else {
    bench_bytes = 1 << 30;
  }

  bool bench_random;
  if (vm.count("io-pattern")) {
    bench_random = vm["io-pattern"].as<bool>();
  } else {
    bench_random = false;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", "", false, &rados,
                                 &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_bench(image, bench_io_type, bench_io_size, bench_io_threads,
		     bench_bytes, bench_random);
  if (r < 0) {
    std::cerr << "bench failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

int execute_for_write(const po::variables_map &vm) {
  std::cerr << "rbd: bench-write is deprecated, use rbd bench --io-type write ..." << std::endl;
  return bench_execute(vm, IO_TYPE_WRITE);
}

int execute_for_bench(const po::variables_map &vm) {
  io_type_t bench_io_type;
  if (vm.count("io-type")) {
    bench_io_type = vm["io-type"].as<io_type_t>();
  } else {
    std::cerr << "rbd: --io-type must be specified." << std::endl;
    return -EINVAL;
  }

  return bench_execute(vm, bench_io_type);
}

Shell::Action action_write(
  {"bench-write"}, {}, "Simple write benchmark. (Deprecated, please use `rbd bench --io-type write` instead.)",
		   "", &get_arguments_for_write, &execute_for_write, false);

Shell::Action action_bench(
  {"bench"}, {}, "Simple benchmark.", "", &get_arguments_for_bench, &execute_for_bench);

} // namespace bench
} // namespace action
} // namespace rbd
