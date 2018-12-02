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

using namespace std::chrono;

namespace rbd {
namespace action {
namespace bench {

namespace at = argument_types;
namespace po = boost::program_options;

namespace {

enum io_type_t {
  IO_TYPE_READ = 0,
  IO_TYPE_WRITE,
  IO_TYPE_RW,

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
  uint64_t size = strict_iecstrtoll(s.c_str(), &parse_error);
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
  else if (io_type_string == "readwrite" || io_type_string == "rw")
    return IO_TYPE_RW;
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
    if (io_type == IO_TYPE_WRITE || io_type == IO_TYPE_RW) {
      bufferptr bp(io_size);
      memset(bp.c_str(), rand() & 0xff, io_size);
      write_bl.push_back(bp);
    }
  }
    
  void start_io(int max, uint64_t off, uint64_t len, int op_flags, bool read_flag)
  {
    {
      Mutex::Locker l(lock);
      in_flight++;
    }

    librbd::RBD::AioCompletion *c;
    if (read_flag) {
      bufferlist *read_bl = new bufferlist();
      c = new librbd::RBD::AioCompletion((void *)(new bencher_completer(this, read_bl)),
					 rbd_bencher_completion);
      image->aio_read2(off, len, *read_bl, c, op_flags);
    } else {
      c = new librbd::RBD::AioCompletion((void *)(new bencher_completer(this, NULL)),
					 rbd_bencher_completion);
      image->aio_write2(off, len, write_bl, c, op_flags);
    }
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

bool should_read(uint64_t read_proportion)
{
  uint64_t rand_num = rand() % 100;

  if (rand_num < read_proportion)
    return true;
  else
    return false;
}

int do_bench(librbd::Image& image, io_type_t io_type,
		   uint64_t io_size, uint64_t io_threads,
		   uint64_t io_bytes, bool random, uint64_t read_proportion)
{
  uint64_t size = 0;
  image.size(&size);
  if (io_size > size) {
    std::cerr << "rbd: io-size " << byte_u_t(io_size) << " "
              << "larger than image size " << byte_u_t(size) << std::endl;
    return -EINVAL;
  }

  if (io_size > std::numeric_limits<uint32_t>::max()) {
    std::cerr << "rbd: io-size should be less than 4G" << std::endl;
    return -EINVAL;
  }

  int r = image.flush();
  if (r < 0 && (r != -EROFS || io_type != IO_TYPE_READ)) {
    std::cerr << "rbd: failed to flush: " << cpp_strerror(r) << std::endl;
    return r;
  }

  rbd_bencher b(&image, io_type, io_size);

  std::cout << "bench "
       << " type " << (io_type == IO_TYPE_READ ? "read" :
                       io_type == IO_TYPE_WRITE ? "write" : "readwrite")
       << (io_type == IO_TYPE_RW ? " read:write=" +
           to_string(read_proportion) + ":" + to_string(100 - read_proportion) : "")
       << " io_size " << io_size
       << " io_threads " << io_threads
       << " bytes " << io_bytes
       << " pattern " << (random ? "random" : "sequential")
       << std::endl;

  srand(time(NULL) % (unsigned long) -1);

  coarse_mono_time start = coarse_mono_clock::now();
  chrono::duration<double> last = chrono::duration<double>::zero();
  unsigned ios = 0;

  vector<uint64_t> thread_offset;
  uint64_t i;
  uint64_t start_pos;

  uint64_t unit_len = size/io_size/io_threads;
  // disturb all thread's offset
  for (i = 0; i < io_threads; i++) {
    if (random) {
      start_pos = (rand() % (size / io_size)) * io_size;
    } else {
      start_pos = unit_len * i * io_size;
    }
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
  int read_ops = 0;
  int write_ops = 0;

  for (off = 0; off < io_bytes; ) {
    // Issue I/O
    i = 0;
    while (i < io_threads && off < io_bytes) {
      bool read_flag = should_read(read_proportion);

      b.wait_for(io_threads - 1);
      b.start_io(io_threads, thread_offset[i], io_size, op_flags, read_flag);

      ++i;
      ++ios;
      off += io_size;

      ++cur_ios;
      cur_off += io_size;

      if (read_flag)
        read_ops++;
      else
        write_ops++;
    }

    // Set the thread_offsets of next I/O
    for (i = 0; i < io_threads; ++i) {
      if (random) {
        thread_offset[i] = (rand() % (size / io_size)) * io_size;
        continue;
      }
      if (off < (io_size * unit_len * io_threads) ) {
        thread_offset[i] += io_size;
      } else {
        // thread_offset is adjusted to the chunks unassigned to threads.
        thread_offset[i] = off + (i * io_size);
      }
      if (thread_offset[i] + io_size > size)
        thread_offset[i] = unit_len * i * io_size;
    }

    coarse_mono_time now = coarse_mono_clock::now();
    chrono::duration<double> elapsed = now - start;
    if (last == chrono::duration<double>::zero()) {
      last = elapsed;
    } else if ((int)elapsed.count() != (int)last.count()) {
      time_acc((elapsed - last).count());
      ios_acc(static_cast<double>(cur_ios));
      off_acc(static_cast<double>(cur_off));
      cur_ios = 0;
      cur_off = 0;

      double time_sum = boost::accumulators::rolling_sum(time_acc);
      printf("%5d  %8d  %8.2lf  %8.2lf\n",
             (int)elapsed.count(),
             (int)(ios - io_threads),
             boost::accumulators::rolling_sum(ios_acc) / time_sum,
             boost::accumulators::rolling_sum(off_acc) / time_sum);
      last = elapsed;
    }
  }
  b.wait_for(0);

  if (io_type != IO_TYPE_READ) {
    r = image.flush();
    if (r < 0) {
      std::cerr << "rbd: failed to flush at the end: " << cpp_strerror(r)
                << std::endl;
    }
  }

  coarse_mono_time now = coarse_mono_clock::now();
  chrono::duration<double> elapsed = now - start;

  printf("elapsed: %5d  ops: %8d  ops/sec: %8.2lf  bytes/sec: %8.2lf\n",
         (int)elapsed.count(), ios, (double)ios / elapsed.count(),
         (double)off / elapsed.count());

  if (io_type == IO_TYPE_RW) {
    printf("read_ops: %5d   read_ops/sec: %8.2lf   read_bytes/sec: %8.2lf\n",
           read_ops, (double)read_ops / elapsed.count(),
           (double)read_ops * io_size / elapsed.count());

    printf("write_ops: %5d   write_ops/sec: %8.2lf   write_bytes/sec: %8.2lf\n",
           write_ops, (double)write_ops / elapsed.count(),
           (double)write_ops * io_size / elapsed.count());
  }

  return 0;
}

void add_bench_common_options(po::options_description *positional,
			      po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);

  options->add_options()
    ("io-size", po::value<Size>(), "IO size (in B/K/M/G/T) [default: 4K]")
    ("io-threads", po::value<uint32_t>(), "ios in flight [default: 16]")
    ("io-total", po::value<Size>(), "total size for IO (in B/K/M/G/T) [default: 1G]")
    ("io-pattern", po::value<IOPattern>(), "IO pattern (rand or seq) [default: seq]")
    ("rw-mix-read", po::value<uint64_t>(), "read proportion in readwrite (<= 100) [default: 50]");
}

void get_arguments_for_write(po::options_description *positional,
                             po::options_description *options) {
  add_bench_common_options(positional, options);
}

void get_arguments_for_bench(po::options_description *positional,
                             po::options_description *options) {
  add_bench_common_options(positional, options);

  options->add_options()
    ("io-type", po::value<IOType>()->required(), "IO type (read , write, or readwrite(rw))");
}

int bench_execute(const po::variables_map &vm, io_type_t bench_io_type) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  utils::SnapshotPresence snap_presence = utils::SNAPSHOT_PRESENCE_NONE;
  if (bench_io_type == IO_TYPE_READ)
    snap_presence = utils::SNAPSHOT_PRESENCE_PERMITTED;

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, true, snap_presence, utils::SPEC_VALIDATION_NONE);
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

  uint64_t bench_read_proportion;
  if (bench_io_type == IO_TYPE_READ) {
    bench_read_proportion = 100;
  } else if (bench_io_type == IO_TYPE_WRITE) {
    bench_read_proportion = 0;
  } else {
    if (vm.count("rw-mix-read")) {
      bench_read_proportion = vm["rw-mix-read"].as<uint64_t>();
    } else {
      bench_read_proportion = 50;
    }

    if (bench_read_proportion > 100) {
      std::cerr << "rbd: --rw-mix-read should not be larger than 100." << std::endl;
      return -EINVAL;
    }
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "",
                                 snap_name, false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_bench(image, bench_io_type, bench_io_size, bench_io_threads,
		     bench_bytes, bench_random, bench_read_proportion);
  if (r < 0) {
    std::cerr << "bench failed: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

int execute_for_write(const po::variables_map &vm,
                      const std::vector<std::string> &ceph_global_init_args) {
  std::cerr << "rbd: bench-write is deprecated, use rbd bench --io-type write ..." << std::endl;
  return bench_execute(vm, IO_TYPE_WRITE);
}

int execute_for_bench(const po::variables_map &vm,
                      const std::vector<std::string> &ceph_global_init_args) {
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
