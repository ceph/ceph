// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "global/global_init.h"
#include <string>
#include <vector>

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "random-write: "

namespace {

const uint32_t NUM_THREADS = 8;
const uint32_t MAX_IO_SIZE = 24576;
const uint32_t MIN_IO_SIZE = 4;

void usage() {
  std::cout << "usage: ceph_test_rbd_mirror_random_write [options...] \\" << std::endl;
  std::cout << "           <pool> <image>" << std::endl;
  std::cout << std::endl;
  std::cout << "  pool                 image pool" << std::endl;
  std::cout << "  image         image to write" << std::endl;
  std::cout << std::endl;
  std::cout << "options:\n";
  std::cout << "  -m monaddress[:port]      connect to specified monitor\n";
  std::cout << "  --keyring=<path>          path to keyring for local cluster\n";
  std::cout << "  --log-file=<logfile>      file to log debug output\n";
  std::cout << "  --debug-rbd-mirror=<log-level>/<memory-level>  set rbd-mirror debug level\n";
  generic_server_usage();
}

void rbd_bencher_completion(void *c, void *pc);

struct rbd_bencher {
  librbd::Image *image;
  Mutex lock;
  Cond cond;
  int in_flight;

  explicit rbd_bencher(librbd::Image *i)
    : image(i),
      lock("rbd_bencher::lock"),
      in_flight(0) {
  }

  bool start_write(int max, uint64_t off, uint64_t len, bufferlist& bl,
                   int op_flags) {
    {
      Mutex::Locker l(lock);
      if (in_flight >= max)
        return false;
      in_flight++;
    }
    librbd::RBD::AioCompletion *c =
      new librbd::RBD::AioCompletion((void *)this, rbd_bencher_completion);
    image->aio_write2(off, len, bl, c, op_flags);
    //cout << "start " << c << " at " << off << "~" << len << std::endl;
    return true;
  }

  void wait_for(int max) {
    Mutex::Locker l(lock);
    while (in_flight > max) {
      utime_t dur;
      dur.set_from_double(.2);
      cond.WaitInterval(g_ceph_context, lock, dur);
    }
  }

};

void rbd_bencher_completion(void *vc, void *pc) {
  librbd::RBD::AioCompletion *c = (librbd::RBD::AioCompletion *)vc;
  rbd_bencher *b = static_cast<rbd_bencher *>(pc);
  //cout << "complete " << c << std::endl;
  int ret = c->get_return_value();
  if (ret != 0) {
    cout << "write error: " << cpp_strerror(ret) << std::endl;
    exit(ret < 0 ? -ret : ret);
  }
  b->lock.Lock();
  b->in_flight--;
  b->cond.Signal();
  b->lock.Unlock();
  c->release();
}

void write_image(librbd::Image &image) {
  srand(time(NULL) % (unsigned long) -1);

  uint64_t max_io_bytes = MAX_IO_SIZE * 1024;
  bufferptr bp(max_io_bytes);
  memset(bp.c_str(), rand() & 0xff, bp.length());
  bufferlist bl;
  bl.push_back(bp);

  uint64_t size = 0;
  image.size(&size);
  assert(size != 0);

  vector<uint64_t> thread_offset;
  uint64_t i;
  uint64_t start_pos;

  // disturb all thread's offset, used by seq write
  for (i = 0; i < NUM_THREADS; i++) {
    start_pos = (rand() % (size / max_io_bytes)) * max_io_bytes;
    thread_offset.push_back(start_pos);
  }

  uint64_t total_ios = 0;
  uint64_t total_bytes = 0;
  rbd_bencher b(&image);
  while (true) {
    b.wait_for(NUM_THREADS - 1);
    for (uint32_t i = 0; i < NUM_THREADS; ++i) {
      // mostly small writes with a small chance of large writes
      uint32_t io_modulo = MIN_IO_SIZE + 1;
      if (rand() % 30 == 0) {
        io_modulo += MAX_IO_SIZE;
      }

      uint32_t io_size = (((rand() % io_modulo) + MIN_IO_SIZE) * 1024);
      thread_offset[i] = (rand() % (size / io_size)) * io_size;
      if (!b.start_write(NUM_THREADS, thread_offset[i], io_size, bl,
                         LIBRADOS_OP_FLAG_FADVISE_RANDOM)) {
        break;
      }
      ++i;

      ++total_ios;
      total_bytes += io_size;
      if (total_ios % 100 == 0) {
        std::cout << total_ios << " IOs, " << total_bytes << " bytes"
                  << std::endl;
      }
    }
  }
  b.wait_for(0);
}

} // anonymous namespace

int main(int argc, const char **argv)
{
  std::vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);

  for (auto i = args.begin(); i != args.end(); ++i) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return EXIT_SUCCESS;
    }
  }

  if (args.size() < 2) {
    usage();
    return EXIT_FAILURE;
  }

  std::string pool_name = args[0];
  std::string image_name = args[1];

  common_init_finish(g_ceph_context);

  dout(5) << "connecting to cluster" << dendl;
  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::RBD rbd;
  librbd::Image image;
  int r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "could not initialize RADOS handle" << dendl;
    return EXIT_FAILURE;
  }

  r = rados.connect();
  if (r < 0) {
    derr << "error connecting to local cluster" << dendl;
    return EXIT_FAILURE;
  }

  r = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (r < 0) {
    derr << "error finding local pool " << pool_name << ": "
	 << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }

  r = rbd.open(io_ctx, image, image_name.c_str());
  if (r < 0) {
    derr << "error opening image " << image_name << ": "
         << cpp_strerror(r) << dendl;
    return EXIT_FAILURE;
  }

  write_image(image);
  return EXIT_SUCCESS;
}
