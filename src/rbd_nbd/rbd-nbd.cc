#include "include/int_types.h"

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>

#include <linux/nbd.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <iostream>
#include <boost/regex.hpp>

#include "mon/MonClient.h"
#include "common/config.h"

#include "common/errno.h"
#include "common/module.h"
#include "common/safe_io.h"
#include "common/ceph_argparse.h"
#include "common/Preforker.h"
#include "global/global_init.h"

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

static void usage()
{
  std::cout << "Usage: rbd-nbd [options] --map image   Map a image to nbd device\n"
            << "               --unmap nbd_device      Unmap nbd device\n"
            << "               -s                      List mapped nbd devices\n"
            << "Options: --device nbd_device           Specify nbd device path\n"
            << "         --read-only                   Map readonly\n"
            << std::endl;
}

static Preforker forker;
static std::string devpath, poolname("rbd"), imgname, snapname;
static bool readonly = false;

#ifdef CEPH_BIG_ENDIAN
#define ntohll(a) (a)
#elif defined(CEPH_LITTLE_ENDIAN)
#define ntohll(a) swab64(a)
#else
#error "Could not determine endianess"
#endif
#define htonll(a) ntohll(a)

class NBDServer
{
private:
  int fd;
  librbd::Image &image;

public:
  NBDServer(int _fd, librbd::Image& _image)
    : fd(_fd)
    , image(_image)
    , terminated(false)
    , lock("NBDServer::Locker")
    , reader_thread(*this, &NBDServer::reader_entry)
    , writer_thread(*this, &NBDServer::writer_entry)
    , started(false)
  {}

private:
  atomic_t terminated;

  void shutdown()
  {
    if (terminated.compare_and_swap(false, true)) {
      ::shutdown(fd, SHUT_RDWR);

      Mutex::Locker l(lock);
      cond.Signal();
    }
  }

  struct IOContext
  {
    xlist<IOContext*>::item item;
    NBDServer *server;
    struct nbd_request request;
    struct nbd_reply reply;
    bufferlist data;
    int command;

    IOContext()
      : item(this)
    {}
  };

  Mutex lock;
  Cond cond;
  xlist<IOContext*> io_pending;
  xlist<IOContext*> io_finished;

  void io_start(IOContext *ctx)
  {
    Mutex::Locker l(lock);
    io_pending.push_back(&ctx->item);
  }

  void io_finish(IOContext *ctx)
  {
    Mutex::Locker l(lock);
    assert(ctx->item.is_on_list());
    ctx->item.remove_myself();
    io_finished.push_back(&ctx->item);
    cond.Signal();
  }

  IOContext *wait_io_finish()
  {
    Mutex::Locker l(lock);
    while(io_finished.empty() && !terminated.read())
      cond.Wait(lock);

    if (io_finished.empty())
      return NULL;

    IOContext *ret = io_finished.front();
    io_finished.pop_front();

    return ret;
  }

  void wait_clean()
  {
    assert(!reader_thread.is_started());
    Mutex::Locker l(lock);
    while(!io_pending.empty())
      cond.Wait(lock);

    while(!io_finished.empty()) {
      ceph::unique_ptr<IOContext> free_ctx(io_finished.front());
      io_finished.pop_front();
    }
  }

  static void aio_callback(librbd::completion_t cb, void *arg)
  {
    librbd::RBD::AioCompletion *aio_completion =
    reinterpret_cast<librbd::RBD::AioCompletion*>(cb);

    IOContext *ctx = reinterpret_cast<IOContext *>(arg);
    int ret = aio_completion->get_return_value();
    if (ret > 0)
      ret = 0;
    ctx->reply.error = htonl(ret);
    ctx->server->io_finish(ctx);

    aio_completion->release();
  }

  void reader_entry()
  {
    while (!terminated.read()) {
      ceph::unique_ptr<IOContext> ctx(new IOContext());
      ctx->server = this;
      if (safe_read_exact(fd, &ctx->request, sizeof(struct nbd_request)) < 0)
        return;

      if (ctx->request.magic != htonl(NBD_REQUEST_MAGIC))
        return;

      ctx->request.from = ntohll(ctx->request.from);
      ctx->request.type = ntohl(ctx->request.type);
      ctx->request.len = ntohl(ctx->request.len);

      ctx->reply.magic = htonl(NBD_REPLY_MAGIC);
      memcpy(ctx->reply.handle, ctx->request.handle, sizeof(ctx->reply.handle));

      ctx->command = ctx->request.type & 0x0000ffff;

      switch (ctx->command)
      {
        case NBD_CMD_DISC:
          return;
        case NBD_CMD_WRITE:
          bufferptr ptr(ctx->request.len);
          if (safe_read_exact(fd, ptr.c_str(), ctx->request.len) < 0)
            return;
          ctx->data.push_back(ptr);
          break;
      }

      IOContext *pctx = ctx.release();
      io_start(pctx);
      librbd::RBD::AioCompletion *c = new librbd::RBD::AioCompletion(pctx, aio_callback);
      switch (pctx->command)
      {
        case NBD_CMD_WRITE:
          image.aio_write(pctx->request.from, pctx->request.len, pctx->data, c);
          break;
        case NBD_CMD_READ:
          image.aio_read(pctx->request.from, pctx->request.len, pctx->data, c);
          break;
        case NBD_CMD_FLUSH:
          image.aio_flush(c);
          break;
        case NBD_CMD_TRIM:
          image.aio_discard(pctx->request.from, pctx->request.len, c);
          break;
        default:
          return;
      }
    }
  }

  void writer_entry()
  {
    while (!terminated.read()) {
      ceph::unique_ptr<IOContext> ctx(wait_io_finish());
      if (!ctx)
        return;

      if (safe_write(fd, &ctx->reply, sizeof(struct nbd_reply)) < 0)
        return;
      if (ctx->command == NBD_CMD_READ && ctx->reply.error == htonl(0)) {
        if (ctx->data.write_fd(fd) < 0)
          return;
      }
    }
  }

  class ThreadHelper : public Thread
  {
  public:
    typedef void (NBDServer::*entry_func)();
  private:
    NBDServer &server;
    entry_func func;
  public:
    ThreadHelper(NBDServer &_server, entry_func _func)
      :server(_server)
      ,func(_func)
    {}
  protected:
    virtual void* entry()
    {
      (server.*func)();
      server.shutdown();
      return NULL;
    }
  } reader_thread, writer_thread;

  bool started;
public:
  void start()
  {
    if (!started) {
      started = true;

      reader_thread.create();
      writer_thread.create();
    }
  }

  void stop()
  {
    if (started) {
      shutdown();

      reader_thread.join();
      writer_thread.join();

      wait_clean();

      started = false;
    }
  }

  ~NBDServer()
  {
    stop();
  }
};

static int open_device(const char* path)
{
  int nbd = open(path, O_RDWR);
  if (nbd < 0) {
    int r = module_load("nbd", NULL);
    if (r < 0) {
      cerr << "rbd-nbd: failed to load nbd kernel module: " << cpp_strerror(-r) << std::endl;
      return r;
    }
    nbd = open(path, O_RDWR);
  }
  return nbd;
}

static int do_map()
{
  int r;
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx io_ctx;
  librbd::Image image;
  int read_only;
  unsigned long size;
  librbd::image_info_t info;
  int fd[2];
  struct sigaction sa;

  int nbd = open_device(devpath.c_str());
  if (nbd < 0)
    return nbd;

  unsigned long flags = NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM | NBD_FLAG_HAS_FLAGS;
  if (!snapname.empty() || readonly)
    flags |= NBD_FLAG_READ_ONLY;

  r = rados.init_with_context(g_ceph_context);
  if (r < 0)
    goto close_nbd;

  r = rados.connect();
  if (r < 0)
    goto close_nbd;

  r = rados.ioctx_create(poolname.c_str(), io_ctx);
  if (r < 0)
    goto close_nbd;

  r = rbd.open(io_ctx, image, imgname.c_str());
  if (r < 0)
    goto close_nbd;

  if (!snapname.empty()) {
    r = image.snap_set(snapname.c_str());
    if (r < 0)
      goto close_nbd;
  }

  r = image.stat(info, sizeof(info));
  if (r < 0)
    goto close_nbd;

  r = ioctl(nbd, NBD_SET_BLKSIZE, 4096UL);
  if (r < 0)
    goto close_nbd;

  size = info.size >> 12;
  r = ioctl(nbd, NBD_SET_SIZE_BLOCKS, size);
  if (r < 0)
    goto close_nbd;

  ioctl(nbd, NBD_CLEAR_SOCK);
  ioctl(nbd, NBD_SET_FLAGS, flags);

  read_only = snapname.empty() ? 0 : 1;
  r = ioctl(nbd, BLKROSET, (unsigned long) &read_only);
  if (r < 0)
    goto close_nbd;

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd) == -1) {
    r = -errno;
    goto close_nbd;
  }

  r = ioctl(nbd, NBD_SET_SOCK, fd[0]);
  if (r < 0)
    goto close_fd;

  cout << devpath << std::endl;

  if (g_conf->daemonize)
    forker.daemonize();

  {
    NBDServer server(fd[1], image);

    server.start();
    ioctl(nbd, NBD_DO_IT);
    server.stop();
  }

close_fd:
  close(fd[0]);
  close(fd[1]);
close_nbd:
  close(nbd);
  image.close();
  io_ctx.close();
  rados.shutdown();
  return r;
}

static int do_unmap()
{
  int nbd = open(devpath.c_str(), O_RDWR);
  if (nbd < 0)
    return nbd;

  int r = ioctl(nbd, NBD_DISCONNECT);
  if (r < 0)
    goto out;
  r = ioctl(nbd, NBD_CLEAR_SOCK);

out:
  close(nbd);
  return r;
}

static int parse_imgpath(const std::string &imgpath)
{
  boost::regex pattern("^(?:([^/@]+)/)?([^/@]+)(?:@([^/@]+))?$");
  boost::smatch match;
  if (!boost::regex_match(imgpath, match, pattern)) {
    std::cerr << "rbd-nbd: invalid spec '" << imgpath << "'" << std::endl;
    return -EINVAL;
  }

  if (match[1].matched)
    poolname = match[1];

  imgname = match[2];

  if (match[3].matched)
    snapname = match[3];

  return 0;
}

static bool find_empty_nbd(std::string &devpath)
{
  int id = 0;
  char path[64];

  devpath.clear();
  while (devpath.empty()) {
    snprintf(path, sizeof(path), "/dev/nbd%d", id);
    int nbd = open_device(path);
    if (nbd < 0)
      break;

    int r = ioctl(nbd, NBD_DO_IT);
    close(nbd);
    if (r < 0) {
      if (errno == EINVAL) {
        devpath = path;
        break;
      }
      ++id;
    } else {
      break;
    }
  }

  return !devpath.empty();
}

static void list_mapped_devices()
{
  char path[64];
  int m = 0;

  while (1) {
    snprintf(path, sizeof(path), "/dev/nbd%d", m);
    int nbd = open_device(path);
    if (nbd < 0)
      break;
    int r = ioctl(nbd, NBD_DO_IT);
    if (r < 0 && errno == EBUSY)
      cout << path << std::endl;
    close(nbd);
    m++;
  }
}

static int rbd_nbd(int argc, const char *argv[])
{
  int r;
  enum {
    None,
    Connect,
    Disconnect,
    Showmapped
  } cmd = None;
  vector<const char*> args;

  argv_to_vec(argc, argv, args);

  std::string val;
  std::vector<const char*>::iterator i;

  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &val, "--device", (char *)NULL)) {
      devpath = val;
    } else if (ceph_argparse_flag(args, i, "-s", "--showmapped", (char *)NULL)) {
      cmd = Showmapped;
    } else if (ceph_argparse_flag(args, i, "--read-only", (char *)NULL)) {
      readonly = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--map", (char *)NULL)) {
      cmd = Connect;
      if (parse_imgpath(val) < 0)
        return EXIT_FAILURE;
    } else if (ceph_argparse_witharg(args, i, &val, "--unmap", (char *)NULL)) {
      cmd = Disconnect;
      devpath = val;
    } else {
      ++i;
    }
  }

  env_to_vec(args);
  int flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS;
  if (cmd != Connect)
    flags |= CINIT_FLAG_NO_DAEMON_ACTIONS;
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON, flags);
  common_init_finish(g_ceph_context);

  if (args.begin() != args.end()) {
    cerr << "rbd-nbd: Unknown args: " << *args.begin() << std::endl;
    return -EINVAL;
  }

  switch (cmd) {
    case Connect:
      if (imgname.empty()) {
        cerr << "rbd-nbd: image name was not specified" << std::endl;
        return EXIT_FAILURE;
      }
      if (devpath.empty()) {
        if (!find_empty_nbd(devpath)) {
          cerr << "rbd-nbd: failed to find unused NBD device." << std::endl;
          return EXIT_FAILURE;
        }
      }

      r = do_map();
      if (r < 0) {
        cerr << "Failed to map NBD: " << cpp_strerror(r) << std::endl;
        return EXIT_FAILURE;
      }
      break;
    case Disconnect:
      r = do_unmap();
      if (r < 0) {
        cerr << "Failed to unmap NBD: " << cpp_strerror(r) << std::endl;
        return EXIT_FAILURE;
      }
      break;
    case Showmapped:
      list_mapped_devices();
      break;
    default:
      usage();
      return EXIT_FAILURE;
  }

  return 0;
}

int main(int argc, const char *argv[])
{
  std::string err;

  if (forker.prefork(err) < 0) {
    cerr << err << std::endl;
    return EXIT_FAILURE;
  }

  if (forker.is_child()) {
    forker.exit(rbd_nbd(argc, argv));
  } else if (forker.parent_wait(err) < 0) {
    cerr << err << std::endl;
    return EXIT_FAILURE;
  } else {
    return 0;
  }
}
