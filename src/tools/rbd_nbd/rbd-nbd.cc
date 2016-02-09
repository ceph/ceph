// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * rbd-nbd - RBD in userspace
 *
 * Copyright (C) 2015 - 2016 Kylin Corporation
 *
 * Author: Yunchuan Wen <yunchuan.wen@kylin-cloud.com>
 *         Li Wang <li.wang@kylin-cloud.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

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
#include "common/dout.h"

#include "common/errno.h"
#include "common/module.h"
#include "common/safe_io.h"
#include "common/ceph_argparse.h"
#include "common/Preforker.h"
#include "global/global_init.h"

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd-nbd: "

static void usage()
{
  std::cout << "Usage: rbd-nbd [options] map <image-or-snap-spec>  Map a image to nbd device\n"
            << "               unmap <device path>                 Unmap nbd device\n"
            << "               list-mapped                         List mapped nbd devices\n"
            << "Options:\n"
            << "  --device <device path>                    Specify nbd device path\n"
            << "  --read-only                               Map readonly\n"
            << "  --nbds_max <limit>                        Override for module param\n"
            << std::endl;
  generic_server_usage();
}

static std::string devpath, poolname("rbd"), imgname, snapname;
static bool readonly = false;
static int nbds_max = 0;

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

  friend std::ostream &operator<<(std::ostream &os, const IOContext &ctx);

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

    dout(20) << __func__ << ": " << *ctx << dendl;

    if (ret < 0) {
      ctx->reply.error = htonl(-ret);
    } else if ((ctx->command == NBD_CMD_WRITE || ctx->command == NBD_CMD_READ)
	       && ret != static_cast<int>(ctx->request.len)) {
      derr << __func__ << ": " << *ctx << ": unexpected return value: " << ret
	   << " (" << ctx->request.len << " expected)" << dendl;
      ctx->reply.error = htonl(EIO);
    } else {
      ctx->reply.error = htonl(0);
    }
    ctx->server->io_finish(ctx);

    aio_completion->release();
  }

  void reader_entry()
  {
    while (!terminated.read()) {
      ceph::unique_ptr<IOContext> ctx(new IOContext());
      ctx->server = this;

      dout(20) << __func__ << ": waiting for nbd request" << dendl;

      int r = safe_read_exact(fd, &ctx->request, sizeof(struct nbd_request));
      if (r < 0) {
	derr << "failed to read nbd request header: " << cpp_strerror(errno)
	     << dendl;
	return;
      }

      if (ctx->request.magic != htonl(NBD_REQUEST_MAGIC)) {
	derr << "invalid nbd request header" << dendl;
	return;
      }

      ctx->request.from = ntohll(ctx->request.from);
      ctx->request.type = ntohl(ctx->request.type);
      ctx->request.len = ntohl(ctx->request.len);

      ctx->reply.magic = htonl(NBD_REPLY_MAGIC);
      memcpy(ctx->reply.handle, ctx->request.handle, sizeof(ctx->reply.handle));

      ctx->command = ctx->request.type & 0x0000ffff;

      dout(20) << *ctx << ": start" << dendl;

      switch (ctx->command)
      {
        case NBD_CMD_DISC:
	  dout(0) << "disconnect request received" << dendl;
          return;
        case NBD_CMD_WRITE:
          bufferptr ptr(ctx->request.len);
	  r = safe_read_exact(fd, ptr.c_str(), ctx->request.len);
          if (r < 0) {
	    derr << *ctx << ": failed to read nbd request data: "
		 << cpp_strerror(errno) << dendl;
            return;
	  }
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
	  derr << *pctx << ": invalid request command" << dendl;
          return;
      }
    }
    dout(20) << __func__ << ": terminated" << dendl;
  }

  void writer_entry()
  {
    while (!terminated.read()) {
      dout(20) << __func__ << ": waiting for io request" << dendl;
      ceph::unique_ptr<IOContext> ctx(wait_io_finish());
      if (!ctx) {
	dout(20) << __func__ << ": no io requests, terminating" << dendl;
        return;
      }

      dout(20) << __func__ << ": got: " << *ctx << dendl;

      int r = safe_write(fd, &ctx->reply, sizeof(struct nbd_reply));
      if (r < 0) {
	derr << *ctx << ": failed to write reply header: " << cpp_strerror(r)
	     << dendl;
        return;
      }
      if (ctx->command == NBD_CMD_READ && ctx->reply.error == htonl(0)) {
	r = ctx->data.write_fd(fd);
        if (r < 0) {
	  derr << *ctx << ": failed to write replay data: " << cpp_strerror(r)
	       << dendl;
          return;
	}
      }
      dout(20) << *ctx << ": finish" << dendl;
    }
    dout(20) << __func__ << ": terminated" << dendl;
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
      dout(10) << __func__ << ": starting" << dendl;

      started = true;

      reader_thread.create("rbd_reader");
      writer_thread.create("rbd_writer");
    }
  }

  void stop()
  {
    if (started) {
      dout(10) << __func__ << ": terminating" << dendl;

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

std::ostream &operator<<(std::ostream &os, const NBDServer::IOContext &ctx) {

  os << "[" << std::hex << ntohll(*((uint64_t *)ctx.request.handle));

  switch (ctx.command)
  {
  case NBD_CMD_WRITE:
    os << " WRITE ";
    break;
  case NBD_CMD_READ:
    os << " READ ";
    break;
  case NBD_CMD_FLUSH:
    os << " FLUSH ";
    break;
  case NBD_CMD_TRIM:
    os << " TRIM ";
    break;
  default:
    os << " UNKNOW(" << ctx.command << ") ";
    break;
  }

  os << ctx.request.from << "~" << ctx.request.len << " "
     << ntohl(ctx.reply.error) << "]";

  return os;
}

class NBDWatchCtx : public librados::WatchCtx2
{
private:
  int fd;
  librados::IoCtx &io_ctx;
  librbd::Image &image;
  std::string header_oid;
  unsigned long size;
public:
  NBDWatchCtx(int _fd,
              librados::IoCtx &_io_ctx,
              librbd::Image &_image,
              std::string &_header_oid,
              unsigned long _size)
    : fd(_fd)
    , io_ctx(_io_ctx)
    , image(_image)
    , header_oid(_header_oid)
    , size(_size)
  { }

  virtual ~NBDWatchCtx() {}

  virtual void handle_notify(uint64_t notify_id,
                             uint64_t cookie,
                             uint64_t notifier_id,
                             bufferlist& bl)
  {
    librbd::image_info_t info;
    if (image.stat(info, sizeof(info)) == 0) {
      unsigned long new_size = info.size;

      if (new_size != size) {
        if (ioctl(fd, BLKFLSBUF, NULL) < 0)
            derr << "invalidate page cache failed: " << cpp_strerror(errno) << dendl;
        if (ioctl(fd, NBD_SET_SIZE, new_size) < 0)
            derr << "resize failed: " << cpp_strerror(errno) << dendl;
        if (image.invalidate_cache() < 0)
            derr << "invalidate rbd cache failed" << dendl;
        size = new_size;
      }
    }

    bufferlist reply;
    io_ctx.notify_ack(header_oid, notify_id, cookie, reply);
  }

  virtual void handle_error(uint64_t cookie, int err)
  {
    //ignore
  }
};

static int open_device(const char* path, bool try_load_moudle = false)
{
  int nbd = open(path, O_RDWR);
  if (nbd < 0 && try_load_moudle && access("/sys/module/nbd", F_OK) != 0) {
    int r;
    if (nbds_max) {
      ostringstream param;
      param << "nbds_max=" << nbds_max;
      r = module_load("nbd", param.str().c_str());
    } else {
      r = module_load("nbd", NULL);
    }
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
  unsigned long flags;
  unsigned long size;

  int fd[2];
  int nbd;

  uint8_t old_format;
  librbd::image_info_t info;

  Preforker forker;

  if (global_init_prefork(g_ceph_context) >= 0) {
    std::string err;
    r = forker.prefork(err);
    if (r < 0) {
      cerr << err << std::endl;
      return r;
    }

    if (forker.is_parent()) {
      if (forker.parent_wait(err) != 0) {
	return -ENXIO;
      }
      return 0;
    }
  }

  common_init_finish(g_ceph_context);
  global_init_chdir(g_ceph_context);

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd) == -1) {
    r = -errno;
    goto close_ret;
  }

  if (devpath.empty()) {
    char dev[64];
    int index = 0;
    while (true) {
      snprintf(dev, sizeof(dev), "/dev/nbd%d", index);

      nbd = open_device(dev, true);
      if (nbd < 0) {
        r = nbd;
        cerr << "rbd-nbd: failed to find unused device" << std::endl;
        goto close_fd;
      }

      r = ioctl(nbd, NBD_SET_SOCK, fd[0]);
      if (r < 0) {
        close(nbd);
        ++index;
        continue;
      }

      devpath = dev;
      break;
    }
  } else {
    nbd = open_device(devpath.c_str(), true);
    if (nbd < 0) {
      r = nbd;
      cerr << "rbd-nbd: failed to open device: " << devpath << std::endl;
      goto close_fd;
    }

    r = ioctl(nbd, NBD_SET_SOCK, fd[0]);
    if (r < 0) {
      r = -errno;
      cerr << "rbd-nbd: the device " << devpath << " is busy" << std::endl;
      close(nbd);
      goto close_fd;
    }
  }

  flags = NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM | NBD_FLAG_HAS_FLAGS;
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

  r = ioctl(nbd, NBD_SET_BLKSIZE, 512UL);
  if (r < 0) {
    r = -errno;
    goto close_nbd;
  }

  size = info.size;
  r = ioctl(nbd, NBD_SET_SIZE, size);
  if (r < 0) {
    r = -errno;
    goto close_nbd;
  }

  ioctl(nbd, NBD_SET_FLAGS, flags);

  read_only = snapname.empty() ? 0 : 1;
  r = ioctl(nbd, BLKROSET, (unsigned long) &read_only);
  if (r < 0) {
    r = -errno;
    goto close_nbd;
  }

  r = image.old_format(&old_format);
  if (r < 0)
    goto close_nbd;

  {
    string header_oid;
    uint64_t watcher;

    if (old_format != 0) {
      header_oid = imgname + RBD_SUFFIX;
    } else {
      char prefix[RBD_MAX_BLOCK_NAME_SIZE + 1];
      strncpy(prefix, info.block_name_prefix, RBD_MAX_BLOCK_NAME_SIZE);
      prefix[RBD_MAX_BLOCK_NAME_SIZE] = '\0';

      std::string image_id(prefix + strlen(RBD_DATA_PREFIX));
      header_oid = RBD_HEADER_PREFIX + image_id;
    }

    NBDWatchCtx watch_ctx(nbd, io_ctx, image, header_oid, info.size);
    r = io_ctx.watch2(header_oid, &watcher, &watch_ctx);
    if (r < 0)
      goto close_nbd;

    cout << devpath << std::endl;

    if (g_conf->daemonize) {
      forker.daemonize();
      global_init_postfork_start(g_ceph_context);
      global_init_postfork_finish(g_ceph_context);
    }

    {
      NBDServer server(fd[1], image);

      server.start();
      ioctl(nbd, NBD_DO_IT);
      server.stop();
    }

    io_ctx.unwatch2(watcher);
  }

close_nbd:
  if (r < 0) {
    ioctl(nbd, NBD_CLEAR_SOCK);
    cerr << "rbd-nbd: failed to map, status: " << cpp_strerror(-r) << std::endl;
  }
  close(nbd);
close_fd:
  close(fd[0]);
  close(fd[1]);
close_ret:
  image.close();
  io_ctx.close();
  rados.shutdown();

  forker.exit(r < 0 ? EXIT_FAILURE : 0);
  // Unreachable;
  return r;
}

static int do_unmap()
{
  common_init_finish(g_ceph_context);

  int nbd = open_device(devpath.c_str());
  if (nbd < 0) {
    cerr << "rbd-nbd: failed to open device: " << devpath << std::endl;
    return nbd;
  }

  if (ioctl(nbd, NBD_DISCONNECT) < 0)
    cerr << "rbd-nbd: the device is not used" << std::endl;
  ioctl(nbd, NBD_CLEAR_SOCK);
  close(nbd);

  return 0;
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

static int do_list_mapped_devices()
{
  char path[64];
  int m = 0;
  int fd[2];

  common_init_finish(g_ceph_context);

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd) == -1) {
    int r = -errno;
    cerr << "rbd-nbd: socketpair failed: " << cpp_strerror(-r) << std::endl;
    return r;
  }

  while (true) {
    snprintf(path, sizeof(path), "/dev/nbd%d", m);
    int nbd = open_device(path);
    if (nbd < 0)
      break;
    if (ioctl(nbd, NBD_SET_SOCK, fd[0]) != 0)
      cout << path << std::endl;
    else
      ioctl(nbd, NBD_CLEAR_SOCK);
    close(nbd);
    m++;
  }

  close(fd[0]);
  close(fd[1]);

  return 0;
}

static int rbd_nbd(int argc, const char *argv[])
{
  int r;
  enum {
    None,
    Connect,
    Disconnect,
    List
  } cmd = None;

  vector<const char*> args;

  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
              CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  std::vector<const char*>::iterator i;
  std::ostringstream err;

  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &devpath, "--device", (char *)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &nbds_max, err, "--nbds_max", (char *)NULL)) {
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        return EXIT_FAILURE;
      }
      if (nbds_max < 0) {
        cerr << "rbd-nbd: Invalid argument for nbds_max!" << std::endl;
        return EXIT_FAILURE;
      }
    } else if (ceph_argparse_flag(args, i, "--read-only", (char *)NULL)) {
      readonly = true;
    } else {
      ++i;
    }
  }

  if (args.begin() != args.end()) {
    if (strcmp(*args.begin(), "map") == 0) {
      cmd = Connect;
    } else if (strcmp(*args.begin(), "unmap") == 0) {
      cmd = Disconnect;
    } else if (strcmp(*args.begin(), "list-mapped") == 0) {
      cmd = List;
    } else {
      cerr << "rbd-nbd: unknown command: " << *args.begin() << std::endl;
      return EXIT_FAILURE;
    }
    args.erase(args.begin());
  }

  if (cmd == None) {
    cerr << "rbd-nbd: must specify command" << std::endl;
    return EXIT_FAILURE;
  }

  switch (cmd) {
    case Connect:
      if (args.begin() == args.end()) {
        cerr << "rbd-nbd: must specify image-or-snap-spec" << std::endl;
        return EXIT_FAILURE;
      }
      if (parse_imgpath(string(*args.begin())) < 0)
        return EXIT_FAILURE;
      args.erase(args.begin());
      break;
    case Disconnect:
      if (args.begin() == args.end()) {
        cerr << "rbd-nbd: must specify nbd device path" << std::endl;
        return EXIT_FAILURE;
      }
      devpath = *args.begin();
      args.erase(args.begin());
      break;
    default:
      //shut up gcc;
      break;
  }

  if (args.begin() != args.end()) {
    cerr << "rbd-nbd: unknown args: " << *args.begin() << std::endl;
    return EXIT_FAILURE;
  }

  switch (cmd) {
    case Connect:
      if (imgname.empty()) {
        cerr << "rbd-nbd: image name was not specified" << std::endl;
        return EXIT_FAILURE;
      }

      r = do_map();
      if (r < 0)
        return EXIT_FAILURE;
      break;
    case Disconnect:
      r = do_unmap();
      if (r < 0)
        return EXIT_FAILURE;
      break;
    case List:
      r = do_list_mapped_devices();
      if (r < 0)
        return EXIT_FAILURE;
      break;
    default:
      usage();
      return EXIT_FAILURE;
  }

  return 0;
}

int main(int argc, const char *argv[])
{
  return rbd_nbd(argc, argv);
}
