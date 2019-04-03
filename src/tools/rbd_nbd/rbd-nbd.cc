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

#include <linux/nbd.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <boost/algorithm/string/predicate.hpp>

#include "common/Formatter.h"
#include "common/Preforker.h"
#include "common/TextTable.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/module.h"
#include "common/safe_io.h"
#include "common/version.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "include/stringify.h"
#include "include/xlist.h"

#include "mon/MonClient.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd-nbd: "

struct Config {
  int nbds_max = 0;
  int max_part = 255;
  int timeout = -1;

  bool exclusive = false;
  bool readonly = false;
  bool set_max_part = false;

  std::string poolname;
  std::string nsname;
  std::string imgname;
  std::string snapname;
  std::string devpath;

  std::string format;
  bool pretty_format = false;
};

static void usage()
{
  std::cout << "Usage: rbd-nbd [options] map <image-or-snap-spec>  Map an image to nbd device\n"
            << "               unmap <device|image-or-snap-spec>   Unmap nbd device\n"
            << "               [options] list-mapped               List mapped nbd devices\n"
            << "Map options:\n"
            << "  --device <device path>  Specify nbd device path\n"
            << "  --read-only             Map read-only\n"
            << "  --nbds_max <limit>      Override for module param nbds_max\n"
            << "  --max_part <limit>      Override for module param max_part\n"
            << "  --exclusive             Forbid writes by other clients\n"
            << "  --timeout <seconds>     Set nbd request timeout\n"
            << "\n"
            << "List options:\n"
            << "  --format plain|json|xml Output format (default: plain)\n"
            << "  --pretty-format         Pretty formatting (json and xml)\n"
            << std::endl;
  generic_server_usage();
}

static int nbd = -1;

enum Command {
  None,
  Connect,
  Disconnect,
  List
};

static Command cmd = None;

#define RBD_NBD_BLKSIZE 512UL

#define HELP_INFO 1
#define VERSION_INFO 2

#ifdef CEPH_BIG_ENDIAN
#define ntohll(a) (a)
#elif defined(CEPH_LITTLE_ENDIAN)
#define ntohll(a) swab(a)
#else
#error "Could not determine endianess"
#endif
#define htonll(a) ntohll(a)

static int parse_args(vector<const char*>& args, std::ostream *err_msg,
                      Command *command, Config *cfg);

static void handle_signal(int signum)
{
  ceph_assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got signal " << sig_str(signum) << " ***" << dendl;
  dout(20) << __func__ << ": " << "sending NBD_DISCONNECT" << dendl;
  if (ioctl(nbd, NBD_DISCONNECT) < 0) {
    derr << "rbd-nbd: disconnect failed: " << cpp_strerror(errno) << dendl;
  } else {
    dout(20) << __func__ << ": " << "disconnected" << dendl;
  }
}

class NBDServer
{
private:
  int fd;
  librbd::Image &image;

public:
  NBDServer(int _fd, librbd::Image& _image)
    : fd(_fd)
    , image(_image)
    , lock("NBDServer::Locker")
    , reader_thread(*this, &NBDServer::reader_entry)
    , writer_thread(*this, &NBDServer::writer_entry)
    , started(false)
  {}

private:
  std::atomic<bool> terminated = { false };

  void shutdown()
  {
    bool expected = false;
    if (terminated.compare_exchange_strong(expected, true)) {
      ::shutdown(fd, SHUT_RDWR);

      Mutex::Locker l(lock);
      cond.Signal();
    }
  }

  struct IOContext
  {
    xlist<IOContext*>::item item;
    NBDServer *server = nullptr;
    struct nbd_request request;
    struct nbd_reply reply;
    bufferlist data;
    int command = 0;

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
    ceph_assert(ctx->item.is_on_list());
    ctx->item.remove_myself();
    io_finished.push_back(&ctx->item);
    cond.Signal();
  }

  IOContext *wait_io_finish()
  {
    Mutex::Locker l(lock);
    while(io_finished.empty() && !terminated)
      cond.Wait(lock);

    if (io_finished.empty())
      return NULL;

    IOContext *ret = io_finished.front();
    io_finished.pop_front();

    return ret;
  }

  void wait_clean()
  {
    ceph_assert(!reader_thread.is_started());
    Mutex::Locker l(lock);
    while(!io_pending.empty())
      cond.Wait(lock);

    while(!io_finished.empty()) {
      std::unique_ptr<IOContext> free_ctx(io_finished.front());
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

    if (ret == -EINVAL) {
      // if shrinking an image, a pagecache writeback might reference
      // extents outside of the range of the new image extents
      dout(0) << __func__ << ": masking IO out-of-bounds error" << dendl;
      ctx->data.clear();
      ret = 0;
    }

    if (ret < 0) {
      ctx->reply.error = htonl(-ret);
    } else if ((ctx->command == NBD_CMD_READ) &&
                ret < static_cast<int>(ctx->request.len)) {
      int pad_byte_count = static_cast<int> (ctx->request.len) - ret;
      ctx->data.append_zero(pad_byte_count);
      dout(20) << __func__ << ": " << *ctx << ": Pad byte count: "
               << pad_byte_count << dendl;
      ctx->reply.error = htonl(0);
    } else {
      ctx->reply.error = htonl(0);
    }
    ctx->server->io_finish(ctx);

    aio_completion->release();
  }

  void reader_entry()
  {
    while (!terminated) {
      std::unique_ptr<IOContext> ctx(new IOContext());
      ctx->server = this;

      dout(20) << __func__ << ": waiting for nbd request" << dendl;

      int r = safe_read_exact(fd, &ctx->request, sizeof(struct nbd_request));
      if (r < 0) {
	derr << "failed to read nbd request header: " << cpp_strerror(r)
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
          // NBD_DO_IT will return when pipe is closed
	  dout(0) << "disconnect request received" << dendl;
          return;
        case NBD_CMD_WRITE:
          bufferptr ptr(ctx->request.len);
	  r = safe_read_exact(fd, ptr.c_str(), ctx->request.len);
          if (r < 0) {
	    derr << *ctx << ": failed to read nbd request data: "
		 << cpp_strerror(r) << dendl;
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
          c->release();
          return;
      }
    }
    dout(20) << __func__ << ": terminated" << dendl;
  }

  void writer_entry()
  {
    while (!terminated) {
      dout(20) << __func__ << ": waiting for io request" << dendl;
      std::unique_ptr<IOContext> ctx(wait_io_finish());
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
    void* entry() override
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

  ~NBDServer()
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
    os << " UNKNOWN(" << ctx.command << ") ";
    break;
  }

  os << ctx.request.from << "~" << ctx.request.len << " "
     << std::dec << ntohl(ctx.reply.error) << "]";

  return os;
}

class NBDWatchCtx : public librbd::UpdateWatchCtx
{
private:
  int fd;
  librados::IoCtx &io_ctx;
  librbd::Image &image;
  unsigned long size;
public:
  NBDWatchCtx(int _fd,
              librados::IoCtx &_io_ctx,
              librbd::Image &_image,
              unsigned long _size)
    : fd(_fd)
    , io_ctx(_io_ctx)
    , image(_image)
    , size(_size)
  { }

  ~NBDWatchCtx() override {}

  void handle_notify() override
  {
    librbd::image_info_t info;
    if (image.stat(info, sizeof(info)) == 0) {
      unsigned long new_size = info.size;

      if (new_size != size) {
        dout(5) << "resize detected" << dendl;
        if (ioctl(fd, BLKFLSBUF, NULL) < 0)
            derr << "invalidate page cache failed: " << cpp_strerror(errno)
                 << dendl;
        if (ioctl(fd, NBD_SET_SIZE, new_size) < 0) {
            derr << "resize failed: " << cpp_strerror(errno) << dendl;
        } else {
          size = new_size;
        }
        if (ioctl(fd, BLKRRPART, NULL) < 0) {
          derr << "rescan of partition table failed: " << cpp_strerror(errno)
               << dendl;
        }
        if (image.invalidate_cache() < 0)
            derr << "invalidate rbd cache failed" << dendl;
      }
    }
  }
};

class NBDListIterator {
public:
  bool get(int *pid, Config *cfg) {
    while (true) {
      std::string nbd_path = "/sys/block/nbd" + stringify(m_index);
      if(access(nbd_path.c_str(), F_OK) != 0) {
        return false;
      }

      *cfg = Config();
      cfg->devpath = "/dev/nbd" + stringify(m_index++);

      std::ifstream ifs;
      ifs.open(nbd_path + "/pid", std::ifstream::in);
      if (!ifs.is_open()) {
        continue;
      }
      ifs >> *pid;

      int r = get_mapped_info(*pid, cfg);
      if (r < 0) {
        continue;
      }

      return true;
    }
  }

private:
  int m_index = 0;

  int get_mapped_info(int pid, Config *cfg) {
    int r;
    std::string path = "/proc/" + stringify(pid) + "/cmdline";
    std::ifstream ifs;
    std::string cmdline;
    std::vector<const char*> args;

    ifs.open(path.c_str(), std::ifstream::in);
    if (!ifs.is_open())
      return -1;
    ifs >> cmdline;

    for (unsigned i = 0; i < cmdline.size(); i++) {
      const char *arg = &cmdline[i];
      if (i == 0) {
        if (strcmp(basename(arg) , "rbd-nbd") != 0) {
          return -EINVAL;
        }
      } else {
        args.push_back(arg);
      }

      while (cmdline[i] != '\0') {
        i++;
      }
    }

    std::ostringstream err_msg;
    Command command;
    r = parse_args(args, &err_msg, &command, cfg);
    if (r < 0) {
      return r;
    }

    if (command != Connect) {
      return -ENOENT;
    }

    return 0;
  }
};

static int open_device(const char* path, Config *cfg = nullptr, bool try_load_module = false)
{
  int nbd = open(path, O_RDWR);
  bool loaded_module = false;

  if (nbd < 0 && try_load_module && access("/sys/module/nbd", F_OK) != 0) {
    ostringstream param;
    int r;
    if (cfg->nbds_max) {
      param << "nbds_max=" << cfg->nbds_max;
    }
    if (cfg->max_part) {
        param << " max_part=" << cfg->max_part;
    }
    r = module_load("nbd", param.str().c_str());
    if (r < 0) {
      cerr << "rbd-nbd: failed to load nbd kernel module: " << cpp_strerror(-r) << std::endl;
      return r;
    } else {
      loaded_module = true;
    }
    nbd = open(path, O_RDWR);
  }

  if (try_load_module && !loaded_module &&
      (cfg->nbds_max || cfg->set_max_part)) {
    cerr << "rbd-nbd: ignoring kernel module parameter options: nbd module already loaded" 
         << std::endl;
  }

  return nbd;
}

static int check_device_size(int nbd_index, unsigned long expected_size)
{
  // There are bugs with some older kernel versions that result in an
  // overflow for large image sizes. This check is to ensure we are
  // not affected.

  unsigned long size = 0;
  std::string path = "/sys/block/nbd" + stringify(nbd_index) + "/size";
  std::ifstream ifs;
  ifs.open(path.c_str(), std::ifstream::in);
  if (!ifs.is_open()) {
    cerr << "rbd-nbd: failed to open " << path << std::endl;
    return -EINVAL;
  }
  ifs >> size;
  size *= RBD_NBD_BLKSIZE;

  if (size == 0) {
    // Newer kernel versions will report real size only after nbd
    // connect. Assume this is the case and return success.
    return 0;
  }

  if (size != expected_size) {
    cerr << "rbd-nbd: kernel reported invalid device size (" << size
         << ", expected " << expected_size << ")" << std::endl;
    return -EINVAL;
  }

  return 0;
}

static int do_map(int argc, const char *argv[], Config *cfg)
{
  int r;

  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx io_ctx;
  librbd::Image image;

  int read_only = 0;
  unsigned long flags;
  unsigned long size;

  int index = 0;
  int fd[2];

  librbd::image_info_t info;

  Preforker forker;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_DAEMON,
                         CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  g_ceph_context->_conf.set_val_or_die("pid_file", "");

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
    global_init_postfork_start(g_ceph_context);
  }

  common_init_finish(g_ceph_context);
  global_init_chdir(g_ceph_context);

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd) == -1) {
    r = -errno;
    goto close_ret;
  }

  r = rados.init_with_context(g_ceph_context);
  if (r < 0)
    goto close_fd;

  r = rados.connect();
  if (r < 0)
    goto close_fd;

  r = rados.ioctx_create(cfg->poolname.c_str(), io_ctx);
  if (r < 0)
    goto close_fd;

  io_ctx.set_namespace(cfg->nsname);

  r = rbd.open(io_ctx, image, cfg->imgname.c_str());
  if (r < 0)
    goto close_fd;

  if (cfg->exclusive) {
    r = image.lock_acquire(RBD_LOCK_MODE_EXCLUSIVE);
    if (r < 0) {
      cerr << "rbd-nbd: failed to acquire exclusive lock: " << cpp_strerror(r)
           << std::endl;
      goto close_fd;
    }
  }

  if (!cfg->snapname.empty()) {
    r = image.snap_set(cfg->snapname.c_str());
    if (r < 0)
      goto close_fd;
  }

  r = image.stat(info, sizeof(info));
  if (r < 0)
    goto close_fd;

  if (cfg->devpath.empty()) {
    char dev[64];
    bool try_load_module = true;
    const char *path = "/sys/module/nbd/parameters/nbds_max";
    int nbds_max = -1;
    if (access(path, F_OK) == 0) {
      std::ifstream ifs;
      ifs.open(path, std::ifstream::in);
      if (ifs.is_open()) {
        ifs >> nbds_max;
        ifs.close();
      }
    }

    while (true) {
      snprintf(dev, sizeof(dev), "/dev/nbd%d", index);

      nbd = open_device(dev, cfg, try_load_module);
      try_load_module = false;
      if (nbd < 0) {
        if (nbd == -EPERM && nbds_max != -1 && index < (nbds_max-1)) {
          ++index;
          continue;
        }
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

      cfg->devpath = dev;
      break;
    }
  } else {
    r = sscanf(cfg->devpath.c_str(), "/dev/nbd%d", &index);
    if (r < 0) {
      cerr << "rbd-nbd: invalid device path: " << cfg->devpath
           << " (expected /dev/nbd{num})" << std::endl;
      goto close_fd;
    }
    nbd = open_device(cfg->devpath.c_str(), cfg, true);
    if (nbd < 0) {
      r = nbd;
      cerr << "rbd-nbd: failed to open device: " << cfg->devpath << std::endl;
      goto close_fd;
    }

    r = ioctl(nbd, NBD_SET_SOCK, fd[0]);
    if (r < 0) {
      r = -errno;
      cerr << "rbd-nbd: the device " << cfg->devpath << " is busy" << std::endl;
      close(nbd);
      goto close_fd;
    }
  }

  flags = NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM | NBD_FLAG_HAS_FLAGS;
  if (!cfg->snapname.empty() || cfg->readonly) {
    flags |= NBD_FLAG_READ_ONLY;
    read_only = 1;
  }

  r = ioctl(nbd, NBD_SET_BLKSIZE, RBD_NBD_BLKSIZE);
  if (r < 0) {
    r = -errno;
    goto close_nbd;
  }

  if (info.size > ULONG_MAX) {
    r = -EFBIG;
    cerr << "rbd-nbd: image is too large (" << byte_u_t(info.size)
         << ", max is " << byte_u_t(ULONG_MAX) << ")" << std::endl;
    goto close_nbd;
  }

  size = info.size;

  r = ioctl(nbd, NBD_SET_SIZE, size);
  if (r < 0) {
    r = -errno;
    goto close_nbd;
  }

  r = check_device_size(index, size);
  if (r < 0) {
    goto close_nbd;
  }

  ioctl(nbd, NBD_SET_FLAGS, flags);

  r = ioctl(nbd, BLKROSET, (unsigned long) &read_only);
  if (r < 0) {
    r = -errno;
    goto close_nbd;
  }

  if (cfg->timeout >= 0) {
    r = ioctl(nbd, NBD_SET_TIMEOUT, (unsigned long)cfg->timeout);
    if (r < 0) {
      r = -errno;
      cerr << "rbd-nbd: failed to set timeout: " << cpp_strerror(r)
           << std::endl;
      goto close_nbd;
    }
  }

  {
    uint64_t handle;

    NBDWatchCtx watch_ctx(nbd, io_ctx, image, info.size);
    r = image.update_watch(&watch_ctx, &handle);
    if (r < 0)
      goto close_nbd;

    cout << cfg->devpath << std::endl;

    if (g_conf()->daemonize) {
      global_init_postfork_finish(g_ceph_context);
      forker.daemonize();
    }

    {
      NBDServer server(fd[1], image);

      server.start();

      init_async_signal_handler();
      register_async_signal_handler(SIGHUP, sighup_handler);
      register_async_signal_handler_oneshot(SIGINT, handle_signal);
      register_async_signal_handler_oneshot(SIGTERM, handle_signal);

      ioctl(nbd, NBD_DO_IT);

      unregister_async_signal_handler(SIGHUP, sighup_handler);
      unregister_async_signal_handler(SIGINT, handle_signal);
      unregister_async_signal_handler(SIGTERM, handle_signal);
      shutdown_async_signal_handler();
    }

    r = image.update_unwatch(handle);
    ceph_assert(r == 0);
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

static int do_unmap(const std::string &devpath)
{
  int r = 0;

  int nbd = open_device(devpath.c_str());
  if (nbd < 0) {
    cerr << "rbd-nbd: failed to open device: " << devpath << std::endl;
    return nbd;
  }

  r = ioctl(nbd, NBD_DISCONNECT);
  if (r < 0) {
      cerr << "rbd-nbd: the device is not used" << std::endl; 
  }

  close(nbd);

  return r;
}

static int parse_imgpath(const std::string &imgpath, Config *cfg,
                         std::ostream *err_msg) {
  std::regex pattern("^(?:([^/]+)/(?:([^/@]+)/)?)?([^@]+)(?:@([^/@]+))?$");
  std::smatch match;
  if (!std::regex_match(imgpath, match, pattern)) {
    std::cerr << "rbd-nbd: invalid spec '" << imgpath << "'" << std::endl;
    return -EINVAL;
  }

  if (match[1].matched) {
    cfg->poolname = match[1];
  }

  if (match[2].matched) {
    cfg->nsname = match[2];
  }

  cfg->imgname = match[3];

  if (match[4].matched)
    cfg->snapname = match[4];

  return 0;
}

static int do_list_mapped_devices(const std::string &format, bool pretty_format)
{
  bool should_print = false;
  std::unique_ptr<ceph::Formatter> f;
  TextTable tbl;

  if (format == "json") {
    f.reset(new JSONFormatter(pretty_format));
  } else if (format == "xml") {
    f.reset(new XMLFormatter(pretty_format));
  } else if (!format.empty() && format != "plain") {
    std::cerr << "rbd-nbd: invalid output format: " << format << std::endl;
    return -EINVAL;
  }

  if (f) {
    f->open_array_section("devices");
  } else {
    tbl.define_column("id", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("pool", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("namespace", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("image", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("snap", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("device", TextTable::LEFT, TextTable::LEFT);
  }

  int pid;
  Config cfg;
  NBDListIterator it;
  while (it.get(&pid, &cfg)) {
    if (f) {
      f->open_object_section("device");
      f->dump_int("id", pid);
      f->dump_string("pool", cfg.poolname);
      f->dump_string("namespace", cfg.nsname);
      f->dump_string("image", cfg.imgname);
      f->dump_string("snap", cfg.snapname);
      f->dump_string("device", cfg.devpath);
      f->close_section();
    } else {
      should_print = true;
      if (cfg.snapname.empty()) {
        cfg.snapname = "-";
      }
      tbl << pid << cfg.poolname << cfg.nsname << cfg.imgname << cfg.snapname
          << cfg.devpath << TextTable::endrow;
    }
  }

  if (f) {
    f->close_section(); // devices
    f->flush(std::cout);
  }
  if (should_print) {
    std::cout << tbl;
  }
  return 0;
}

static bool find_mapped_dev_by_spec(Config *cfg) {
  int pid;
  Config c;
  NBDListIterator it;
  while (it.get(&pid, &c)) {
    if (c.poolname == cfg->poolname && c.imgname == cfg->imgname &&
        c.snapname == cfg->snapname) {
      *cfg = c;
      return true;
    }
  }
  return false;
}


static int parse_args(vector<const char*>& args, std::ostream *err_msg,
                      Command *command, Config *cfg) {
  std::string conf_file_list;
  std::string cluster;
  CephInitParameters iparams = ceph_argparse_early_args(
          args, CEPH_ENTITY_TYPE_CLIENT, &cluster, &conf_file_list);

  ConfigProxy config{false};
  config->name = iparams.name;
  config->cluster = cluster;

  if (!conf_file_list.empty()) {
    config.parse_config_files(conf_file_list.c_str(), nullptr, 0);
  } else {
    config.parse_config_files(nullptr, nullptr, 0);
  }
  config.parse_env(CEPH_ENTITY_TYPE_CLIENT);
  config.parse_argv(args);
  cfg->poolname = config.get_val<std::string>("rbd_default_pool");

  std::vector<const char*>::iterator i;
  std::ostringstream err;

  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      return HELP_INFO;
    } else if (ceph_argparse_flag(args, i, "-v", "--version", (char*)NULL)) {
      return VERSION_INFO;
    } else if (ceph_argparse_witharg(args, i, &cfg->devpath, "--device", (char *)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &cfg->nbds_max, err, "--nbds_max", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-nbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->nbds_max < 0) {
        *err_msg << "rbd-nbd: Invalid argument for nbds_max!";
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &cfg->max_part, err, "--max_part", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-nbd: " << err.str();
        return -EINVAL;
      }
      if ((cfg->max_part < 0) || (cfg->max_part > 255)) {
        *err_msg << "rbd-nbd: Invalid argument for max_part(0~255)!";
        return -EINVAL;
      }
      cfg->set_max_part = true;
    } else if (ceph_argparse_flag(args, i, "--read-only", (char *)NULL)) {
      cfg->readonly = true;
    } else if (ceph_argparse_flag(args, i, "--exclusive", (char *)NULL)) {
      cfg->exclusive = true;
    } else if (ceph_argparse_witharg(args, i, &cfg->timeout, err, "--timeout",
                                     (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-nbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->timeout < 0) {
        *err_msg << "rbd-nbd: Invalid argument for timeout!";
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &cfg->format, err, "--format",
                                     (char *)NULL)) {
    } else if (ceph_argparse_flag(args, i, "--pretty-format", (char *)NULL)) {
      cfg->pretty_format = true;
    } else {
      ++i;
    }
  }

  Command cmd = None;
  if (args.begin() != args.end()) {
    if (strcmp(*args.begin(), "map") == 0) {
      cmd = Connect;
    } else if (strcmp(*args.begin(), "unmap") == 0) {
      cmd = Disconnect;
    } else if (strcmp(*args.begin(), "list-mapped") == 0) {
      cmd = List;
    } else {
      *err_msg << "rbd-nbd: unknown command: " <<  *args.begin();
      return -EINVAL;
    }
    args.erase(args.begin());
  }

  if (cmd == None) {
    *err_msg << "rbd-nbd: must specify command";
    return -EINVAL;
  }

  switch (cmd) {
    case Connect:
      if (args.begin() == args.end()) {
        *err_msg << "rbd-nbd: must specify image-or-snap-spec";
        return -EINVAL;
      }
      if (parse_imgpath(*args.begin(), cfg, err_msg) < 0) {
        return -EINVAL;
      }
      args.erase(args.begin());
      break;
    case Disconnect:
      if (args.begin() == args.end()) {
        *err_msg << "rbd-nbd: must specify nbd device or image-or-snap-spec";
        return -EINVAL;
      }
      if (boost::starts_with(*args.begin(), "/dev/")) {
        cfg->devpath = *args.begin();
      } else {
        if (parse_imgpath(*args.begin(), cfg, err_msg) < 0) {
          return -EINVAL;
        }
        if (!find_mapped_dev_by_spec(cfg)) {
          *err_msg << "rbd-nbd: " << *args.begin() << " is not mapped";
          return -ENOENT;
        }
      }
      args.erase(args.begin());
      break;
    default:
      //shut up gcc;
      break;
  }

  if (args.begin() != args.end()) {
    *err_msg << "rbd-nbd: unknown args: " << *args.begin();
    return -EINVAL;
  }

  *command = cmd;
  return 0;
}

static int rbd_nbd(int argc, const char *argv[])
{
  int r;
  Config cfg;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  std::ostringstream err_msg;
  r = parse_args(args, &err_msg, &cmd, &cfg);
  if (r == HELP_INFO) {
    usage();
    return 0;
  } else if (r == VERSION_INFO) {
    std::cout << pretty_version_to_str() << std::endl;
    return 0;
  } else if (r < 0) {
    cerr << err_msg.str() << std::endl;
    return r;
  }

  switch (cmd) {
    case Connect:
      if (cfg.imgname.empty()) {
        cerr << "rbd-nbd: image name was not specified" << std::endl;
        return -EINVAL;
      }

      r = do_map(argc, argv, &cfg);
      if (r < 0)
        return -EINVAL;
      break;
    case Disconnect:
      r = do_unmap(cfg.devpath);
      if (r < 0)
        return -EINVAL;
      break;
    case List:
      r = do_list_mapped_devices(cfg.format, cfg.pretty_format);
      if (r < 0)
        return -EINVAL;
      break;
    default:
      usage();
      break;
  }

  return 0;
}

int main(int argc, const char *argv[])
{
  int r = rbd_nbd(argc, argv);
  if (r < 0) {
    return EXIT_FAILURE;
  }
  return 0;
}
