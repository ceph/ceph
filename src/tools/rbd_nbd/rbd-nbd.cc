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

#include "acconfig.h"
#include "include/int_types.h"
#include "include/scope_guard.h"

#include <boost/endian/conversion.hpp>

#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include <linux/nbd.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/syscall.h>

#include "nbd-netlink.h"
#include <libnl3/netlink/genl/genl.h>
#include <libnl3/netlink/genl/ctrl.h>
#include <libnl3/netlink/genl/mngt.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>

#include "common/Formatter.h"
#include "common/Preforker.h"
#include "common/SubProcess.h"
#include "common/TextTable.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/event_socket.h"
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

using namespace std;
namespace fs = std::filesystem;

using boost::endian::big_to_native;
using boost::endian::native_to_big;

enum Command {
  None,
  Map,
  Unmap,
  Attach,
  Detach,
  List
};

struct Config {
  int nbds_max = 0;
  int max_part = 255;
  int io_timeout = -1;
  int reattach_timeout = 30;

  bool exclusive = false;
  bool notrim = false;
  bool quiesce = false;
  bool readonly = false;
  bool set_max_part = false;
  bool show_cookie = false;

  std::string poolname;
  std::string nsname;
  std::string imgname;
  std::string snapname;
  std::string devpath;
  std::string quiesce_hook = CMAKE_INSTALL_LIBEXECDIR "/rbd-nbd/rbd-nbd_quiesce";

  std::string format;
  bool pretty_format = false;

  std::vector<librbd::encryption_format_t> encryption_formats;
  std::vector<std::string> encryption_passphrase_files;

  Command command = None;
  int pid = 0;
  std::string cookie;
  uint64_t snapid = CEPH_NOSNAP;

  std::string image_spec() const {
    std::string spec = poolname + "/";

    if (!nsname.empty()) {
      spec += nsname + "/";
    }
    spec += imgname;

    if (!snapname.empty()) {
      spec += "@" + snapname;
    }

    return spec;
  }
};

static void usage()
{
  std::cout << "Usage: rbd-nbd [options] map <image-or-snap-spec>    Map image to nbd device\n"
            << "               detach <device|image-or-snap-spec>    Detach image from nbd device\n"
            << "               [options] attach <image-or-snap-spec> Attach image to nbd device\n"
            << "               unmap <device|image-or-snap-spec>     Unmap nbd device\n"
            << "               [options] list-mapped                 List mapped nbd devices\n"
            << "Map and attach options:\n"
            << "  --device <device path>        Specify nbd device path (/dev/nbd{num})\n"
            << "  --encryption-format luks|luks1|luks2\n"
            << "                                Image encryption format (default: luks)\n"
            << "  --encryption-passphrase-file  Path of file containing passphrase for unlocking image encryption\n"
            << "  --exclusive                   Forbid writes by other clients\n"
            << "  --notrim                      Turn off trim/discard\n"
            << "  --io-timeout <sec>            Set nbd IO timeout\n"
            << "  --max_part <limit>            Override for module param max_part\n"
            << "  --nbds_max <limit>            Override for module param nbds_max\n"
            << "  --quiesce                     Use quiesce callbacks\n"
            << "  --quiesce-hook <path>         Specify quiesce hook path\n"
            << "                                (default: " << Config().quiesce_hook << ")\n"
            << "  --read-only                   Map read-only\n"
            << "  --reattach-timeout <sec>      Set nbd re-attach timeout\n"
            << "                                (default: " << Config().reattach_timeout << ")\n"
            << "  --show-cookie                 Show device cookie\n"
            << "  --cookie                      Specify device cookie\n"
            << "  --snap-id <snap-id>           Specify snapshot by ID instead of by name\n"
            << "\n"
            << "Unmap and detach options:\n"
            << "  --device <device path>        Specify nbd device path (/dev/nbd{num})\n"
            << "  --snap-id <snap-id>           Specify snapshot by ID instead of by name\n"
            << "\n"
            << "List options:\n"
            << "  --format plain|json|xml Output format (default: plain)\n"
            << "  --pretty-format         Pretty formatting (json and xml)\n"
            << std::endl;
  generic_server_usage();
}

static int nbd = -1;
static int nbd_index = -1;
static EventSocket terminate_event_sock;

#define RBD_NBD_BLKSIZE 512UL

#define HELP_INFO 1
#define VERSION_INFO 2

static int parse_args(vector<const char*>& args, std::ostream *err_msg,
                      Config *cfg);
static int netlink_disconnect(int index);
static int netlink_resize(int nbd_index, const std::string& cookie,
                          uint64_t size);

static int run_quiesce_hook(const std::string &quiesce_hook,
                            const std::string &devpath,
                            const std::string &command);

static std::string get_cookie(const std::string &devpath);

class NBDServer
{
public:
  uint64_t quiesce_watch_handle = 0;

private:
  int fd;
  librbd::Image &image;
  Config *cfg;

public:
  NBDServer(int fd, librbd::Image& image, Config *cfg)
    : fd(fd)
    , image(image)
    , cfg(cfg)
    , reader_thread(*this, &NBDServer::reader_entry)
    , writer_thread(*this, &NBDServer::writer_entry)
    , quiesce_thread(*this, &NBDServer::quiesce_entry)
  {
    std::vector<librbd::config_option_t> options;
    image.config_list(&options);
    for (auto &option : options) {
      if ((option.name == std::string("rbd_cache") ||
           option.name == std::string("rbd_cache_writethrough_until_flush")) &&
          option.value == "false") {
        allow_internal_flush = true;
        break;
      }
    }
  }

  Config *get_cfg() const {
    return cfg;
  }

private:
  int terminate_event_fd = -1;
  ceph::mutex disconnect_lock =
    ceph::make_mutex("NBDServer::DisconnectLocker");
  ceph::condition_variable disconnect_cond;
  std::atomic<bool> terminated = { false };
  std::atomic<bool> allow_internal_flush = { false };

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

  ceph::mutex lock = ceph::make_mutex("NBDServer::Locker");
  ceph::condition_variable cond;
  xlist<IOContext*> io_pending;
  xlist<IOContext*> io_finished;

  void io_start(IOContext *ctx)
  {
    std::lock_guard l{lock};
    io_pending.push_back(&ctx->item);
  }

  void io_finish(IOContext *ctx)
  {
    std::lock_guard l{lock};
    ceph_assert(ctx->item.is_on_list());
    ctx->item.remove_myself();
    io_finished.push_back(&ctx->item);
    cond.notify_all();
  }

  IOContext *wait_io_finish()
  {
    std::unique_lock l{lock};
    cond.wait(l, [this] {
                   return !io_finished.empty() ||
                          (io_pending.empty() && terminated);
                 });

    if (io_finished.empty())
      return NULL;

    IOContext *ret = io_finished.front();
    io_finished.pop_front();

    return ret;
  }

  void wait_clean()
  {
    std::unique_lock l{lock};
    cond.wait(l, [this] { return io_pending.empty(); });

    while(!io_finished.empty()) {
      std::unique_ptr<IOContext> free_ctx(io_finished.front());
      io_finished.pop_front();
    }
  }

  void assert_clean()
  {
    std::unique_lock l{lock};

    ceph_assert(!reader_thread.is_started());
    ceph_assert(!writer_thread.is_started());
    ceph_assert(io_pending.empty());
    ceph_assert(io_finished.empty());
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
      ctx->reply.error = native_to_big<uint32_t>(-ret);
    } else if ((ctx->command == NBD_CMD_READ) &&
                ret < static_cast<int>(ctx->request.len)) {
      int pad_byte_count = static_cast<int> (ctx->request.len) - ret;
      ctx->data.append_zero(pad_byte_count);
      dout(20) << __func__ << ": " << *ctx << ": Pad byte count: "
               << pad_byte_count << dendl;
      ctx->reply.error = native_to_big<uint32_t>(0);
    } else {
      ctx->reply.error = native_to_big<uint32_t>(0);
    }
    ctx->server->io_finish(ctx);

    aio_completion->release();
  }

  void reader_entry()
  {
    struct pollfd poll_fds[2];
    memset(poll_fds, 0, sizeof(struct pollfd) * 2);
    poll_fds[0].fd = fd;
    poll_fds[0].events = POLLIN;
    poll_fds[1].fd = terminate_event_fd;
    poll_fds[1].events = POLLIN;

    while (true) {
      std::unique_ptr<IOContext> ctx(new IOContext());
      ctx->server = this;

      dout(20) << __func__ << ": waiting for nbd request" << dendl;

      int r = poll(poll_fds, 2, -1);
      if (r == -1) {
        if (errno == EINTR) {
          continue;
        }
        r = -errno;
        derr << "failed to poll nbd: " << cpp_strerror(r) << dendl;
        goto error;
      }

      if ((poll_fds[1].revents & POLLIN) != 0) {
        dout(0) << __func__ << ": terminate received" << dendl;
        goto signal;
      }

      if ((poll_fds[0].revents & POLLIN) == 0) {
        dout(20) << __func__ << ": nothing to read" << dendl;
        continue;
      }

      r = safe_read_exact(fd, &ctx->request, sizeof(struct nbd_request));
      if (r < 0) {
	derr << "failed to read nbd request header: " << cpp_strerror(r)
	     << dendl;
	goto error;
      }

      if (ctx->request.magic != htonl(NBD_REQUEST_MAGIC)) {
	derr << "invalid nbd request header" << dendl;
	goto signal;
      }

      ctx->request.from = big_to_native(ctx->request.from);
      ctx->request.type = big_to_native(ctx->request.type);
      ctx->request.len = big_to_native(ctx->request.len);

      ctx->reply.magic = native_to_big<uint32_t>(NBD_REPLY_MAGIC);
      memcpy(ctx->reply.handle, ctx->request.handle, sizeof(ctx->reply.handle));

      ctx->command = ctx->request.type & 0x0000ffff;

      dout(20) << *ctx << ": start" << dendl;

      switch (ctx->command)
      {
        case NBD_CMD_DISC:
          // NBD_DO_IT will return when pipe is closed
	  dout(0) << "disconnect request received" << dendl;
          goto signal;
        case NBD_CMD_WRITE:
          bufferptr ptr(ctx->request.len);
	  r = safe_read_exact(fd, ptr.c_str(), ctx->request.len);
          if (r < 0) {
	    derr << *ctx << ": failed to read nbd request data: "
		 << cpp_strerror(r) << dendl;
            goto error;
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
          allow_internal_flush = true;
          break;
        case NBD_CMD_TRIM:
          image.aio_discard(pctx->request.from, pctx->request.len, c);
          break;
        default:
	  derr << *pctx << ": invalid request command" << dendl;
          c->release();
          goto signal;
      }
    }
error:
    {
      int r = netlink_disconnect(nbd_index);
      if (r == 1) {
        ioctl(nbd, NBD_DISCONNECT);
      }
    }
signal:
    std::lock_guard l{lock};
    terminated = true;
    cond.notify_all();

    std::lock_guard disconnect_l{disconnect_lock};
    disconnect_cond.notify_all();

    dout(20) << __func__ << ": terminated" << dendl;
  }

  void writer_entry()
  {
    while (true) {
      dout(20) << __func__ << ": waiting for io request" << dendl;
      std::unique_ptr<IOContext> ctx(wait_io_finish());
      if (!ctx) {
	dout(20) << __func__ << ": no io requests, terminating" << dendl;
        goto done;
      }

      dout(20) << __func__ << ": got: " << *ctx << dendl;

      int r = safe_write(fd, &ctx->reply, sizeof(struct nbd_reply));
      if (r < 0) {
	derr << *ctx << ": failed to write reply header: " << cpp_strerror(r)
	     << dendl;
        goto error;
      }
      if (ctx->command == NBD_CMD_READ && ctx->reply.error == htonl(0)) {
	r = ctx->data.write_fd(fd);
        if (r < 0) {
	  derr << *ctx << ": failed to write replay data: " << cpp_strerror(r)
	       << dendl;
          goto error;
	}
      }
      dout(20) << *ctx << ": finish" << dendl;
    }
  error:
    wait_clean();
  done:
    ::shutdown(fd, SHUT_RDWR);

    dout(20) << __func__ << ": terminated" << dendl;
  }

  bool wait_quiesce() {
    dout(20) << __func__ << dendl;

    std::unique_lock locker{lock};
    cond.wait(locker, [this] { return quiesce || terminated; });

    if (terminated) {
      return false;
    }

    dout(20) << __func__ << ": got quiesce request" << dendl;
    return true;
  }

  void wait_unquiesce(std::unique_lock<ceph::mutex> &locker) {
    dout(20) << __func__ << dendl;

    cond.wait(locker, [this] { return !quiesce || terminated; });

    dout(20) << __func__ << ": got unquiesce request" << dendl;
  }

  void wait_inflight_io() {
    if (!allow_internal_flush) {
        return;
    }

    uint64_t features = 0;
    image.features(&features);
    if ((features & RBD_FEATURE_EXCLUSIVE_LOCK) != 0) {
      bool is_owner = false;
      image.is_exclusive_lock_owner(&is_owner);
      if (!is_owner) {
        return;
      }
    }

    dout(20) << __func__ << dendl;

    int r = image.flush();
    if (r < 0) {
      derr << "flush failed: " << cpp_strerror(r) << dendl;
    }
  }

  void quiesce_entry()
  {
    ceph_assert(cfg->quiesce);

    while (wait_quiesce()) {

      int r = run_quiesce_hook(cfg->quiesce_hook, cfg->devpath, "quiesce");

      wait_inflight_io();

      {
        std::unique_lock locker{lock};
        ceph_assert(quiesce == true);

        image.quiesce_complete(quiesce_watch_handle, r);

        if (r < 0) {
          quiesce = false;
          continue;
        }

        wait_unquiesce(locker);
      }

      run_quiesce_hook(cfg->quiesce_hook, cfg->devpath, "unquiesce");
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
      return NULL;
    }
  } reader_thread, writer_thread, quiesce_thread;

  bool started = false;
  bool quiesce = false;

public:
  void start()
  {
    if (!started) {
      dout(10) << __func__ << ": starting" << dendl;

      started = true;

      terminate_event_fd = eventfd(0, EFD_NONBLOCK);
      ceph_assert(terminate_event_fd > 0);
      int r = terminate_event_sock.init(terminate_event_fd,
                                        EVENT_SOCKET_TYPE_EVENTFD);
      ceph_assert(r >= 0);

      reader_thread.create("rbd_reader");
      writer_thread.create("rbd_writer");
      if (cfg->quiesce) {
        quiesce_thread.create("rbd_quiesce");
      }
    }
  }

  void wait_for_disconnect()
  {
    if (!started)
      return;

    std::unique_lock l{disconnect_lock};
    disconnect_cond.wait(l);
  }

  void notify_quiesce() {
    dout(10) << __func__ << dendl;

    ceph_assert(cfg->quiesce);

    std::unique_lock locker{lock};
    ceph_assert(quiesce == false);
    quiesce = true;
    cond.notify_all();
  }

  void notify_unquiesce() {
    dout(10) << __func__ << dendl;

    ceph_assert(cfg->quiesce);

    std::unique_lock locker{lock};
    ceph_assert(quiesce == true);
    quiesce = false;
    cond.notify_all();
  }

  ~NBDServer()
  {
    if (started) {
      dout(10) << __func__ << ": terminating" << dendl;

      terminate_event_sock.notify();

      reader_thread.join();
      writer_thread.join();
      if (cfg->quiesce) {
        quiesce_thread.join();
      }

      assert_clean();

      close(terminate_event_fd);
      started = false;
    }
  }
};

std::ostream &operator<<(std::ostream &os, const NBDServer::IOContext &ctx) {

  os << "[" << std::hex << big_to_native(*((uint64_t *)ctx.request.handle));

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
  case NBD_CMD_DISC:
    os << " DISC ";
    break;
  default:
    os << " UNKNOWN(" << ctx.command << ") ";
    break;
  }

  os << ctx.request.from << "~" << ctx.request.len << " "
     << std::dec << big_to_native(ctx.reply.error) << "]";

  return os;
}

class NBDQuiesceWatchCtx : public librbd::QuiesceWatchCtx
{
public:
  NBDQuiesceWatchCtx(NBDServer *server) : server(server) {
  }

  void handle_quiesce() override {
    server->notify_quiesce();
  }

  void handle_unquiesce() override {
    server->notify_unquiesce();
  }

private:
  NBDServer *server;
};

class NBDWatchCtx : public librbd::UpdateWatchCtx
{
private:
  int fd;
  int nbd_index;
  bool use_netlink;
  librados::IoCtx &io_ctx;
  librbd::Image &image;
  uint64_t size;
  std::thread handle_notify_thread;
  ceph::condition_variable cond;
  ceph::mutex lock = ceph::make_mutex("NBDWatchCtx::Locker");
  bool notify = false;
  bool terminated = false;
  std::string cookie;

  bool wait_notify() {
    dout(10) << __func__ << dendl;

    std::unique_lock locker{lock};
    cond.wait(locker, [this] { return notify || terminated; });

    if (terminated) {
      return false;
    }

    dout(10) << __func__ << ": got notify request" << dendl;
    notify = false;
    return true;
  }

  void handle_notify_entry() {
    dout(10) << __func__ << dendl;

    while (wait_notify()) {
      uint64_t new_size;
      int ret = image.size(&new_size);
      if (ret < 0) {
        derr << "getting image size failed: " << cpp_strerror(ret) << dendl;
        continue;
      }
      if (new_size == size) {
        continue;
      }
      dout(5) << "resize detected" << dendl;
      if (ioctl(fd, BLKFLSBUF, NULL) < 0) {
        derr << "invalidate page cache failed: " << cpp_strerror(errno)
             << dendl;
      }
      if (use_netlink) {
        ret = netlink_resize(nbd_index, cookie, new_size);
      } else {
        ret = ioctl(fd, NBD_SET_SIZE, new_size);
        if (ret < 0) {
          derr << "ioctl resize failed: " << cpp_strerror(errno) << dendl;
        }
      }
      if (!ret) {
        size = new_size;
      }
      if (ioctl(fd, BLKRRPART, NULL) < 0) {
        derr << "rescan of partition table failed: " << cpp_strerror(errno)
             << dendl;
      }
      if (image.invalidate_cache() < 0) {
        derr << "invalidate rbd cache failed" << dendl;
      }
    }
  }

public:
  NBDWatchCtx(int _fd,
              int _nbd_index,
              bool _use_netlink,
              librados::IoCtx &_io_ctx,
              librbd::Image &_image,
              unsigned long _size,
              std::string _cookie)
    : fd(_fd)
    , nbd_index(_nbd_index)
    , use_netlink(_use_netlink)
    , io_ctx(_io_ctx)
    , image(_image)
    , size(_size)
    , cookie(std::move(_cookie))
  {
    handle_notify_thread = make_named_thread("rbd_handle_notify",
                                             &NBDWatchCtx::handle_notify_entry,
                                             this);
  }

  ~NBDWatchCtx() override
  {
    dout(10) << __func__ << ": terminating" << dendl;
    std::unique_lock locker{lock};
    terminated = true;
    cond.notify_all();
    locker.unlock();

    handle_notify_thread.join();
    dout(10) << __func__ << ": finish" << dendl;
  }

  void handle_notify() override
  {
    dout(10) << __func__ << dendl;

    std::unique_lock locker{lock};
    notify = true;
    cond.notify_all();
  }
};

class NBDListIterator {
public:
  bool get(Config *cfg) {
    while (true) {
      std::string nbd_path = "/sys/block/nbd" + stringify(m_index);
      if(access(nbd_path.c_str(), F_OK) != 0) {
        return false;
      }

      *cfg = Config();
      cfg->devpath = "/dev/nbd" + stringify(m_index++);

      int pid;
      std::ifstream ifs;
      ifs.open(nbd_path + "/pid", std::ifstream::in);
      if (!ifs.is_open()) {
        continue;
      }
      ifs >> pid;
      ifs.close();

      // If the rbd-nbd is re-attached the pid may store garbage
      // here. We are sure this is the case when it is negative or
      // zero. Then we just try to find the attached process scanning
      // /proc fs. If it is positive we check the process with this
      // pid first and if it is not rbd-nbd fallback to searching the
      // attached process.
      do {
        if (pid <= 0) {
          pid = find_attached(cfg->devpath);
          if (pid <= 0) {
            break;
          }
        }

        if (get_mapped_info(pid, cfg) >= 0) {
          return true;
        }
        pid = -1;
      } while (true);
    }
  }

private:
  int m_index = 0;
  std::map<int, Config> m_mapped_info_cache;

  int get_mapped_info(int pid, Config *cfg) {
    ceph_assert(!cfg->devpath.empty());

    auto it = m_mapped_info_cache.find(pid);
    if (it != m_mapped_info_cache.end()) {
      if (it->second.devpath != cfg->devpath) {
        return -EINVAL;
      }
      *cfg = it->second;
      return 0;
    }

    m_mapped_info_cache[pid] = {};

    int r;
    std::string path = "/proc/" + stringify(pid) + "/comm";
    std::ifstream ifs;
    std::string comm;
    ifs.open(path.c_str(), std::ifstream::in);
    if (!ifs.is_open())
      return -1;
    ifs >> comm;
    if (comm != "rbd-nbd") {
      return -EINVAL;
    }
    ifs.close();

    path = "/proc/" + stringify(pid) + "/cmdline";
    std::string cmdline;
    std::vector<const char*> args;

    ifs.open(path.c_str(), std::ifstream::in);
    if (!ifs.is_open())
      return -1;
    ifs >> cmdline;

    if (cmdline.empty()) {
      return -EINVAL;
    }

    for (unsigned i = 0; i < cmdline.size(); i++) {
      char *arg = &cmdline[i];
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
    Config c;
    r = parse_args(args, &err_msg, &c);
    if (r < 0) {
      return r;
    }

    if (c.command != Map && c.command != Attach) {
      return -ENOENT;
    }

    c.pid = pid;
    m_mapped_info_cache.erase(pid);
    if (!c.devpath.empty()) {
      m_mapped_info_cache[pid] = c;
      if (c.devpath != cfg->devpath) {
        return -ENOENT;
      }
    } else {
      c.devpath = cfg->devpath;
    }

    c.cookie = get_cookie(cfg->devpath);
    *cfg = c;
    return 0;
  }

  int find_attached(const std::string &devpath) {
    for (auto &entry : fs::directory_iterator("/proc")) {
      if (!fs::is_directory(entry.status())) {
        continue;
      }

      int pid;
      try {
        pid = boost::lexical_cast<uint64_t>(entry.path().filename().c_str());
      } catch (boost::bad_lexical_cast&) {
        continue;
      }

      Config cfg;
      cfg.devpath = devpath;
      if (get_mapped_info(pid, &cfg) >=0 && cfg.command == Attach) {
        return cfg.pid;
      }
    }

    return -1;
  }
};

struct EncryptionOptions {
  std::vector<librbd::encryption_spec_t> specs;

  ~EncryptionOptions() {
    for (auto& spec : specs) {
      switch (spec.format) {
      case RBD_ENCRYPTION_FORMAT_LUKS: {
        auto opts =
            static_cast<librbd::encryption_luks_format_options_t*>(spec.opts);
        ceph_memzero_s(opts->passphrase.data(), opts->passphrase.size(),
                       opts->passphrase.size());
        delete opts;
        break;
      }
      case RBD_ENCRYPTION_FORMAT_LUKS1: {
        auto opts =
            static_cast<librbd::encryption_luks1_format_options_t*>(spec.opts);
        ceph_memzero_s(opts->passphrase.data(), opts->passphrase.size(),
                       opts->passphrase.size());
        delete opts;
        break;
      }
      case RBD_ENCRYPTION_FORMAT_LUKS2: {
        auto opts =
            static_cast<librbd::encryption_luks2_format_options_t*>(spec.opts);
        ceph_memzero_s(opts->passphrase.data(), opts->passphrase.size(),
                       opts->passphrase.size());
        delete opts;
        break;
      }
      default:
        ceph_abort();
      }
    }
  }
};

static std::string get_cookie(const std::string &devpath)
{
  std::string cookie;
  std::ifstream ifs;
  std::string path = "/sys/block/" + devpath.substr(sizeof("/dev/") - 1) + "/backend";

  ifs.open(path, std::ifstream::in);
  if (ifs.is_open()) {
    std::getline(ifs, cookie);
    ifs.close();
  }
  return cookie;
}

static int load_module(Config *cfg)
{
  ostringstream param;
  int ret;

  if (cfg->nbds_max)
    param << "nbds_max=" << cfg->nbds_max;

  if (cfg->max_part)
    param << " max_part=" << cfg->max_part;

  if (!access("/sys/module/nbd", F_OK)) {
    if (cfg->nbds_max || cfg->set_max_part)
      cerr << "rbd-nbd: ignoring kernel module parameter options: nbd module already loaded"
           << std::endl;
    return 0;
  }

  ret = module_load("nbd", param.str().c_str());
  if (ret < 0)
    cerr << "rbd-nbd: failed to load nbd kernel module: " << cpp_strerror(-ret)
         << std::endl;

  return ret;
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

static int parse_nbd_index(const std::string& devpath)
{
  int index, ret;

  ret = sscanf(devpath.c_str(), "/dev/nbd%d", &index);
  if (ret <= 0) {
    // mean an early matching failure. But some cases need a negative value.
    if (ret == 0)
      ret = -EINVAL;
    cerr << "rbd-nbd: invalid device path: " <<  devpath
         << " (expected /dev/nbd{num})" << std::endl;
    return ret;
  }

  return index;
}

static int try_ioctl_setup(Config *cfg, int fd, uint64_t size,
                           uint64_t blksize, uint64_t flags)
{
  int index = 0, r;

  if (cfg->devpath.empty()) {
    char dev[64];
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

      nbd = open(dev, O_RDWR);
      if (nbd < 0) {
        if (nbd == -EPERM && nbds_max != -1 && index < (nbds_max-1)) {
          ++index;
          continue;
        }
        r = nbd;
        cerr << "rbd-nbd: failed to find unused device" << std::endl;
        goto done;
      }

      r = ioctl(nbd, NBD_SET_SOCK, fd);
      if (r < 0) {
        close(nbd);
        ++index;
        continue;
      }

      cfg->devpath = dev;
      break;
    }
  } else {
    r = parse_nbd_index(cfg->devpath);
    if (r < 0)
      goto done;
    index = r;

    nbd = open(cfg->devpath.c_str(), O_RDWR);
    if (nbd < 0) {
      r = nbd;
      cerr << "rbd-nbd: failed to open device: " << cfg->devpath << std::endl;
      goto done;
    }

    r = ioctl(nbd, NBD_SET_SOCK, fd);
    if (r < 0) {
      r = -errno;
      cerr << "rbd-nbd: the device " << cfg->devpath << " is busy" << std::endl;
      close(nbd);
      goto done;
    }
  }

  r = ioctl(nbd, NBD_SET_BLKSIZE, blksize);
  if (r < 0) {
    r = -errno;
    cerr << "rbd-nbd: NBD_SET_BLKSIZE failed" << std::endl;
    goto close_nbd;
  }

  r = ioctl(nbd, NBD_SET_SIZE, size);
  if (r < 0) {
    cerr << "rbd-nbd: NBD_SET_SIZE failed" << std::endl;
    r = -errno;
    goto close_nbd;
  }

  ioctl(nbd, NBD_SET_FLAGS, flags);

  if (cfg->io_timeout >= 0) {
    r = ioctl(nbd, NBD_SET_TIMEOUT, (unsigned long)cfg->io_timeout);
    if (r < 0) {
      r = -errno;
      cerr << "rbd-nbd: failed to set IO timeout: " << cpp_strerror(r)
           << std::endl;
      goto close_nbd;
    }
  }

  dout(10) << "ioctl setup complete for " << cfg->devpath << dendl;
  nbd_index = index;
  return 0;

close_nbd:
  if (r < 0) {
    ioctl(nbd, NBD_CLEAR_SOCK);
    cerr << "rbd-nbd: failed to map, status: " << cpp_strerror(-r) << std::endl;
  }
  close(nbd);
done:
  return r;
}

static void netlink_cleanup(struct nl_sock *sock)
{
  if (!sock)
    return;

  nl_close(sock);
  nl_socket_free(sock);
}

static struct nl_sock *netlink_init(int *id)
{
  struct nl_sock *sock;
  int ret;

  sock = nl_socket_alloc();
  if (!sock) {
    cerr << "rbd-nbd: Could not allocate netlink socket." << std::endl;
    return NULL;
  }

  ret = genl_connect(sock);
  if (ret < 0) {
    cerr << "rbd-nbd: Could not connect netlink socket. Error " << ret
         << std::endl;
    goto free_sock;
  }

  *id = genl_ctrl_resolve(sock, "nbd");
  if (*id < 0)
    //  nbd netlink interface not supported.
    goto close_sock;

  return sock;

close_sock:
  nl_close(sock);
free_sock:
  nl_socket_free(sock);
  return NULL;
}

static int netlink_disconnect(int index)
{
  struct nl_sock *sock;
  struct nl_msg *msg;
  int ret, nl_id;

  sock = netlink_init(&nl_id);
  if (!sock)
    // Try ioctl
    return 1;

  nl_socket_modify_cb(sock, NL_CB_VALID, NL_CB_CUSTOM, genl_handle_msg, NULL);

  msg = nlmsg_alloc();
  if (!msg) {
    cerr << "rbd-nbd: Could not allocate netlink message." << std::endl;
    goto free_sock;
  }

  if (!genlmsg_put(msg, NL_AUTO_PORT, NL_AUTO_SEQ, nl_id, 0, 0,
                   NBD_CMD_DISCONNECT, 0)) {
    cerr << "rbd-nbd: Could not setup message." << std::endl;
    goto nla_put_failure;
  }

  NLA_PUT_U32(msg, NBD_ATTR_INDEX, index);

  ret = nl_send_sync(sock, msg);
  netlink_cleanup(sock);
  if (ret < 0) {
    cerr << "rbd-nbd: netlink disconnect failed: " << nl_geterror(-ret)
         << std::endl;
    return -EIO;
  }

  return 0;

nla_put_failure:
  nlmsg_free(msg);
free_sock:
  netlink_cleanup(sock);
  return -EIO;
}

static int netlink_disconnect_by_path(const std::string& devpath)
{
  int index;

  index = parse_nbd_index(devpath);
  if (index < 0)
    return index;

  return netlink_disconnect(index);
}

static int netlink_resize(int nbd_index, const std::string& cookie,
                          uint64_t size)
{
  struct nl_sock *sock;
  struct nl_msg *msg;
  int nl_id, ret;

  sock = netlink_init(&nl_id);
  if (!sock) {
    derr << __func__ << ": netlink interface not supported" << dendl;
    return -EINVAL;
  }

  nl_socket_modify_cb(sock, NL_CB_VALID, NL_CB_CUSTOM, genl_handle_msg, NULL);

  msg = nlmsg_alloc();
  if (!msg) {
    derr << __func__ << ": could not allocate netlink message" << dendl;
    goto free_sock;
  }

  if (!genlmsg_put(msg, NL_AUTO_PORT, NL_AUTO_SEQ, nl_id, 0, 0,
                   NBD_CMD_RECONFIGURE, 0)) {
    derr << __func__ << ": could not setup netlink message" << dendl;
    goto free_msg;
  }

  NLA_PUT_U32(msg, NBD_ATTR_INDEX, nbd_index);
  NLA_PUT_U64(msg, NBD_ATTR_SIZE_BYTES, size);
  if (!cookie.empty())
    NLA_PUT_STRING(msg, NBD_ATTR_BACKEND_IDENTIFIER, cookie.c_str());

  ret = nl_send_sync(sock, msg);
  if (ret < 0) {
    derr << __func__ << ": netlink resize failed: " << nl_geterror(ret)
         << dendl;
    goto free_sock;
  }

  netlink_cleanup(sock);
  dout(10) << "netlink resize complete for nbd" << nbd_index << dendl;
  return 0;

nla_put_failure:
free_msg:
  nlmsg_free(msg);
free_sock:
  netlink_cleanup(sock);
  return -EIO;
}

static int netlink_connect_cb(struct nl_msg *msg, void *arg)
{
  struct genlmsghdr *gnlh = (struct genlmsghdr *)nlmsg_data(nlmsg_hdr(msg));
  Config *cfg = (Config *)arg;
  struct nlattr *msg_attr[NBD_ATTR_MAX + 1];
  uint32_t index;
  int ret;

  ret = nla_parse(msg_attr, NBD_ATTR_MAX, genlmsg_attrdata(gnlh, 0),
                  genlmsg_attrlen(gnlh, 0), NULL);
  if (ret) {
    cerr << "rbd-nbd: Unsupported netlink reply" << std::endl;
    return -NLE_MSGTYPE_NOSUPPORT;
  }

  if (!msg_attr[NBD_ATTR_INDEX]) {
    cerr << "rbd-nbd: netlink connect reply missing device index." << std::endl;
    return -NLE_MSGTYPE_NOSUPPORT;
  }

  index = nla_get_u32(msg_attr[NBD_ATTR_INDEX]);
  cfg->devpath = "/dev/nbd" + stringify(index);
  nbd_index = index;

  return NL_OK;
}

static int netlink_connect(Config *cfg, struct nl_sock *sock, int nl_id, int fd,
                           uint64_t size, uint64_t flags, bool reconnect)
{
  struct nlattr *sock_attr;
  struct nlattr *sock_opt;
  struct nl_msg *msg;
  int ret;

  if (reconnect) {
    dout(10) << "netlink try reconnect for " << cfg->devpath << dendl;

    nl_socket_modify_cb(sock, NL_CB_VALID, NL_CB_CUSTOM, genl_handle_msg, NULL);
  } else {
    nl_socket_modify_cb(sock, NL_CB_VALID, NL_CB_CUSTOM, netlink_connect_cb,
                        cfg);
  }

  msg = nlmsg_alloc();
  if (!msg) {
    cerr << "rbd-nbd: Could not allocate netlink message." << std::endl;
    return -ENOMEM;
  }

  if (!genlmsg_put(msg, NL_AUTO_PORT, NL_AUTO_SEQ, nl_id, 0, 0,
                   reconnect ? NBD_CMD_RECONFIGURE : NBD_CMD_CONNECT, 0)) {
    cerr << "rbd-nbd: Could not setup message." << std::endl;
    goto free_msg;
  }

  if (!cfg->devpath.empty()) {
    ret = parse_nbd_index(cfg->devpath);
    if (ret < 0)
      goto free_msg;

    NLA_PUT_U32(msg, NBD_ATTR_INDEX, ret);
    if (reconnect) {
      nbd_index = ret;
    }
  }

  if (cfg->io_timeout >= 0)
    NLA_PUT_U64(msg, NBD_ATTR_TIMEOUT, cfg->io_timeout);

  NLA_PUT_U64(msg, NBD_ATTR_SIZE_BYTES, size);
  NLA_PUT_U64(msg, NBD_ATTR_BLOCK_SIZE_BYTES, RBD_NBD_BLKSIZE);
  NLA_PUT_U64(msg, NBD_ATTR_SERVER_FLAGS, flags);
  NLA_PUT_U64(msg, NBD_ATTR_DEAD_CONN_TIMEOUT, cfg->reattach_timeout);
  if (!cfg->cookie.empty())
    NLA_PUT_STRING(msg, NBD_ATTR_BACKEND_IDENTIFIER, cfg->cookie.c_str());

  sock_attr = nla_nest_start(msg, NBD_ATTR_SOCKETS);
  if (!sock_attr) {
    cerr << "rbd-nbd: Could not init sockets in netlink message." << std::endl;
    goto free_msg;
  }

  sock_opt = nla_nest_start(msg, NBD_SOCK_ITEM);
  if (!sock_opt) {
    cerr << "rbd-nbd: Could not init sock in netlink message." << std::endl;
    goto free_msg;
  }

  NLA_PUT_U32(msg, NBD_SOCK_FD, fd);
  nla_nest_end(msg, sock_opt);
  nla_nest_end(msg, sock_attr);

  ret = nl_send_sync(sock, msg);
  if (ret < 0) {
    cerr << "rbd-nbd: netlink connect failed: " << nl_geterror(ret)
         << std::endl;
    return -EIO;
  }

  dout(10) << "netlink connect complete for " << cfg->devpath << dendl;
  return 0;

nla_put_failure:
free_msg:
  nlmsg_free(msg);
  return -EIO;
}

static int try_netlink_setup(Config *cfg, int fd, uint64_t size, uint64_t flags,
                             bool reconnect)
{
  struct nl_sock *sock;
  int nl_id, ret;

  sock = netlink_init(&nl_id);
  if (!sock) {
    cerr << "rbd-nbd: Netlink interface not supported. Using ioctl interface."
         << std::endl;
    return 1;
  }

  dout(10) << "netlink interface supported." << dendl;

  ret = netlink_connect(cfg, sock, nl_id, fd, size, flags, reconnect);
  netlink_cleanup(sock);

  if (ret != 0)
    return ret;

  nbd = open(cfg->devpath.c_str(), O_RDWR);
  if (nbd < 0) {
    cerr << "rbd-nbd: failed to open device: " << cfg->devpath << std::endl;
    return nbd;
  }

  return 0;
}

static int run_quiesce_hook(const std::string &quiesce_hook,
                            const std::string &devpath,
                            const std::string &command) {
  dout(10) << __func__ << ": " << quiesce_hook << " " << devpath << " "
           << command << dendl;

  SubProcess hook(quiesce_hook.c_str(), SubProcess::CLOSE, SubProcess::PIPE,
                  SubProcess::PIPE);
  hook.add_cmd_args(devpath.c_str(), command.c_str(), NULL);
  bufferlist err;
  int r = hook.spawn();
  if (r < 0) {
    err.append("subprocess spawn failed");
  } else {
    err.read_fd(hook.get_stderr(), 16384);
    r = hook.join();
    if (r > 0) {
      r = -r;
    }
  }
  if (r < 0) {
    derr << __func__ << ": " << quiesce_hook << " " << devpath << " "
         << command << " failed: " << err.to_str() << dendl;
  } else {
    dout(10) << " succeeded: " << err.to_str() << dendl;
  }

  return r;
}

static void handle_signal(int signum)
{
  ceph_assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got signal " << sig_str(signum) << " ***" << dendl;

  dout(20) << __func__ << ": " << "notifying terminate" << dendl;

  ceph_assert(terminate_event_sock.is_valid());
  terminate_event_sock.notify();
}

static NBDServer *start_server(int fd, librbd::Image& image, Config *cfg)
{
  NBDServer *server;

  server = new NBDServer(fd, image, cfg);
  server->start();

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  return server;
}

static void run_server(Preforker& forker, NBDServer *server, bool netlink_used)
{
  if (g_conf()->daemonize) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }

  if (netlink_used)
    server->wait_for_disconnect();
  else
    ioctl(nbd, NBD_DO_IT);

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
}

// Eventually it should be removed when pidfd_open is widely supported.

static int wait_for_terminate_legacy(int pid, int timeout)
{
  for (int i = 0; ; i++) {
    if (kill(pid, 0) == -1) {
      if (errno == ESRCH) {
        return 0;
      }
      int r = -errno;
      cerr << "rbd-nbd: kill(" << pid << ", 0) failed: "
           << cpp_strerror(r) << std::endl;
      return r;
    }
    if (i >= timeout * 2) {
      break;
    }
    usleep(500000);
  }

  cerr << "rbd-nbd: waiting for process exit timed out" << std::endl;
  return -ETIMEDOUT;
}

// Eventually it should be replaced with glibc' pidfd_open
// when it is widely available.

#ifdef __NR_pidfd_open
static int pidfd_open(pid_t pid, unsigned int flags)
{
  return syscall(__NR_pidfd_open, pid, flags);
}
#else
static int pidfd_open(pid_t pid, unsigned int flags)
{
  errno = ENOSYS;
  return -1;
}
#endif

static int wait_for_terminate(int pid, int timeout)
{
  int fd = pidfd_open(pid, 0);
  if (fd == -1) {
    if (errno == ENOSYS) {
      return wait_for_terminate_legacy(pid, timeout);
    }
    if (errno == ESRCH) {
      return 0;
    }
    int r = -errno;
    cerr << "rbd-nbd: pidfd_open(" << pid << ") failed: "
         << cpp_strerror(r) << std::endl;
    return r;
  }

  struct pollfd poll_fds[1];
  memset(poll_fds, 0, sizeof(struct pollfd));
  poll_fds[0].fd = fd;
  poll_fds[0].events = POLLIN;

  int r = poll(poll_fds, 1, timeout * 1000);
  if (r == -1) {
    r = -errno;
    cerr << "rbd-nbd: failed to poll rbd-nbd process: " << cpp_strerror(r)
         << std::endl;
    goto done;
  } else {
    r = 0;
  }

  if ((poll_fds[0].revents & POLLIN) == 0) {
    cerr << "rbd-nbd: waiting for process exit timed out" << std::endl;
    r = -ETIMEDOUT;
  }

done:
  close(fd);

  return r;
}

static int do_map(int argc, const char *argv[], Config *cfg, bool reconnect)
{
  int r;

  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx io_ctx;
  librbd::Image image;

  int read_only = 0;
  unsigned long flags;
  unsigned long size;
  unsigned long blksize = RBD_NBD_BLKSIZE;
  bool use_netlink = true;

  int fd[2];

  librbd::image_info_t info;

  Preforker forker;
  NBDServer *server;

  auto args = argv_to_vec(argc, argv);
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

  if (cfg->snapid != CEPH_NOSNAP) {
    r = image.snap_set_by_id(cfg->snapid);
    if (r < 0) {
      cerr << "rbd-nbd: failed to set snap id: " << cpp_strerror(r)
           << std::endl;
      goto close_fd;
    }
  } else if (!cfg->snapname.empty()) {
    r = image.snap_set(cfg->snapname.c_str());
    if (r < 0) {
      cerr << "rbd-nbd: failed to set snap name: " << cpp_strerror(r)
           << std::endl;
      goto close_fd;
    }
  }

  if (!cfg->encryption_formats.empty()) {
    EncryptionOptions encryption_options;
    encryption_options.specs.reserve(cfg->encryption_formats.size());

    for (size_t i = 0; i < cfg->encryption_formats.size(); ++i) {
      std::ifstream file(cfg->encryption_passphrase_files[i],
                         std::ios::in | std::ios::binary);
      if (file.fail()) {
        r = -errno;
        std::cerr << "rbd-nbd: unable to open passphrase file '"
                  << cfg->encryption_passphrase_files[i] << "': "
                  << cpp_strerror(r) << std::endl;
        goto close_fd;
      }
      std::string passphrase((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
      file.close();

      switch (cfg->encryption_formats[i]) {
      case RBD_ENCRYPTION_FORMAT_LUKS: {
        auto opts = new librbd::encryption_luks_format_options_t{
            std::move(passphrase)};
        encryption_options.specs.push_back(
            {RBD_ENCRYPTION_FORMAT_LUKS, opts, sizeof(*opts)});
        break;
      }
      case RBD_ENCRYPTION_FORMAT_LUKS1: {
        auto opts = new librbd::encryption_luks1_format_options_t{
            .passphrase = std::move(passphrase)};
        encryption_options.specs.push_back(
            {RBD_ENCRYPTION_FORMAT_LUKS1, opts, sizeof(*opts)});
        break;
      }
      case RBD_ENCRYPTION_FORMAT_LUKS2: {
        auto opts = new librbd::encryption_luks2_format_options_t{
            .passphrase = std::move(passphrase)};
        encryption_options.specs.push_back(
            {RBD_ENCRYPTION_FORMAT_LUKS2, opts, sizeof(*opts)});
        break;
      }
      default:
        ceph_abort();
      }
    }

    r = image.encryption_load2(encryption_options.specs.data(),
                               encryption_options.specs.size());
    if (r != 0) {
      cerr << "rbd-nbd: failed to load encryption: " << cpp_strerror(r)
           << std::endl;
      goto close_fd;
    }

    // luks2 block size can vary upto 4096, while luks1 always uses 512
    // currently we don't have an rbd API for querying the loaded encryption
    blksize = 4096;
  }

  r = image.stat(info, sizeof(info));
  if (r < 0)
    goto close_fd;

  flags = NBD_FLAG_SEND_FLUSH | NBD_FLAG_HAS_FLAGS;
  if (!cfg->notrim) {
    flags |= NBD_FLAG_SEND_TRIM;
  }
  if (!cfg->snapname.empty() || cfg->readonly) {
    flags |= NBD_FLAG_READ_ONLY;
    read_only = 1;
  }

  if (info.size > ULONG_MAX) {
    r = -EFBIG;
    cerr << "rbd-nbd: image is too large (" << byte_u_t(info.size)
         << ", max is " << byte_u_t(ULONG_MAX) << ")" << std::endl;
    goto close_fd;
  }

  size = info.size;

  r = load_module(cfg);
  if (r < 0)
    goto close_fd;

  server = start_server(fd[1], image, cfg);

  // generate when the cookie is not supplied at CLI
  if (!reconnect && cfg->cookie.empty()) {
    uuid_d uuid_gen;
    uuid_gen.generate_random();
    cfg->cookie = uuid_gen.to_string();
  }
  r = try_netlink_setup(cfg, fd[0], size, flags, reconnect);
  if (r < 0) {
    goto free_server;
  } else if (r == 1) {
    use_netlink = false;
  }

  if (!use_netlink) {
    r = try_ioctl_setup(cfg, fd[0], size, blksize, flags);
    if (r < 0)
      goto free_server;
  }

  r = check_device_size(nbd_index, size);
  if (r < 0)
    goto close_nbd;

  r = ioctl(nbd, BLKROSET, (unsigned long) &read_only);
  if (r < 0) {
    r = -errno;
    goto close_nbd;
  }

  {
    NBDQuiesceWatchCtx quiesce_watch_ctx(server);
    if (cfg->quiesce) {
      r = image.quiesce_watch(&quiesce_watch_ctx,
                              &server->quiesce_watch_handle);
      if (r < 0) {
        goto close_nbd;
      }
    }

    uint64_t handle;

    NBDWatchCtx watch_ctx(nbd, nbd_index, use_netlink, io_ctx, image,
                          info.size, cfg->cookie);
    r = image.update_watch(&watch_ctx, &handle);
    if (r < 0)
      goto close_nbd;

    std::string cookie;
    if (use_netlink) {
      cookie = get_cookie(cfg->devpath);
      ceph_assert(cookie == cfg->cookie || cookie.empty());
    }
    if (cfg->show_cookie && !cookie.empty()) {
      cout << cfg->devpath << " " << cookie << std::endl;
    } else {
      cout << cfg->devpath << std::endl;
    }

    run_server(forker, server, use_netlink);

    if (cfg->quiesce) {
      r = image.quiesce_unwatch(server->quiesce_watch_handle);
      ceph_assert(r == 0);
    }

    r = image.update_unwatch(handle);
    ceph_assert(r == 0);
  }

close_nbd:
  if (r < 0) {
    if (use_netlink) {
      netlink_disconnect(nbd_index);
    } else {
      ioctl(nbd, NBD_CLEAR_SOCK);
      cerr << "rbd-nbd: failed to map, status: " << cpp_strerror(-r)
	   << std::endl;
    }
  }
  close(nbd);
free_server:
  delete server;
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

static int do_detach(Config *cfg)
{
  int r = kill(cfg->pid, SIGTERM);
  if (r == -1) {
    r = -errno;
    cerr << "rbd-nbd: failed to terminate " << cfg->pid << ": "
         << cpp_strerror(r) << std::endl;
    return r;
  }

  return wait_for_terminate(cfg->pid, cfg->reattach_timeout);
}

static int do_unmap(Config *cfg)
{
  /*
   * The netlink disconnect call supports devices setup with netlink or ioctl,
   * so we always try that first.
   */
  int r = netlink_disconnect_by_path(cfg->devpath);
  if (r < 0) {
    return r;
  }

  if (r == 1) {
    int nbd = open(cfg->devpath.c_str(), O_RDWR);
    if (nbd < 0) {
      cerr << "rbd-nbd: failed to open device: " << cfg->devpath << std::endl;
      return nbd;
    }

    r = ioctl(nbd, NBD_DISCONNECT);
    if (r < 0) {
      cerr << "rbd-nbd: the device is not used" << std::endl;
    }

    close(nbd);

    if (r < 0) {
      return r;
    }
  }

  if (cfg->pid > 0) {
    r = wait_for_terminate(cfg->pid, cfg->reattach_timeout);
  }

  return 0;
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
    tbl.define_column("cookie", TextTable::LEFT, TextTable::LEFT);
  }

  Config cfg;
  NBDListIterator it;
  while (it.get(&cfg)) {
    std::string snap = (cfg.snapid != CEPH_NOSNAP ?
        "@" + std::to_string(cfg.snapid) : cfg.snapname);
    if (f) {
      f->open_object_section("device");
      f->dump_int("id", cfg.pid);
      f->dump_string("pool", cfg.poolname);
      f->dump_string("namespace", cfg.nsname);
      f->dump_string("image", cfg.imgname);
      f->dump_string("snap", snap);
      f->dump_string("device", cfg.devpath);
      f->dump_string("cookie", cfg.cookie);
      f->close_section();
    } else {
      should_print = true;
      tbl << cfg.pid << cfg.poolname << cfg.nsname << cfg.imgname
          << (snap.empty() ? "-" : snap) << cfg.devpath << cfg.cookie
	  << TextTable::endrow;
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

static bool find_mapped_dev_by_spec(Config *cfg, int skip_pid=-1) {
  Config c;
  NBDListIterator it;
  while (it.get(&c)) {
    if (c.pid != skip_pid &&
        c.poolname == cfg->poolname && c.nsname == cfg->nsname &&
        c.imgname == cfg->imgname && c.snapname == cfg->snapname &&
        (cfg->devpath.empty() || c.devpath == cfg->devpath) &&
        c.snapid == cfg->snapid) {
      *cfg = c;
      return true;
    }
  }
  return false;
}

static int find_proc_by_dev(Config *cfg) {
  Config c;
  NBDListIterator it;
  while (it.get(&c)) {
    if (c.devpath == cfg->devpath) {
      *cfg = c;
      return true;
    }
  }
  return false;
}

static int parse_args(vector<const char*>& args, std::ostream *err_msg,
                      Config *cfg) {
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
  std::string arg_value;
  long long snapid;

  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      return HELP_INFO;
    } else if (ceph_argparse_flag(args, i, "-v", "--version", (char*)NULL)) {
      return VERSION_INFO;
    } else if (ceph_argparse_witharg(args, i, &cfg->devpath, "--device", (char *)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &cfg->io_timeout, err,
                                     "--io-timeout", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-nbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->io_timeout < 0) {
        *err_msg << "rbd-nbd: Invalid argument for io-timeout!";
        return -EINVAL;
      }
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
    } else if (ceph_argparse_flag(args, i, "--quiesce", (char *)NULL)) {
      cfg->quiesce = true;
    } else if (ceph_argparse_witharg(args, i, &cfg->quiesce_hook,
                                     "--quiesce-hook", (char *)NULL)) {
    } else if (ceph_argparse_flag(args, i, "--read-only", (char *)NULL)) {
      cfg->readonly = true;
    } else if (ceph_argparse_witharg(args, i, &cfg->reattach_timeout, err,
                                     "--reattach-timeout", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-nbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->reattach_timeout < 0) {
        *err_msg << "rbd-nbd: Invalid argument for reattach-timeout!";
        return -EINVAL;
      }
    } else if (ceph_argparse_flag(args, i, "--exclusive", (char *)NULL)) {
      cfg->exclusive = true;
    } else if (ceph_argparse_flag(args, i, "--notrim", (char *)NULL)) {
      cfg->notrim = true;
    } else if (ceph_argparse_witharg(args, i, &cfg->io_timeout, err,
                                     "--timeout", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-nbd: " << err.str();
        return -EINVAL;
      }
      if (cfg->io_timeout < 0) {
        *err_msg << "rbd-nbd: Invalid argument for timeout!";
        return -EINVAL;
      }
      *err_msg << "rbd-nbd: --timeout is deprecated (use --io-timeout)";
    } else if (ceph_argparse_witharg(args, i, &cfg->format, err, "--format",
                                     (char *)NULL)) {
    } else if (ceph_argparse_flag(args, i, "--pretty-format", (char *)NULL)) {
      cfg->pretty_format = true;
    } else if (ceph_argparse_flag(args, i, "--try-netlink", (char *)NULL)) {
      // netlink used by default. option not required anymore.
      // accept for compatibility.
    } else if (ceph_argparse_flag(args, i, "--show-cookie", (char *)NULL)) {
      cfg->show_cookie = true;
    } else if (ceph_argparse_witharg(args, i, &cfg->cookie, "--cookie", (char *)NULL)) {
    } else if (ceph_argparse_witharg(args, i, &snapid, err,
                                     "--snap-id", (char *)NULL)) {
      if (!err.str().empty()) {
        *err_msg << "rbd-nbd: " << err.str();
        return -EINVAL;
      }
      if (snapid < 0) {
        *err_msg << "rbd-nbd: Invalid argument for snap-id!";
        return -EINVAL;
      }
      cfg->snapid = snapid;
    } else if (ceph_argparse_witharg(args, i, &arg_value,
                                     "--encryption-format", (char *)NULL)) {
      if (arg_value == "luks1") {
        cfg->encryption_formats.push_back(RBD_ENCRYPTION_FORMAT_LUKS1);
      } else if (arg_value == "luks2") {
        cfg->encryption_formats.push_back(RBD_ENCRYPTION_FORMAT_LUKS2);
      } else if (arg_value == "luks") {
        cfg->encryption_formats.push_back(RBD_ENCRYPTION_FORMAT_LUKS);
      } else {
        *err_msg << "rbd-nbd: Invalid encryption format";
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &arg_value,
                                     "--encryption-passphrase-file",
                                     (char *)NULL)) {
      cfg->encryption_passphrase_files.push_back(arg_value);
    } else {
      ++i;
    }
  }

  if (cfg->encryption_formats.empty() &&
      !cfg->encryption_passphrase_files.empty()) {
    cfg->encryption_formats.resize(cfg->encryption_passphrase_files.size(),
                                   RBD_ENCRYPTION_FORMAT_LUKS);
  }

  if (cfg->encryption_formats.size() != cfg->encryption_passphrase_files.size()) {
    *err_msg << "rbd-nbd: Encryption formats count does not match "
             << "passphrase files count";
    return -EINVAL;
  }

  Command cmd = None;
  if (args.begin() != args.end()) {
    if (strcmp(*args.begin(), "map") == 0) {
      cmd = Map;
    } else if (strcmp(*args.begin(), "unmap") == 0) {
      cmd = Unmap;
    } else if (strcmp(*args.begin(), "attach") == 0) {
      cmd = Attach;
    } else if (strcmp(*args.begin(), "detach") == 0) {
      cmd = Detach;
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

  std::string cookie;
  switch (cmd) {
    case Attach:
      if (cfg->devpath.empty()) {
        *err_msg << "rbd-nbd: must specify device to attach";
        return -EINVAL;
      }
      // Allowing attach without --cookie option for kernel without
      // NBD_ATTR_BACKEND_IDENTIFIER support for compatibility
      cookie = get_cookie(cfg->devpath);
      if (!cookie.empty()) {
        if (cfg->cookie.empty()) {
          *err_msg << "rbd-nbd: must specify cookie to attach";
          return -EINVAL;
	} else if (cookie != cfg->cookie) {
          *err_msg << "rbd-nbd: cookie mismatch";
          return -EINVAL;
        }
      } else if (!cfg->cookie.empty()) {
        *err_msg << "rbd-nbd: kernel does not have cookie support";
        return -EINVAL;
      }
      [[fallthrough]];
    case Map:
      if (args.begin() == args.end()) {
        *err_msg << "rbd-nbd: must specify image-or-snap-spec";
        return -EINVAL;
      }
      if (parse_imgpath(*args.begin(), cfg, err_msg) < 0) {
        return -EINVAL;
      }
      args.erase(args.begin());
      break;
    case Detach:
    case Unmap:
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
      }
      args.erase(args.begin());
      break;
    default:
      //shut up gcc;
      break;
  }

  if (cfg->snapid != CEPH_NOSNAP && !cfg->snapname.empty()) {
    *err_msg << "rbd-nbd: use either snapname or snapid, not both";
    return -EINVAL;
  }

  if (args.begin() != args.end()) {
    *err_msg << "rbd-nbd: unknown args: " << *args.begin();
    return -EINVAL;
  }

  cfg->command = cmd;
  return 0;
}

static int rbd_nbd(int argc, const char *argv[])
{
  int r;
  Config cfg;
  auto args = argv_to_vec(argc, argv);
  std::ostringstream err_msg;
  r = parse_args(args, &err_msg, &cfg);
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

  if (!err_msg.str().empty()) {
    cerr << err_msg.str() << std::endl;
  }

  switch (cfg.command) {
    case Attach:
      ceph_assert(!cfg.devpath.empty());
      if (find_mapped_dev_by_spec(&cfg, getpid())) {
        cerr << "rbd-nbd: " << cfg.devpath << " has process " << cfg.pid
             << " connected" << std::endl;
        return -EBUSY;
      }
      [[fallthrough]];
    case Map:
      if (cfg.imgname.empty()) {
        cerr << "rbd-nbd: image name was not specified" << std::endl;
        return -EINVAL;
      }

      r = do_map(argc, argv, &cfg, cfg.command == Attach);
      if (r < 0)
        return -EINVAL;
      break;
    case Detach:
      if (cfg.devpath.empty()) {
        if (!find_mapped_dev_by_spec(&cfg)) {
          cerr << "rbd-nbd: " << cfg.image_spec() << " is not mapped"
               << std::endl;
          return -ENOENT;
        }
      } else if (!find_proc_by_dev(&cfg)) {
        cerr << "rbd-nbd: no process attached to " << cfg.devpath << " found"
             << std::endl;
        return -ENOENT;
      }
      r = do_detach(&cfg);
      if (r < 0)
        return -EINVAL;
      break;
    case Unmap:
      if (cfg.devpath.empty()) {
        if (!find_mapped_dev_by_spec(&cfg)) {
          cerr << "rbd-nbd: " << cfg.image_spec() << " is not mapped"
               << std::endl;
          return -ENOENT;
        }
      } else if (!find_proc_by_dev(&cfg)) {
        // still try to send disconnect to the device
      }
      r = do_unmap(&cfg);
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
