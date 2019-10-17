// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/int_types.h"

#include <sys/types.h>

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

#include <iostream>
#include <boost/regex.hpp>

#include "common/Preforker.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

#include "Driver.h"
#include "Server.h"
#include "Watcher.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd-ggate: " << __func__ << ": "

static void usage() {
  std::cout << "Usage: rbd-ggate [options] map <image-or-snap-spec>  Map an image to ggate device\n"
            << "                           unmap <device path>       Unmap ggate device\n"
            << "                           list                      List mapped ggate devices\n"
            << "Options:\n"
            << "  --device <device path>  Specify ggate device path\n"
            << "  --read-only             Map readonly\n"
            << "  --exclusive             Forbid writes by other clients\n"
            << std::endl;
  generic_server_usage();
}

static std::string devpath, poolname("rbd"), imgname, snapname;
static bool readonly = false;
static bool exclusive = false;

static std::unique_ptr<rbd::ggate::Driver> drv;

static void handle_signal(int signum)
{
  derr << "*** Got signal " << sig_str(signum) << " ***" << dendl;

  assert(signum == SIGINT || signum == SIGTERM);
  assert(drv);

  drv->shut_down();
}

static int do_map(int argc, const char *argv[])
{
  int r;

  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx io_ctx;
  librbd::Image image;

  librbd::image_info_t info;
  std::string desc;

  Preforker forker;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_DAEMON,
                         CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  g_ceph_context->_conf->set_val_or_die("pid_file", "");

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

  std::string devname = (devpath.compare(0, 5, "/dev/") == 0) ?
    devpath.substr(5) : devpath;
  std::unique_ptr<rbd::ggate::Watcher> watcher;
  uint64_t handle;

  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    goto done;
  }

  r = rados.connect();
  if (r < 0) {
    goto done;
  }

  r = rados.ioctx_create(poolname.c_str(), io_ctx);
  if (r < 0) {
    goto done;
  }

  r = rbd.open(io_ctx, image, imgname.c_str());
  if (r < 0) {
    goto done;
  }

  if (exclusive) {
    r = image.lock_acquire(RBD_LOCK_MODE_EXCLUSIVE);
    if (r < 0) {
      cerr << "rbd-ggate: failed to acquire exclusive lock: " << cpp_strerror(r)
           << std::endl;
      goto done;
    }
  }

  desc = "RBD " + poolname + "/" + imgname;

  if (!snapname.empty()) {
    r = image.snap_set(snapname.c_str());
    if (r < 0) {
      goto done;
    }
    readonly = true;
    desc += "@" + snapname;
  }

  r = image.stat(info, sizeof(info));
  if (r < 0) {
    goto done;
  }

  rbd::ggate::Driver::load();
  drv.reset(new rbd::ggate::Driver(devname, 512, info.size, readonly, desc));
  r = drv->init();
  if (r < 0) {
    r = -errno;
    goto done;
  }

  watcher.reset(new rbd::ggate::Watcher(drv.get(), io_ctx, image, info.size));
  r = image.update_watch(watcher.get(), &handle);
  if (r < 0) {
    drv->shut_down();
    goto done;
  }

  std::cout << "/dev/" << drv->get_devname() << std::endl;

  if (g_conf->daemonize) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  rbd::ggate::Server(drv.get(), image).run();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();

  r = image.update_unwatch(handle);
  assert(r == 0);

done:
  image.close();
  io_ctx.close();
  rados.shutdown();

  forker.exit(r < 0 ? EXIT_FAILURE : 0);
  // Unreachable;
  return r;
}

static int do_unmap()
{
  std::string devname = (devpath.compare(0, 5, "/dev/") == 0) ?
    devpath.substr(5) : devpath;

  int r = rbd::ggate::Driver::kill(devname);
  if (r < 0) {
    cerr << "rbd-ggate: failed to destroy " << devname << ": "
         << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

static int parse_imgpath(const std::string &imgpath)
{
  boost::regex pattern("^(?:([^/@]+)/)?([^/@]+)(?:@([^/@]+))?$");
  boost::smatch match;
  if (!boost::regex_match(imgpath, match, pattern)) {
    std::cerr << "rbd-ggate: invalid spec '" << imgpath << "'" << std::endl;
    return -EINVAL;
  }

  if (match[1].matched) {
    poolname = match[1];
  }

  imgname = match[2];

  if (match[3].matched) {
    snapname = match[3];
  }

  return 0;
}

static int do_list()
{
  rbd::ggate::Driver::load();

  std::list<std::string> devs;
  int r = rbd::ggate::Driver::list(devs);
  if (r < 0) {
    return -r;
  }

  for (auto &devname : devs) {
    cout << "/dev/" << devname << std::endl;
  }
  return 0;
}

int main(int argc, const char *argv[]) {
  int r;
  enum {
    None,
    Connect,
    Disconnect,
    List
  } cmd = None;

  vector<const char*> args;

  argv_to_vec(argc, argv, args);
  md_config_t().parse_argv(args);

  std::vector<const char*>::iterator i;

  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &devpath, "--device",
                                     (char *)NULL)) {
    } else if (ceph_argparse_flag(args, i, "--read-only", (char *)NULL)) {
      readonly = true;
    } else if (ceph_argparse_flag(args, i, "--exclusive", (char *)NULL)) {
      exclusive = true;
    } else {
      ++i;
    }
  }

  if (args.begin() != args.end()) {
    if (strcmp(*args.begin(), "map") == 0) {
      cmd = Connect;
    } else if (strcmp(*args.begin(), "unmap") == 0) {
      cmd = Disconnect;
    } else if (strcmp(*args.begin(), "list") == 0) {
      cmd = List;
    } else {
      cerr << "rbd-ggate: unknown command: " << *args.begin() << std::endl;
      return EXIT_FAILURE;
    }
    args.erase(args.begin());
  }

  if (cmd == None) {
    cerr << "rbd-ggate: must specify command" << std::endl;
    return EXIT_FAILURE;
  }

  switch (cmd) {
    case Connect:
      if (args.begin() == args.end()) {
        cerr << "rbd-ggate: must specify image-or-snap-spec" << std::endl;
        return EXIT_FAILURE;
      }
      if (parse_imgpath(string(*args.begin())) < 0)
        return EXIT_FAILURE;
      args.erase(args.begin());
      break;
    case Disconnect:
      if (args.begin() == args.end()) {
        cerr << "rbd-ggate: must specify ggate device path" << std::endl;
        return EXIT_FAILURE;
      }
      devpath = *args.begin();
      args.erase(args.begin());
      break;
    default:
      break;
  }

  if (args.begin() != args.end()) {
    cerr << "rbd-ggate: unknown args: " << *args.begin() << std::endl;
    return EXIT_FAILURE;
  }

  switch (cmd) {
    case Connect:
      if (imgname.empty()) {
        cerr << "rbd-ggate: image name was not specified" << std::endl;
        return EXIT_FAILURE;
      }

      r = do_map(argc, argv);
      if (r < 0)
        return EXIT_FAILURE;
      break;
    case Disconnect:
      r = do_unmap();
      if (r < 0)
        return EXIT_FAILURE;
      break;
    case List:
      r = do_list();
      if (r < 0)
        return EXIT_FAILURE;
      break;
    default:
      usage();
      return EXIT_FAILURE;
  }

  return 0;
}
