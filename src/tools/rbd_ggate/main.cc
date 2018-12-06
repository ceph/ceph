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
#include <memory>
#include <boost/algorithm/string/predicate.hpp>
#include <regex>

#include "common/Formatter.h"
#include "common/Preforker.h"
#include "common/TextTable.h"
#include "common/ceph_argparse.h"
#include "common/config_proxy.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "include/stringify.h"

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
            << "\n"
            << "Map options:\n"
            << "  --device <device path>  Specify ggate device path\n"
            << "  --read-only             Map readonly\n"
            << "  --exclusive             Forbid writes by other clients\n"
            << "\n"
            << "List options:\n"
            << "  --format plain|json|xml Output format (default: plain)\n"
            << "  --pretty-format         Pretty formatting (json and xml)\n"
            << std::endl;
  generic_server_usage();
}

static std::string devpath, poolname, nsname, imgname, snapname;
static bool readonly = false;
static bool exclusive = false;

static std::unique_ptr<rbd::ggate::Driver> drv;

static void handle_signal(int signum)
{
  derr << "*** Got signal " << sig_str(signum) << " ***" << dendl;

  ceph_assert(signum == SIGINT || signum == SIGTERM);
  ceph_assert(drv);

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

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_DAEMON,
                         CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  g_ceph_context->_conf.set_val_or_die("pid_file", "");

  if (global_init_prefork(g_ceph_context) >= 0) {
    std::string err;
    r = forker.prefork(err);
    if (r < 0) {
      std::cerr << err << std::endl;
      return r;
    }

    if (forker.is_parent()) {
      global_init_postfork_start(g_ceph_context);
      if (forker.parent_wait(err) != 0) {
        return -ENXIO;
      }
      return 0;
    }
  }

  common_init_finish(g_ceph_context);
  global_init_chdir(g_ceph_context);

  if (poolname.empty()) {
    poolname = g_ceph_context->_conf.get_val<std::string>("rbd_default_pool");
  }

  std::string devname = boost::starts_with(devpath, "/dev/") ?
    devpath.substr(5) : devpath;
  std::unique_ptr<rbd::ggate::Watcher> watcher;
  uint64_t handle;

  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    goto done;
  }

  r = rados.connect();
  if (r < 0) {
    std::cerr << "rbd-ggate: failed to connect to cluster: " << cpp_strerror(r)
              << std::endl;
    goto done;
  }

  r = rados.ioctx_create(poolname.c_str(), io_ctx);
  if (r < 0) {
    std::cerr << "rbd-ggate: failed to acces pool " << poolname << ": "
              << cpp_strerror(r) << std::endl;
    goto done;
  }

  io_ctx.set_namespace(nsname);

  r = rbd.open(io_ctx, image, imgname.c_str());
  if (r < 0) {
    std::cerr << "rbd-ggate: failed to open image " << imgname << ": "
              << cpp_strerror(r) << std::endl;
    goto done;
  }

  if (exclusive) {
    r = image.lock_acquire(RBD_LOCK_MODE_EXCLUSIVE);
    if (r < 0) {
      std::cerr << "rbd-ggate: failed to acquire exclusive lock: "
                << cpp_strerror(r) << std::endl;
      goto done;
    }
  }

  desc = "RBD " + poolname + "/" + (nsname.empty() ? "" : nsname + "/") +
    imgname;

  if (!snapname.empty()) {
    r = image.snap_set(snapname.c_str());
    if (r < 0) {
      std::cerr << "rbd-ggate: failed to set snapshot " << snapname << ": "
                << cpp_strerror(r) << std::endl;
      goto done;
    }
    readonly = true;
    desc += "@" + snapname;
  }

  r = image.stat(info, sizeof(info));
  if (r < 0) {
    std::cerr << "rbd-ggate: image stat failed: " << cpp_strerror(r)
              << std::endl;
    goto done;
  }

  rbd::ggate::Driver::load();
  drv.reset(new rbd::ggate::Driver(devname, 512, info.size, readonly, desc));
  r = drv->init();
  if (r < 0) {
    r = -errno;
    std::cerr << "rbd-ggate: failed to create ggate device: " << cpp_strerror(r)
              << std::endl;
    goto done;
  }

  watcher.reset(new rbd::ggate::Watcher(drv.get(), io_ctx, image, info.size));
  r = image.update_watch(watcher.get(), &handle);
  if (r < 0) {
    std::cerr << "rbd-ggate: failed to set watcher: " << cpp_strerror(r)
              << std::endl;
    drv->shut_down();
    goto done;
  }

  std::cout << "/dev/" << drv->get_devname() << std::endl;

  if (g_conf()->daemonize) {
    forker.daemonize();
    global_init_postfork_start(g_ceph_context);
    global_init_postfork_finish(g_ceph_context);
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
  ceph_assert(r == 0);

done:
  image.close();
  io_ctx.close();
  rados.shutdown();

  if (r < 0) {
    std::cerr << "rbd-ggate: failed to map: " << cpp_strerror(r) << std::endl;
  }

  forker.exit(r < 0 ? EXIT_FAILURE : 0);
  // Unreachable;
  return r;
}

static int do_unmap()
{
  std::string devname = boost::starts_with(devpath, "/dev/") ?
    devpath.substr(5) : devpath;

  int r = rbd::ggate::Driver::kill(devname);
  if (r < 0) {
    cerr << "rbd-ggate: failed to destroy " << devname << ": "
         << cpp_strerror(r) << std::endl;
    return r;
  }

  return 0;
}

static int parse_imgpath(const std::string &imgpath, std::string *poolname,
                         std::string *nsname, std::string *imgname,
                         std::string *snapname) {
  std::regex pattern("^(?:([^/]+)/(?:([^/@]+)/)?)?([^@]+)(?:@([^/@]+))?$");
  std::smatch match;
  if (!std::regex_match(imgpath, match, pattern)) {
    std::cerr << "rbd-ggate: invalid spec '" << imgpath << "'" << std::endl;
    return -EINVAL;
  }

  if (match[1].matched) {
    *poolname = match[1];
  }

  if (match[2].matched) {
    *nsname = match[2];
  }

  *imgname = match[3];

  if (match[4].matched) {
    *snapname = match[4];
  }

  return 0;
}

static bool find_mapped_dev_by_spec(const std::string &spec,
                                    std::string *devname) {
  std::string poolname, nsname, imgname, snapname;
  int r = parse_imgpath(spec, &poolname, &nsname, &imgname, &snapname);
  if (r < 0) {
    return false;
  }
  if (poolname.empty()) {
    // We could use rbd_default_pool config to set pool name but then
    // we would need to initialize the global context. So right now it
    // is mandatory for the user to specify a pool. Fortunately the
    // preferred way for users to call rbd-ggate is via rbd, which
    // cares to set the pool name.
    return false;
  }

  std::map<std::string, rbd::ggate::Driver::DevInfo> devs;
  r = rbd::ggate::Driver::list(&devs);
  if (r < 0) {
    return false;
  }

  for (auto &it : devs) {
    auto &name = it.second.first;
    auto &info = it.second.second;
    if (!boost::starts_with(info, "RBD ")) {
      continue;
    }

    std::string p, n, i, s;
    parse_imgpath(info.substr(4), &p, &n, &i, &s);
    if (p == poolname && n == nsname && i == imgname && s == snapname) {
      *devname = name;
      return true;
    }
  }

  return false;
}

static int do_list(const std::string &format, bool pretty_format)
{
  rbd::ggate::Driver::load();

  std::map<std::string, rbd::ggate::Driver::DevInfo> devs;
  int r = rbd::ggate::Driver::list(&devs);
  if (r < 0) {
    return -r;
  }

  std::unique_ptr<ceph::Formatter> f;
  TextTable tbl;

  if (format == "json") {
    f.reset(new JSONFormatter(pretty_format));
  } else if (format == "xml") {
    f.reset(new XMLFormatter(pretty_format));
  } else if (!format.empty() && format != "plain") {
    std::cerr << "rbd-ggate: invalid output format: " << format << std::endl;
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

  int count = 0;

  for (auto &it : devs) {
    auto &id = it.first;
    auto &name = it.second.first;
    auto &info = it.second.second;
    if (!boost::starts_with(info, "RBD ")) {
      continue;
    }

    std::string poolname;
    std::string nsname;
    std::string imgname;
    std::string snapname(f ? "" : "-");
    parse_imgpath(info.substr(4), &poolname, &nsname, &imgname, &snapname);

    if (f) {
      f->open_object_section("device");
      f->dump_string("id", id);
      f->dump_string("pool", poolname);
      f->dump_string("namespace", nsname);
      f->dump_string("image", imgname);
      f->dump_string("snap", snapname);
      f->dump_string("device", "/dev/" + name);
      f->close_section();
    } else {
      tbl << id << poolname << nsname << imgname << snapname << "/dev/" + name
          << TextTable::endrow;
    }
    count++;
  }

  if (f) {
    f->close_section(); // devices
    f->flush(std::cout);
  } else if (count > 0) {
    std::cout << tbl;
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
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }
  // filter out ceph config options
  ConfigProxy{false}.parse_argv(args);

  std::string format;
  bool pretty_format = false;
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
    } else if (ceph_argparse_witharg(args, i, &format, "--format",
                                     (char *)NULL)) {
    } else if (ceph_argparse_flag(args, i, "--pretty-format", (char *)NULL)) {
      pretty_format = true;
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
      if (parse_imgpath(*args.begin(), &poolname, &nsname, &imgname,
                        &snapname) < 0) {
        return EXIT_FAILURE;
      }
      args.erase(args.begin());
      break;
    case Disconnect:
      if (args.begin() == args.end()) {
        std::cerr << "rbd-ggate: must specify ggate device or image-or-snap-spec"
                  << std::endl;
        return EXIT_FAILURE;
      }
      if (boost::starts_with(*args.begin(), "/dev/") ||
          !find_mapped_dev_by_spec(*args.begin(), &devpath)) {
        devpath = *args.begin();
      }
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
      r = do_list(format, pretty_format);
      if (r < 0)
        return EXIT_FAILURE;
      break;
    default:
      usage();
      return EXIT_FAILURE;
  }

  return 0;
}
