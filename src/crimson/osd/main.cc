// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <sys/types.h>
#include <unistd.h>

#include <iostream>
#include <random>

#include <seastar/core/app-template.hh>
#include <seastar/core/print.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/std-compat.hh>

#include "auth/KeyRing.h"
#include "common/ceph_argparse.h"
#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"
#include "crimson/net/Messenger.h"
#include "global/pidfile.h"

#include "osd.h"

using config_t = crimson::common::ConfigProxy;
namespace fs = seastar::compat::filesystem;

void usage(const char* prog) {
  std::cout << "usage: " << prog << " -i <ID>\n"
            << "  --help-seastar    show Seastar help messages\n";
  generic_server_usage();
}

auto partition_args(seastar::app_template& app, char** argv_begin, char** argv_end)
{
  namespace bpo = boost::program_options;
  // collect all options consumed by seastar::app_template
  auto parsed = bpo::command_line_parser(std::distance(argv_begin, argv_end),
                                         argv_begin)
    .options(app.get_options_description()).allow_unregistered().run();
  auto unknown_args = bpo::collect_unrecognized(parsed.options,
                                                bpo::include_positional);
  std::vector<const char*> ceph_args, app_args;
  // ceph_argparse_early_args() and
  // seastar::smp::get_options_description() use "-c" for different
  // options. and ceph wins
  auto consume_conf_arg = [&](char** argv) {
    if (std::strcmp(*argv, "-c") == 0) {
      ceph_args.push_back(*argv++);
      if (argv != argv_end) {
        ceph_args.push_back(*argv++);
      }
    }
    return argv;
  };
  auto unknown = unknown_args.begin();
  auto consume_unknown_arg = [&](char** argv) {
    for (; unknown != unknown_args.end() &&
           argv != argv_end &&
           *unknown == *argv; ++argv, ++unknown) {
      if (std::strcmp(*argv, "--help-seastar") == 0) {
        app_args.push_back("--help");
      } else {
        ceph_args.push_back(*argv);
      }
    }
    return argv;
  };
  for (auto argv = argv_begin; argv != argv_end;) {
    if (auto next_arg = consume_conf_arg(argv); next_arg != argv) {
      argv = next_arg;
    } else if (auto next_arg = consume_unknown_arg(argv); next_arg != argv) {
      argv = next_arg;
    } else {
      app_args.push_back(*argv++);
    }
  }
  return make_pair(std::move(ceph_args), std::move(app_args));
}

using crimson::common::local_conf;

seastar::future<> make_keyring()
{
  const auto path = local_conf().get_val<string>("keyring");
  return seastar::file_exists(path).then([path](bool exists) {
    KeyRing keyring;
    EntityName name{local_conf()->name};
    EntityAuth auth;
    if (exists &&
        keyring.load(nullptr, path) == 0 &&
        keyring.get_auth(name, auth)) {
      seastar::fprint(std::cerr, "already have key in keyring: %s\n", path);
      return seastar::now();
    } else {
      auth.key.create(std::make_unique<CephContext>().get(), CEPH_CRYPTO_AES);
      keyring.add(name, auth);
      bufferlist bl;
      keyring.encode_plaintext(bl);
      const auto permissions = (seastar::file_permissions::user_read |
                              seastar::file_permissions::user_write);
      return ceph::buffer::write_file(std::move(bl), path, permissions);
    }
  }).handle_exception_type([path](const fs::filesystem_error& e) {
    seastar::fprint(std::cerr, "FATAL: writing new keyring to %s: %s\n", path, e.what());
    throw e;
  });
}

uint64_t get_nonce()
{
  if (auto pid = getpid(); pid != 1) {
    return pid;
  } else {
    // we're running in a container; use a random number instead!
    std::random_device rd;
    std::default_random_engine rng{rd()};
    return std::uniform_int_distribution<uint64_t>{}(rng);
  }
}

int main(int argc, char* argv[])
{
  seastar::app_template app;
  app.add_options()
    ("mkkey", "generate a new secret key. "
              "This is normally used in combination with --mkfs")
    ("mkfs", "create a [new] data directory")
    ("debug", "enable debug output on all loggers");

  auto [ceph_args, app_args] = partition_args(app, argv, argv + argc);
  if (ceph_argparse_need_usage(ceph_args) &&
      std::find(app_args.begin(), app_args.end(), "--help") == app_args.end()) {
    usage(argv[0]);
    return EXIT_SUCCESS;
  }
  std::string cluster_name;
  std::string conf_file_list;
  // ceph_argparse_early_args() could _exit(), while local_conf() won't ready
  // until it's started. so do the boilerplate-settings parsing here.
  auto init_params = ceph_argparse_early_args(ceph_args,
                                              CEPH_ENTITY_TYPE_OSD,
                                              &cluster_name,
                                              &conf_file_list);
  seastar::sharded<crimson::osd::OSD> osd;
  using crimson::common::sharded_conf;
  using crimson::common::sharded_perf_coll;
  try {
    return app.run_deprecated(app_args.size(), const_cast<char**>(app_args.data()),
      [&, &ceph_args=ceph_args] {
      auto& config = app.configuration();
      return seastar::async([&] {
	if (config.count("debug")) {
	    seastar::global_logger_registry().set_all_loggers_level(
	      seastar::log_level::debug
	    );
	}
        sharded_conf().start(init_params.name, cluster_name).get();
        seastar::engine().at_exit([] {
          return sharded_conf().stop();
        });
        sharded_perf_coll().start().get();
        seastar::engine().at_exit([] {
          return sharded_perf_coll().stop();
        });
        local_conf().parse_config_files(conf_file_list).get();
        local_conf().parse_argv(ceph_args).get();
        if (const auto ret = pidfile_write(local_conf()->pid_file);
            ret == -EACCES || ret == -EAGAIN) {
          ceph_abort_msg(
            "likely there is another crimson-osd instance with the same id");
        } else if (ret < 0) {
          ceph_abort_msg(fmt::format("pidfile_write failed with {} {}",
                                     ret, cpp_strerror(-ret)));
        }
        const int whoami = std::stoi(local_conf()->name.get_id());
        const auto nonce = get_nonce();
        crimson::net::MessengerRef cluster_msgr, client_msgr;
        crimson::net::MessengerRef hb_front_msgr, hb_back_msgr;
        for (auto [msgr, name] : {make_pair(std::ref(cluster_msgr), "cluster"s),
                                  make_pair(std::ref(client_msgr), "client"s),
                                  make_pair(std::ref(hb_front_msgr), "hb_front"s),
                                  make_pair(std::ref(hb_back_msgr), "hb_back"s)}) {
          msgr = crimson::net::Messenger::create(entity_name_t::OSD(whoami), name,
                                                 nonce);
          if (local_conf()->ms_crc_data) {
            msgr->set_crc_data();
          }
          if (local_conf()->ms_crc_header) {
            msgr->set_crc_header();
          }
        }
        osd.start_single(whoami, nonce,
                         cluster_msgr, client_msgr,
                         hb_front_msgr, hb_back_msgr).get();
        if (config.count("mkkey")) {
          make_keyring().handle_exception([](std::exception_ptr) {
            seastar::engine().exit(1);
          }).get();
        }
        if (config.count("mkfs")) {
          osd.invoke_on(
	    0,
	    &crimson::osd::OSD::mkfs,
	    local_conf().get_val<uuid_d>("osd_uuid"),
	    local_conf().get_val<uuid_d>("fsid")).get();
        }
        seastar::engine().at_exit([&] {
          return osd.stop();
        });
        if (config.count("mkkey") || config.count("mkfs")) {
          seastar::engine().exit(0);
        } else {
          osd.invoke_on(0, &crimson::osd::OSD::start).get();
        }
      });
    });
  } catch (...) {
    seastar::fprint(std::cerr, "FATAL: Exception during startup, aborting: %s\n", std::current_exception());
    return EXIT_FAILURE;
  }
}

/*
 * Local Variables:
 * compile-command: "make -j4 \
 * -C ../../../build \
 * crimson-osd"
 * End:
 */
