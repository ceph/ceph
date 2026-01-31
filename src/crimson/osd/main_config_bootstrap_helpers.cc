// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/osd/main_config_bootstrap_helpers.h"

#include <seastar/core/print.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/std-compat.hh>
#include <seastar/core/app-template.hh>

#include <boost/program_options.hpp>
#include "common/ceph_argparse.h"
#include "common/config_tracker.h"
#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/fatal_signal.h"
#include "crimson/common/perf_counters_collection.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Messenger.h"
#include "crimson/osd/main_config_bootstrap_helpers.h"

#include <sys/wait.h> // for waitpid()
#include <unistd.h>   // for pipe(), fork(), read(), write()
#include <errno.h>    // for errno

using namespace std::literals;
using crimson::common::local_conf;
using crimson::common::sharded_conf;
using crimson::common::sharded_perf_coll;

static seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}

namespace crimson::osd {

void usage(const char* prog)
{
  std::cout << "crimson osd usage: " << prog << " -i <ID> [flags...]" << std::endl;
  generic_server_usage();
}


seastar::future<> populate_config_from_mon()
{
  logger().info("populating config from monitor");
  // i don't have any client before joining the cluster, so no need to have
  // a proper auth handler
  class DummyAuthHandler : public crimson::common::AuthHandler {
  public:
    void handle_authentication(const EntityName& name,
                               const AuthCapsInfo& caps)
    {}
  };
  return seastar::async([] {
    auto auth_handler = std::make_unique<DummyAuthHandler>();
    auto msgr = crimson::net::Messenger::create(entity_name_t::CLIENT(),
                                                "temp_mon_client",
                                                get_nonce(),
                                                true);
    crimson::mon::Client monc{*msgr, *auth_handler};
    msgr->set_auth_client(&monc);
    msgr->start({&monc}).get();
    auto stop_msgr = seastar::defer([&] {
      msgr->stop();
      msgr->shutdown().get();
    });
    monc.start().handle_exception([] (auto ep) {
      fmt::print(std::cerr, "FATAL: unable to connect to cluster: {}\n", ep);
      return seastar::make_exception_future<>(ep);
    }).get();
    auto stop_monc = seastar::defer([&] {
      monc.stop().get();
    });
    monc.sub_want("config", 0, 0);
    monc.renew_subs().get();
    // wait for monmap and config
    monc.wait_for_config().get();
    auto fsid = monc.get_fsid().to_string();
    local_conf().set_val("fsid", fsid).get();
    logger().debug("{}: got config from monitor, fsid {}", __func__, fsid);
  });
}

struct SeastarOption {
  std::string option_name;  // Command-line option name
  std::string config_key;   // Configuration key
  Option::type_t value_type ;   // Type of configuration value
};

// Define a list of Seastar options
const std::vector<SeastarOption> seastar_options = {
  {"--task-quota-ms", "crimson_reactor_task_quota_ms", Option::TYPE_FLOAT},
  {"--io-latency-goal-ms", "crimson_reactor_io_latency_goal_ms", Option::TYPE_FLOAT},
  {"--idle-poll-time-us", "crimson_reactor_idle_poll_time_us", Option::TYPE_UINT},
  {"--poll-mode", "crimson_poll_mode", Option::TYPE_BOOL}
};

// Function to get the option value as a string
std::optional<std::string> get_option_value(const SeastarOption& option) {
  switch (option.value_type) {
    case Option::TYPE_FLOAT: {
      if (auto value = crimson::common::get_conf<double>(option.config_key)) {
        return std::to_string(value);
      }
      break;
    }
    case Option::TYPE_UINT: {
      if (auto value = crimson::common::get_conf<uint64_t>(option.config_key)) {
        return std::to_string(value);
      }
      break;
    }
    case Option::TYPE_BOOL: {
     if (crimson::common::get_conf<bool>(option.config_key)) {
        return "true";
      }
      break;
    }
    default:
      logger().warn("get_option_value --option_name {} encountered unknown type", option.config_key);
      return std::nullopt;
  }
  return std::nullopt;
}

static tl::expected<early_config_t, int>
_get_early_config(const std::vector<const char*>& args)
{
  early_config_t ret;

  std::vector<const char *> early_args(args.begin(), args.end());

  ret.init_params = ceph_argparse_early_args(
    early_args,
    CEPH_ENTITY_TYPE_OSD,
    &ret.cluster_name,
    &ret.conf_file_list);

  seastar::app_template::config app_cfg;
  app_cfg.name = "Crimson-startup";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));
  const char *bootstrap_args[] = { args[0], "--smp", "1" };
  int r = app.run(
    sizeof(bootstrap_args) / sizeof(bootstrap_args[0]),
    const_cast<char**>(bootstrap_args),
    [&args, &ret, &early_args] {
      return seastar::async([&args, &ret, &early_args] {
	seastar::global_logger_registry().set_all_loggers_level(
	  seastar::log_level::debug);
	sharded_conf().start(
	  ret.init_params.name, ret.cluster_name).get();
	local_conf().start().get();
	auto stop_conf = seastar::deferred_stop(sharded_conf());

	sharded_perf_coll().start().get();
	auto stop_perf_coll = seastar::deferred_stop(sharded_perf_coll());

	local_conf().parse_env().get();
	local_conf().parse_argv(early_args).get();
	local_conf().parse_config_files(ret.conf_file_list).get();

	if (local_conf()->no_mon_config) {
	  logger().info("bypassing the config fetch due to --no-mon-config");
	} else {
	  populate_config_from_mon().get();
	}

	std::set_difference(
	  args.begin(), args.end(),
	  std::begin(early_args),
	  std::end(early_args),
	  std::back_inserter(ret.ceph_args));

	ret.early_args.insert(
	  std::end(ret.early_args),
	  std::begin(early_args),
	  std::end(early_args));

        for (const auto& option : seastar_options) {
          auto option_value = get_option_value(option);
          if (option_value) {
            logger().info("Configure option_name {} with value : {}", option.config_key, option_value);
            ret.early_args.emplace_back(option.option_name);
            if (option.value_type != Option::TYPE_BOOL) {
              ret.early_args.emplace_back(*option_value);
            }
          }
        }
	if (auto found = std::find_if(
	      std::begin(early_args),
	      std::end(early_args),
	      [](auto* arg) { return "--cpuset"sv == arg; });
	    found == std::end(early_args)) {
	  auto cpu_cores = crimson::common::get_conf<std::string>("crimson_cpu_set");
	  if (!cpu_cores.empty()) {
	    // Set --cpuset based on crimson_cpu_set config option
	    // --smp default is one per CPU
	    ret.early_args.emplace_back("--cpuset");
	    ret.early_args.emplace_back(cpu_cores);
	    ret.early_args.emplace_back("--thread-affinity");
	    ret.early_args.emplace_back("1");
	    logger().info("get_early_config: set --thread-affinity 1 --cpuset {}",
	                  cpu_cores);
	  } else {
	    auto reactor_num = crimson::common::get_conf<uint64_t>("crimson_cpu_num");
	    if (!reactor_num) {
	      // We would like to avoid seastar using all available cores.
	      logger().error("get_early_config: crimson_cpu_set"
	                     " or crimson_cpu_num must be set");
	      ceph_abort();
	    }
	    std::string smp = fmt::format("{}", reactor_num);
	    ret.early_args.emplace_back("--smp");
	    ret.early_args.emplace_back(smp);
	    ret.early_args.emplace_back("--thread-affinity");
	    ret.early_args.emplace_back("0");
	    logger().info("get_early_config: set --thread-affinity 0 --smp {}",
	                  smp);

	  }
	} else {
	  logger().error("get_early_config: --cpuset can be "
	                 "set only using crimson_cpu_set");
	  ceph_abort();
	}
	return 0;
      });
    });
  if (r < 0) {
    return tl::unexpected(r);
  }
  return ret;
}

/* get_early_config handles obtaining config parameters required prior
 * to reactor startup.  Most deployment mechanisms (cephadm for one)
 * rely on pulling configs from the monitor rather than shipping around
 * config files, so this process needs to support pulling config options
 * from the monitors.
 *
 * Of particular interest are config params related to the seastar
 * reactor itself which can't be modified after the reactor has been
 * started -- like the number of cores to use (smp::count).  Contacting
 * the monitors, however, requires a MonClient, which in turn needs a
 * running reactor.
 *
 * Unfortunately, seastar doesn't clean up thread local state
 * associated with seastar::smp task queues etc, so we can't
 * start a reactor, stop it, and restart it in the same thread
 * without an impractical amount of cleanup in seastar.
 *
 * More unfortunately, starting a reactor in a seperate thread
 * and then joining the thread still doesn't avoid all global state,
 * I observed tasks from the previous reactor incarnation nevertheless
 * continuing to run in the new one resulting in a crash as they access
 * freed memory.
 *
 * The approach taken here, therefore, is to actually fork, start a
 * reactor in the child process, encode the resulting early_config_t,
 * and send it back to the parent process.
 */
tl::expected<early_config_t, int>
get_early_config(int argc, const char *argv[])
{
  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    std::cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  crimson_options_t crimson_options;
  namespace bpo = boost::program_options;
  bpo::options_description desc{"Crimson OSD Options"};
  desc.add_options()
    ("help,h", "produce help message")
    ("mkkey", bpo::bool_switch(&crimson_options.mkkey),
     "generate a new secret key. This is normally used in combination with --mkfs")
    ("mkfs", bpo::bool_switch(&crimson_options.mkfs),
     "create a [new] data directory")
    ("debug", bpo::bool_switch(&crimson_options.debug),
     "enable debug output on all loggers")
    ("trace", bpo::bool_switch(&crimson_options.trace),
     "enable trace output on all loggers")
    ("osdspec-affinity", bpo::value<std::string>(&crimson_options.osdspec_affinity)->default_value(""),
     "set affinity to an osdspec")
    ("prometheus_port", bpo::value<uint16_t>(&crimson_options.prometheus_port)->default_value(0),
     "Prometheus port. Set to zero to disable")
    ("prometheus_address", bpo::value<std::string>(&crimson_options.prometheus_address)->default_value("0.0.0.0"),
     "Prometheus listening address")
    ("prometheus_prefix", bpo::value<std::string>(&crimson_options.prometheus_prefix)->default_value("osd"),
     "Prometheus metrics prefix");
  bpo::variables_map vm;
  std::vector<const char*> seastar_options;
  std::vector<std::string> unrecognized_options; // avoid lifetime issues
  try {
    auto parsed = bpo::command_line_parser(argc, argv)
      .options(desc)
      .allow_unregistered()
      .run();
    bpo::store(parsed, vm);

    if (vm.count("help")) {
      std::cout << "usage: crimson-osd -i <ID> [flags]\n\n";
      std::cout << "Common Ceph options:\n";
      std::cout << "  --conf/-c FILE    read configuration from the given configuration file\n";
      std::cout << "  --id/-i ID        set ID portion of my name\n";
      std::cout << "  --name/-n TYPE.ID set name\n";
      std::cout << "  --cluster NAME    set cluster name (default: ceph)\n";
      std::cout << "  --setuser USER    set uid to user or uid (and gid to user's gid)\n";
      std::cout << "  --setgroup GROUP  set gid to group or gid\n";
      std::cout << "  --version         show version and quit\n";
      std::cout << "  -d                run in foreground, log to stderr\n";
      std::cout << "  -f                run in foreground, log to usual location\n";
      std::cout << "  --debug_ms N      set message debug level (e.g. 1)\n\n";
      std::cout << desc << "\n";
      exit(0);
    }

    bpo::notify(vm);
    unrecognized_options =
      bpo::collect_unrecognized(parsed.options, bpo::include_positional);
  } catch(const bpo::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    std::cerr << desc << std::endl;
    return tl::unexpected(-EINVAL);
  }

  int pipes[2];
  int r = pipe2(pipes, 0);
  if (r < 0) {
    std::cerr << "get_early_config: failed to create pipes: "
	      << -errno << std::endl;
    return tl::unexpected(-errno);
  }

  pid_t worker = fork();
  if (worker < 0) {
    close(pipes[0]);
    close(pipes[1]);
    std::cerr << "get_early_config: failed to fork: "
	      << -errno << std::endl;
    return tl::unexpected(-errno);
  } else if (worker == 0) { // child
    close(pipes[0]);
    seastar_options.push_back(argv[0]);
    for (auto& opt : unrecognized_options) {
      seastar_options.push_back(opt.c_str());
    }
    auto ret = _get_early_config(seastar_options);
    if (ret.has_value()) {
      bufferlist bl;
      ::encode(ret.value(), bl);
      r = bl.write_fd(pipes[1]);
      close(pipes[1]);
      if (r < 0) {
	std::cerr << "get_early_config: child failed to write_fd: "
		  << r << std::endl;
	exit(-r);
      } else {
	exit(0);
      }
    } else {
      std::cerr << "get_early_config: child failed: "
		<< -ret.error() << std::endl;
      exit(-ret.error());
    }
    return tl::unexpected(-1);
  } else { // parent
    close(pipes[1]);

    bufferlist bl;
    early_config_t ret;
    bool have_data = false;
    while ((r = bl.read_fd(pipes[0], 1024)) > 0) {
      have_data = true;
    }
    close(pipes[0]);

    int status;
    waitpid(worker, &status, 0);

    // One of the parameters was taged as exit(0) in the child process
    // so we need to check if we should exit here
    if (!have_data && WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      exit(0);
    }
    if (r < 0) {
      std::cerr << "get_early_config: parent failed to read from pipe: "
		<< r << std::endl;
      return tl::unexpected(r);
    }
    try {
      auto bliter = bl.cbegin();
      ::decode(ret, bliter);
      // crimson options are collected in the parent process and don't need to be passed through the pipe
      ret.crimson_options = crimson_options;
      return ret;
    } catch (...) {
      std::cerr << "get_early_config: parent failed to decode" << std::endl;
      return tl::unexpected(-EINVAL);
    }
  }
}

}
