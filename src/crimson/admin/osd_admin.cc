// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/admin/osd_admin.h"
#include <string>
#include <string_view>

#include <fmt/format.h>
#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/scollectd_api.hh>

#include "common/config.h"
#include "crimson/admin/admin_socket.h"
#include "crimson/common/log.h"
#include "crimson/osd/exceptions.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/shard_services.h"

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

using namespace std::literals;
using std::string_view;
using std::unique_ptr;
using crimson::osd::OSD;
using crimson::common::local_conf;
using namespace crimson::common;
using ceph::common::cmd_getval;
using ceph::common::cmd_getval_or;

namespace crimson::admin {

template <class Hook, class... Args>
std::unique_ptr<AdminSocketHook> make_asok_hook(Args&&... args)
{
  return std::make_unique<Hook>(std::forward<Args>(args)...);
}

/**
 * An OSD admin hook: OSD status
 */
class OsdStatusHook : public AdminSocketHook {
public:
  explicit OsdStatusHook(const crimson::osd::OSD& osd) :
    AdminSocketHook{"status", "", "OSD status"},
    osd(osd)
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->open_object_section("status");
    osd.dump_status(f.get());
    f->close_section();
    return seastar::make_ready_future<tell_result_t>(std::move(f));
  }
private:
  const crimson::osd::OSD& osd;
};
template std::unique_ptr<AdminSocketHook>
make_asok_hook<OsdStatusHook>(const crimson::osd::OSD& osd);

/**
 * An OSD admin hook: send beacon
 */
class SendBeaconHook : public AdminSocketHook {
public:
  explicit SendBeaconHook(crimson::osd::OSD& osd) :
    AdminSocketHook{"send_beacon",
		    "",
		    "send OSD beacon to mon immediately"},
    osd(osd)
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    return osd.send_beacon().then([] {
      return seastar::make_ready_future<tell_result_t>();
    });
  }
private:
  crimson::osd::OSD& osd;
};
template std::unique_ptr<AdminSocketHook>
make_asok_hook<SendBeaconHook>(crimson::osd::OSD& osd);

/**
 * send the latest pg stats to mgr
 */
class FlushPgStatsHook : public AdminSocketHook {
public:
  explicit FlushPgStatsHook(crimson::osd::OSD& osd) :
    AdminSocketHook("flush_pg_stats",
		    "",
		    "flush pg stats"),
    osd{osd}
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    uint64_t seq = osd.send_pg_stats();
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->dump_unsigned("stat_seq", seq);
    return seastar::make_ready_future<tell_result_t>(std::move(f));
  }

private:
  crimson::osd::OSD& osd;
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<FlushPgStatsHook>(crimson::osd::OSD& osd);

/// dump the history of PGs' peering state
class DumpPGStateHistory final: public AdminSocketHook {
public:
  explicit DumpPGStateHistory(const crimson::osd::PGShardManager &pg_shard_manager) :
    AdminSocketHook{"dump_pgstate_history",
                    "",
                    "dump history of PGs' peering state"},
    pg_shard_manager{pg_shard_manager}
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
                                      std::string_view format,
                                      ceph::bufferlist&& input) const final
  {
    std::unique_ptr<Formatter> fref{
      Formatter::create(format, "json-pretty", "json-pretty")};
    Formatter *f = fref.get();
    f->open_object_section("pgstate_history");
    f->open_array_section("pgs");
    return pg_shard_manager.for_each_pg([f](auto &pgid, auto &pg) {
      f->open_object_section("pg");
      f->dump_stream("pg") << pgid;
      const auto& peering_state = pg->get_peering_state();
      f->dump_string("currently", peering_state.get_current_state());
      peering_state.dump_history(f);
      f->close_section();
    }).then([fref=std::move(fref)]() mutable {
      fref->close_section();
      fref->close_section();
      return seastar::make_ready_future<tell_result_t>(std::move(fref));
    });
  }

private:
  const crimson::osd::PGShardManager &pg_shard_manager;
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<DumpPGStateHistory>(
  const crimson::osd::PGShardManager &);

//dump the contents of perfcounters in osd and store
class DumpPerfCountersHook final: public AdminSocketHook {
public:
  explicit DumpPerfCountersHook() :
    AdminSocketHook{"perfcounters_dump",
                    "name=logger,type=CephString,req=false "
                    "name=counter,type=CephString,req=false",
                    "dump perfcounters in osd and store"}
  {}
  seastar::future<tell_result_t> call(const cmdmap_t& cmdmap,
                                      std::string_view format,
                                      ceph::bufferlist&& input) const final
  {
    std::unique_ptr<Formatter> f{Formatter::create(format,
                                                   "json-pretty",
                                                   "json-pretty")};
    std::string logger;
    std::string counter;
    cmd_getval(cmdmap, "logger", logger);
    cmd_getval(cmdmap, "counter", counter);

    crimson::common::local_perf_coll().dump_formatted(f.get(), false, false, logger, counter);
    return seastar::make_ready_future<tell_result_t>(std::move(f));
  }
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<DumpPerfCountersHook>();



/**
 * A CephContext admin hook: calling assert (if allowed by
 * 'debug_asok_assert_abort')
 */
class AssertAlwaysHook : public AdminSocketHook {
public:
  AssertAlwaysHook()  :
    AdminSocketHook{"assert",
		    "",
		    "asserts"}
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    if (local_conf().get_val<bool>("debug_asok_assert_abort")) {
      ceph_assert_always(0);
      return seastar::make_ready_future<tell_result_t>();
    } else {
      return seastar::make_ready_future<tell_result_t>(
        tell_result_t{-EPERM, "configuration set to disallow asok assert"});
    }
  }
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<AssertAlwaysHook>();

/**
 * A Seastar admin hook: fetching the values of configured metrics
 */
class DumpMetricsHook : public AdminSocketHook {
public:
  DumpMetricsHook() :
    AdminSocketHook("dump_metrics",
                    "name=group,type=CephString,req=false",
                    "dump current configured seastar metrics and their values")
  {}
  seastar::future<tell_result_t> call(const cmdmap_t& cmdmap,
                                      std::string_view format,
                                      ceph::bufferlist&& input) const final
  {
    std::unique_ptr<Formatter> fref{Formatter::create(format, "json-pretty", "json-pretty")};
    auto *f = fref.get();
    std::string prefix;
    cmd_getval(cmdmap, "group", prefix);
    f->open_object_section("metrics");
    f->open_array_section("metrics");
    return seastar::do_with(std::move(prefix), [f](auto &prefix) {
      return crimson::reactor_map_seq([f, &prefix] {
        for (const auto& [full_name, metric_family]: seastar::scollectd::get_value_map()) {
          if (!prefix.empty() && full_name.compare(0, prefix.size(), prefix) != 0) {
            continue;
          }
          for (const auto& [labels, metric] : metric_family) {
            if (metric && metric->is_enabled()) {
	      f->open_object_section(""); // enclosed by array
              DumpMetricsHook::dump_metric_value(f, full_name, *metric, labels);
	      f->close_section();
            }
          }
        }
      });
    }).then([fref = std::move(fref)]() mutable {
      fref->close_section();
      fref->close_section();
      return seastar::make_ready_future<tell_result_t>(std::move(fref));
    });
  }
private:
  using registered_metric = seastar::metrics::impl::registered_metric;
  using data_type = seastar::metrics::impl::data_type;

  static void dump_metric_value(Formatter* f,
                                string_view full_name,
                                const registered_metric& metric,
                                const seastar::metrics::impl::labels_type& labels)
  {
    f->open_object_section(full_name);
    for (const auto& [key, value] : labels) {
      f->dump_string(key, value);
    }
    auto value_name = "value";
    switch (auto v = metric(); v.type()) {
    case data_type::GAUGE:
      f->dump_float(value_name, v.d());
      break;
    case data_type::REAL_COUNTER:
      f->dump_float(value_name, v.d());
      break;
    case data_type::COUNTER:
      double val;
      try {
	val = v.ui();
      } catch (std::range_error&) {
	// seastar's cpu steal time may be negative
	val = 0;
      }
      f->dump_unsigned(value_name, val);
      break;
    case data_type::HISTOGRAM: {
      f->open_object_section(value_name);
      auto&& h = v.get_histogram();
      f->dump_float("sum", h.sample_sum);
      f->dump_unsigned("count", h.sample_count);
      f->open_array_section("buckets");
      for (auto i : h.buckets) {
        f->open_object_section("bucket");
        f->dump_float("le", i.upper_bound);
        f->dump_unsigned("count", i.count);
        f->close_section(); // "bucket"
      }
      {
        f->open_object_section("bucket");
        f->dump_string("le", "+Inf");
        f->dump_unsigned("count", h.sample_count);
        f->close_section();
      }
      f->close_section(); // "buckets"
      f->close_section(); // value_name
    }
      break;
    default:
      std::abort();
      break;
    }
    f->close_section(); // full_name
  }
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<DumpMetricsHook>();


static ghobject_t test_ops_get_object_name(
  const OSDMap& osdmap,
  const cmdmap_t& cmdmap)
{
  auto pool = [&] {
    auto pool_arg = cmd_getval<std::string>(cmdmap, "pool");
    if (!pool_arg) {
      throw std::invalid_argument{"No 'pool' specified"};
    }
    int64_t pool = osdmap.lookup_pg_pool_name(*pool_arg);
    if (pool < 0 && std::isdigit((*pool_arg)[0])) {
      pool = std::atoll(pool_arg->c_str());
    }
    if (pool < 0) {
      // the return type of `fmt::format` is `std::string`
      throw std::invalid_argument{
        fmt::format("Invalid pool '{}'", *pool_arg)
      };
    }
    return pool;
  }();

  auto [ objname, nspace, raw_pg ] = [&] {
    auto obj_arg = cmd_getval<std::string>(cmdmap, "objname");
    if (!obj_arg) {
      throw std::invalid_argument{"No 'objname' specified"};
    }
    std::string objname, nspace;
    if (std::size_t sep_pos = obj_arg->find_first_of('/');
        sep_pos != obj_arg->npos) {
      nspace = obj_arg->substr(0, sep_pos);
      objname = obj_arg->substr(sep_pos+1);
    } else {
      objname = *obj_arg;
    }
    pg_t raw_pg;
    if (object_locator_t oloc(pool, nspace);
        osdmap.object_locator_to_pg(object_t(objname), oloc,  raw_pg) < 0) {
      throw std::invalid_argument{"Invalid namespace/objname"};
    }
    return std::make_tuple(std::move(objname),
                           std::move(nspace),
                           std::move(raw_pg));
  }();

  auto shard_id = cmd_getval_or<int64_t>(cmdmap,
					 "shardid",
					 shard_id_t::NO_SHARD);

  return ghobject_t{
    hobject_t{
      object_t{objname}, std::string{}, CEPH_NOSNAP, raw_pg.ps(), pool, nspace
    },
    ghobject_t::NO_GEN,
    shard_id_t{static_cast<int8_t>(shard_id)}
  };
}

// Usage:
//   injectdataerr <pool> [namespace/]<obj-name> [shardid]
class InjectDataErrorHook : public AdminSocketHook {
public:
  InjectDataErrorHook(crimson::osd::ShardServices& shard_services)  :
   AdminSocketHook("injectdataerr",
    "name=pool,type=CephString " \
    "name=objname,type=CephObjectname " \
    "name=shardid,type=CephInt,req=false,range=0|255",
    "inject data error to an object"),
   shard_services(shard_services) {
  }

  seastar::future<tell_result_t> call(const cmdmap_t& cmdmap,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    ghobject_t obj;
    try {
      obj = test_ops_get_object_name(*shard_services.get_map(), cmdmap);
    } catch (const std::invalid_argument& e) {
      logger().info("error during data error injection: {}", e.what());
      return seastar::make_ready_future<tell_result_t>(-EINVAL,
	                                               e.what());
    }
    return shard_services.get_store().inject_data_error(obj).then([=] {
      logger().info("successfully injected data error for obj={}", obj);
      ceph::bufferlist bl;
      bl.append("ok"sv);
      return seastar::make_ready_future<tell_result_t>(0,
						       std::string{}, // no err
						       std::move(bl));
    });
  }

private:
  crimson::osd::ShardServices& shard_services;
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<InjectDataErrorHook>(
  crimson::osd::ShardServices&);


// Usage:
//   injectmdataerr <pool> [namespace/]<obj-name> [shardid]
class InjectMDataErrorHook : public AdminSocketHook {
public:
  InjectMDataErrorHook(crimson::osd::ShardServices& shard_services)  :
   AdminSocketHook("injectmdataerr",
    "name=pool,type=CephString " \
    "name=objname,type=CephObjectname " \
    "name=shardid,type=CephInt,req=false,range=0|255",
    "inject data error to an object"),
   shard_services(shard_services) {
  }

  seastar::future<tell_result_t> call(const cmdmap_t& cmdmap,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    ghobject_t obj;
    try {
      obj = test_ops_get_object_name(*shard_services.get_map(), cmdmap);
    } catch (const std::invalid_argument& e) {
      logger().info("error during metadata error injection: {}", e.what());
      return seastar::make_ready_future<tell_result_t>(-EINVAL,
	                                               e.what());
    }
    return shard_services.get_store().inject_mdata_error(obj).then([=] {
      logger().info("successfully injected metadata error for obj={}", obj);
      ceph::bufferlist bl;
      bl.append("ok"sv);
      return seastar::make_ready_future<tell_result_t>(0,
						       std::string{}, // no err
						       std::move(bl));
    });
  }

private:
  crimson::osd::ShardServices& shard_services;
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<InjectMDataErrorHook>(
  crimson::osd::ShardServices&);


/**
 * An InFlightOps admin hook: dump current in-flight operations
 */
class DumpInFlightOpsHook : public AdminSocketHook {
public:
  explicit DumpInFlightOpsHook(const crimson::osd::PGShardManager &pg_shard_manager) :
    AdminSocketHook{"dump_ops_in_flight", "", "show the ops currently in flight"},
    pg_shard_manager(pg_shard_manager)
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    unique_ptr<Formatter> fref{
      Formatter::create(format, "json-pretty", "json-pretty")};
    auto *f = fref.get();
    f->open_object_section("ops_in_flight");
    f->open_array_section("ops_in_flight");
    return pg_shard_manager.invoke_on_each_shard_seq([f](const auto &shard_services) {
      return shard_services.dump_ops_in_flight(f);
    }).then([fref=std::move(fref)]() mutable {
      fref->close_section();
      fref->close_section();
      return seastar::make_ready_future<tell_result_t>(std::move(fref));
    });
  }
private:
  const crimson::osd::PGShardManager &pg_shard_manager;
};
template std::unique_ptr<AdminSocketHook>
make_asok_hook<DumpInFlightOpsHook>(const crimson::osd::PGShardManager &);


class DumpHistoricOpsHook : public AdminSocketHook {
public:
  explicit DumpHistoricOpsHook(const crimson::osd::OSDOperationRegistry& op_registry) :
    AdminSocketHook{"dump_historic_ops", "", "show recent ops"},
    op_registry(op_registry)
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->open_object_section("historic_ops");
    op_registry.dump_historic_client_requests(f.get());
    f->close_section();
    f->dump_int("num_ops", 0);
    return seastar::make_ready_future<tell_result_t>(std::move(f));
  }
private:
  const crimson::osd::OSDOperationRegistry& op_registry;
};
template std::unique_ptr<AdminSocketHook>
make_asok_hook<DumpHistoricOpsHook>(const crimson::osd::OSDOperationRegistry& op_registry);


class DumpSlowestHistoricOpsHook : public AdminSocketHook {
public:
  explicit DumpSlowestHistoricOpsHook(const crimson::osd::OSDOperationRegistry& op_registry) :
    AdminSocketHook{"dump_historic_slow_ops", "", "show slowest recent ops"},
    op_registry(op_registry)
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    logger().warn("{}", __func__);
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->open_object_section("historic_slow_ops");
    op_registry.dump_slowest_historic_client_requests(f.get());
    f->close_section();
    f->dump_int("num_ops", 0);
    return seastar::make_ready_future<tell_result_t>(std::move(f));
  }
private:
  const crimson::osd::OSDOperationRegistry& op_registry;
};
template std::unique_ptr<AdminSocketHook>
make_asok_hook<DumpSlowestHistoricOpsHook>(const crimson::osd::OSDOperationRegistry& op_registry);

class DumpRecoveryReservationsHook : public AdminSocketHook {
public:
  explicit DumpRecoveryReservationsHook(crimson::osd::ShardServices& shard_services) :
    AdminSocketHook{"dump_recovery_reservations", "", "show recovery reservations"},
    shard_services(shard_services)
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    logger().debug("{}", __func__);
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    return seastar::do_with(std::move(f), [this](auto&& f) {
      f->open_object_section("reservations");
      f->open_object_section("local_reservations");
      return shard_services.local_dump_reservations(f.get()).then([&f, this] {
        f->close_section();
        f->open_object_section("remote_reservations");
        return shard_services.remote_dump_reservations(f.get()).then([&f] {
          f->close_section();
          f->close_section();
          return seastar::make_ready_future<tell_result_t>(std::move(f));
        });
      });
    });
  }
private:
  crimson::osd::ShardServices& shard_services;
};
template std::unique_ptr<AdminSocketHook>
make_asok_hook<DumpRecoveryReservationsHook>(crimson::osd::ShardServices& shard_services);

} // namespace crimson::admin
