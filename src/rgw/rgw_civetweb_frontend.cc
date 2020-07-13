// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <set>
#include <string>
#include <string_view>

#include "rgw_frontend.h"
#include "rgw_client_io_filters.h"
#include "rgw_dmclock_sync_scheduler.h"

#define dout_subsys ceph_subsys_rgw

namespace dmc = rgw::dmclock;

RGWCivetWebFrontend::RGWCivetWebFrontend(RGWProcessEnv& env,
					 RGWFrontendConfig *conf,
					 dmc::SchedulerCtx& sched_ctx)
  : conf(conf),
    ctx(nullptr),
    env(env)
{

  auto sched_t = dmc::get_scheduler_t(cct());
  switch(sched_t){
  case dmc::scheduler_t::none: [[fallthrough]];
  case dmc::scheduler_t::throttler:
    break;
  case dmc::scheduler_t::dmclock:
    // TODO: keep track of server ready state and use that here civetweb
    // internally tracks in the ctx the threads used and free, while it is
    // expected with the current implementation that the threads waiting on the
    // queue would still show up in the "used" queue, it might be a useful thing
    // to make decisions on in the future. Also while reconfiguring we should
    // probably set this to false
    auto server_ready_f = []() -> bool { return true; };

    scheduler.reset(new dmc::SyncScheduler(cct(),
					   std::ref(sched_ctx.get_dmc_client_counters()),
					   *sched_ctx.get_dmc_client_config(),
					   server_ready_f,
					   std::ref(dmc::SyncScheduler::handle_request_cb),
					   dmc::AtLimit::Reject));
  }

}

static int civetweb_callback(struct mg_connection* conn)
{
  const struct mg_request_info* const req_info = mg_get_request_info(conn);
  return static_cast<RGWCivetWebFrontend *>(req_info->user_data)->process(conn);
}

int RGWCivetWebFrontend::process(struct mg_connection*  const conn)
{
  /* Hold a read lock over access to env.store for reconfiguration. */
  std::shared_lock lock{env.mutex};

  RGWCivetWeb cw_client(conn);
  auto real_client_io = rgw::io::add_reordering(
                          rgw::io::add_buffering(dout_context,
                            rgw::io::add_chunking(
                              rgw::io::add_conlen_controlling(
                                &cw_client))));
  RGWRestfulIO client_io(dout_context, &real_client_io);

  RGWRequest req(env.store->getRados()->get_new_req_id());
  int http_ret = 0;
  //assert (scheduler != nullptr);
  int ret = process_request(env.store, env.rest, &req, env.uri_prefix,
                            *env.auth_registry, &client_io, env.olog,
                            null_yield, scheduler.get() ,&http_ret);
  if (ret < 0) {
    /* We don't really care about return code. */
    dout(20) << "process_request() returned " << ret << dendl;
  }

  if (http_ret <= 0) {
    /* Mark as processed. */
    return 1;
  }

  return http_ret;
}

int RGWCivetWebFrontend::run()
{
  auto& conf_map = conf->get_config_map();

  set_conf_default(conf_map, "num_threads",
                   std::to_string(g_conf()->rgw_thread_pool_size));
  set_conf_default(conf_map, "decode_url", "no");
  set_conf_default(conf_map, "enable_keep_alive", "yes");
  set_conf_default(conf_map, "validate_http_method", "no");
  set_conf_default(conf_map, "canonicalize_url_path", "no");
  set_conf_default(conf_map, "enable_auth_domain_check", "no");
  set_conf_default(conf_map, "allow_unicode_in_urls", "yes");
  set_conf_default(conf_map, "request_timeout_ms", "65000");

  std::string listening_ports;
  // support multiple port= entries
  auto range = conf_map.equal_range("port");
  for (auto p = range.first; p != range.second; ++p) {
    std::string port_str = p->second;
    // support port= entries with multiple values
    std::replace(port_str.begin(), port_str.end(), '+', ',');
    if (!listening_ports.empty()) {
      listening_ports.append(1, ',');
    }
    listening_ports.append(port_str);
  }
  if (listening_ports.empty()) {
    listening_ports = "80";
  }
  conf_map.emplace("listening_ports", std::move(listening_ports));

  /* Set run_as_user. This will cause civetweb to invoke setuid() and setgid()
   * based on pw_uid and pw_gid obtained from pw_name. */
  std::string uid_string = g_ceph_context->get_set_uid_string();
  if (! uid_string.empty()) {
    conf_map.emplace("run_as_user", std::move(uid_string));
  }

  /* Prepare options for CivetWeb. */
  const std::set<std::string_view> rgw_opts = { "port", "prefix" };

  std::vector<const char*> options;

  for (const auto& pair : conf_map) {
    if (! rgw_opts.count(pair.first)) {
      /* CivetWeb doesn't understand configurables of the glue layer between
       * it and RadosGW. We need to strip them out. Otherwise CivetWeb would
       * signalise an error. */
      options.push_back(pair.first.c_str());
      options.push_back(pair.second.c_str());

      dout(20) << "civetweb config: " << pair.first
               << ": " << pair.second << dendl;
    }
  }

  options.push_back(nullptr);
  /* Initialize the CivetWeb right now. */
  struct mg_callbacks cb;
  // FIPS zeroization audit 20191115: this memset is not security related.
  memset((void *)&cb, 0, sizeof(cb));
  cb.begin_request = civetweb_callback;
  cb.log_message = rgw_civetweb_log_callback;
  cb.log_access = rgw_civetweb_log_access_callback;
  ctx = mg_start(&cb, this, options.data());

  return ! ctx ? -EIO : 0;
} /* RGWCivetWebFrontend::run */
