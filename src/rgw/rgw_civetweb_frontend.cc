// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <set>
#include <string>

#include <boost/utility/string_ref.hpp>

#include "rgw_frontend.h"
#include "rgw_client_io_filters.h"

#define dout_subsys ceph_subsys_rgw

static int civetweb_callback(struct mg_connection* conn)
{
  struct mg_request_info* const req_info = mg_get_request_info(conn);
  return static_cast<RGWCivetWebFrontend *>(req_info->user_data)->process(conn);
}

int RGWCivetWebFrontend::process(struct mg_connection*  const conn)
{
  /* Hold a read lock over access to env.store for reconfiguration. */
  RWLock::RLocker lock(env.mutex);

  RGWCivetWeb cw_client(conn, env.port);
  auto real_client_io = rgw::io::add_reordering(
                          rgw::io::add_buffering(
                            rgw::io::add_chunking(
                              rgw::io::add_conlen_controlling(
                                &cw_client))));
  RGWRestfulIO client_io(&real_client_io);

  RGWRequest req(env.store->get_new_req_id());
  int ret = process_request(env.store, env.rest, &req, env.uri_prefix,
                            &client_io, env.olog);
  if (ret < 0) {
    /* We don't really care about return code. */
    dout(20) << "process_request() returned " << ret << dendl;
  }

  /* Mark as processed. */
  return 1;
}

int RGWCivetWebFrontend::run()
{
  auto& conf_map = conf->get_config_map();

  set_conf_default(conf_map, "num_threads",
                   std::to_string(g_conf->rgw_thread_pool_size));
  set_conf_default(conf_map, "decode_url", "no");
  set_conf_default(conf_map, "enable_keep_alive", "yes");
  conf_map["listening_ports"] = conf->get_val("port", "80");

  /* Set run_as_user. This will cause civetweb to invoke setuid() and setgid()
   * based on pw_uid and pw_gid obtained from pw_name. */
  std::string uid_string = g_ceph_context->get_set_uid_string();
  if (! uid_string.empty()) {
    conf_map["run_as_user"] = std::move(uid_string);
  }

  /* Prepare options for CivetWeb. */
  const std::set<boost::string_ref> rgw_opts = { "port", "prefix" };
  const size_t CW_NUM_OPTS = 2 * (conf_map.size() - rgw_opts.size()) + 1;
  const char *options[CW_NUM_OPTS];
  size_t i = 0;

  for (const auto& pair : conf_map) {
    if (! rgw_opts.count(pair.first)) {
      /* CivetWeb doesn't understand configurables of the glue layer between
       * it and RadosGW. We need to strip them out. Otherwise CivetWeb would
       * signalise an error. */
      options[i + 0] = pair.first.c_str();
      options[i + 1] = pair.second.c_str();

      dout(20) << "civetweb config: " << options[i] << ": "
               << (options[i + 1] ? options[i + 1] : "<null>") << dendl;
      i += 2;
    }
  }
  options[i] = nullptr;

  /* Initialize the CivetWeb right now. */
  struct mg_callbacks cb;
  memset((void *)&cb, 0, sizeof(cb));
  cb.begin_request = civetweb_callback;
  cb.log_message = rgw_civetweb_log_callback;
  cb.log_access = rgw_civetweb_log_access_callback;
  ctx = mg_start(&cb, this, (const char **)&options);

  return ! ctx ? -EIO : 0;
} /* RGWCivetWebFrontend::run */
