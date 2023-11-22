// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_LIBRADOS_RADOSCLIENT_H
#define CEPH_LIBRADOS_RADOSCLIENT_H

#include <functional>
#include <memory>
#include <string>

#include "msg/Dispatcher.h"

#include "common/async/context_pool.h"
#include "common/config_fwd.h"
#include "common/Cond.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "common/config_obs.h"
#include "include/common_fwd.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "mon/MonClient.h"
#include "mgr/MgrClient.h"

#include "IoCtxImpl.h"

struct Context;
class Message;
class MLog;
class Messenger;
class AioCompletionImpl;

namespace neorados { namespace detail { class RadosClient; }}

class librados::RadosClient : public Dispatcher,
			      public md_config_obs_t
{
  friend neorados::detail::RadosClient;
public:
  using Dispatcher::cct;
private:
  std::unique_ptr<CephContext,
		  std::function<void(CephContext*)>> cct_deleter;

public:
  const ConfigProxy& conf{cct->_conf};
  ceph::async::io_context_pool poolctx;
private:
  enum {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
  } state{DISCONNECTED};

  MonClient monclient{cct, poolctx};
  MgrClient mgrclient{cct, nullptr, &monclient.monmap};
  Messenger *messenger{nullptr};

  uint64_t instance_id{0};

  bool _dispatch(Message *m);
  bool ms_dispatch(Message *m) override;

  void ms_handle_connect(Connection *con) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override;
  bool ms_handle_refused(Connection *con) override;

  Objecter *objecter{nullptr};

  ceph::mutex lock = ceph::make_mutex("librados::RadosClient::lock");
  ceph::condition_variable cond;
  int refcnt{1};

  version_t log_last_version{0};
  rados_log_callback_t log_cb{nullptr};
  rados_log_callback2_t log_cb2{nullptr};
  void *log_cb_arg{nullptr};
  std::string log_watch;

  bool service_daemon = false;
  std::string daemon_name, service_name;
  std::map<std::string,std::string> daemon_metadata;
  ceph::timespan rados_mon_op_timeout{};

  int wait_for_osdmap();

public:
  boost::asio::io_context::strand finish_strand{poolctx.get_io_context()};

  explicit RadosClient(CephContext *cct);
  ~RadosClient() override;
  int ping_monitor(std::string mon_id, std::string *result);
  int connect();
  void shutdown();

  int watch_flush();
  int async_watch_flush(AioCompletionImpl *c);

  uint64_t get_instance_id();

  int get_min_compatible_osd(int8_t* require_osd_release);
  int get_min_compatible_client(int8_t* min_compat_client,
                                int8_t* require_min_compat_client);

  int wait_for_latest_osdmap();

  int create_ioctx(const char *name, IoCtxImpl **io);
  int create_ioctx(int64_t, IoCtxImpl **io);

  int get_fsid(std::string *s);
  int64_t lookup_pool(const char *name);
  bool pool_requires_alignment(int64_t pool_id);
  int pool_requires_alignment2(int64_t pool_id, bool *req);
  uint64_t pool_required_alignment(int64_t pool_id);
  int pool_required_alignment2(int64_t pool_id, uint64_t *alignment);
  int pool_get_name(uint64_t pool_id, std::string *name,
		    bool wait_latest_map = false);

  int pool_list(std::list<std::pair<int64_t, std::string> >& ls);
  int get_pool_stats(std::list<std::string>& ls, std::map<std::string,::pool_stat_t> *result,
    bool *per_pool);
  int get_fs_stats(ceph_statfs& result);
  int pool_is_in_selfmanaged_snaps_mode(const std::string& pool);

  /*
  -1 was set as the default value and monitor will pickup the right crush rule with below order:
    a) osd pool default crush replicated rule
    b) the first rule
    c) error out if no value find
  */
  int pool_create(std::string& name, int16_t crush_rule=-1);
  int pool_create_async(std::string& name, PoolAsyncCompletionImpl *c,
			int16_t crush_rule=-1);
  int pool_get_base_tier(int64_t pool_id, int64_t* base_tier);
  int pool_delete(const char *name);

  int pool_delete_async(const char *name, PoolAsyncCompletionImpl *c);

  int blocklist_add(const std::string& client_address, uint32_t expire_seconds);

  int mon_command(const std::vector<std::string>& cmd, const bufferlist &inbl,
	          bufferlist *outbl, std::string *outs);
  void mon_command_async(const std::vector<std::string>& cmd, const bufferlist &inbl,
                         bufferlist *outbl, std::string *outs, Context *on_finish);
  int mon_command(int rank,
		  const std::vector<std::string>& cmd, const bufferlist &inbl,
	          bufferlist *outbl, std::string *outs);
  int mon_command(std::string name,
		  const std::vector<std::string>& cmd, const bufferlist &inbl,
	          bufferlist *outbl, std::string *outs);
  int mgr_command(const std::vector<std::string>& cmd, const bufferlist &inbl,
	          bufferlist *outbl, std::string *outs);
  int mgr_command(
    const std::string& name,
    const std::vector<std::string>& cmd, const bufferlist &inbl,
    bufferlist *outbl, std::string *outs);
  int osd_command(int osd, std::vector<std::string>& cmd, const bufferlist& inbl,
                  bufferlist *poutbl, std::string *prs);
  int pg_command(pg_t pgid, std::vector<std::string>& cmd, const bufferlist& inbl,
	         bufferlist *poutbl, std::string *prs);

  void handle_log(MLog *m);
  int monitor_log(const std::string& level, rados_log_callback_t cb,
		  rados_log_callback2_t cb2, void *arg);

  void get();
  bool put();
  void blocklist_self(bool set);

  std::string get_addrs() const;

  int service_daemon_register(
    const std::string& service,  ///< service name (e.g., 'rgw')
    const std::string& name,     ///< daemon name (e.g., 'gwfoo')
    const std::map<std::string,std::string>& metadata); ///< static metadata about daemon
  int service_daemon_update_status(
    std::map<std::string,std::string>&& status);

  mon_feature_t get_required_monitor_features() const;

  int get_inconsistent_pgs(int64_t pool_id, std::vector<std::string>* pgs);
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override;
};

#endif
