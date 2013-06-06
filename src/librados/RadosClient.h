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

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "mon/MonClient.h"
#include "msg/Dispatcher.h"
#include "osd/OSDMap.h"

#include "IoCtxImpl.h"

class AuthAuthorizer;
class CephContext;
class Connection;
struct md_config_t;
class Message;
class MWatchNotify;
class SimpleMessenger;

class librados::RadosClient : public Dispatcher
{
public:
  CephContext *cct;
  md_config_t *conf;
private:
  enum {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
  } state;

  OSDMap osdmap;
  MonClient monclient;
  SimpleMessenger *messenger;

  bool _dispatch(Message *m);
  bool ms_dispatch(Message *m);

  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);

  Objecter *objecter;

  Mutex lock;
  Cond cond;
  SafeTimer timer;
  int refcnt;

public:
  Finisher finisher;

  RadosClient(CephContext *cct_);
  ~RadosClient();
  int connect();
  void shutdown();

  uint64_t get_instance_id();

  int create_ioctx(const char *name, IoCtxImpl **io);

  int get_fsid(std::string *s);
  int64_t lookup_pool(const char *name);
  const char *get_pool_name(int64_t pool_id);
  int pool_get_auid(uint64_t pool_id, unsigned long long *auid);
  int pool_get_name(uint64_t pool_id, std::string *auid);

  int pool_list(std::list<string>& ls);
  int get_pool_stats(std::list<string>& ls, map<string,::pool_stat_t>& result);
  int get_fs_stats(ceph_statfs& result);

  int pool_create(string& name, unsigned long long auid=0, __u8 crush_rule=0);
  int pool_create_async(string& name, PoolAsyncCompletionImpl *c, unsigned long long auid=0,
			__u8 crush_rule=0);
  int pool_delete(const char *name);

  int pool_delete_async(const char *name, PoolAsyncCompletionImpl *c);

  // watch/notify
  uint64_t max_watch_cookie;
  map<uint64_t, librados::WatchContext *> watchers;

  void register_watcher(librados::WatchContext *wc, uint64_t *cookie);
  void unregister_watcher(uint64_t cookie);
  void watch_notify(MWatchNotify *m);
  void get();
  bool put();
  void blacklist_self(bool set);
};

#endif
