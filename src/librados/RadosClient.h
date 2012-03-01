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
#include "osdc/Objecter.h"

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

public:

  RadosClient(CephContext *cct_);
  ~RadosClient();
  int connect();
  void shutdown();

  int64_t lookup_pool(const char *name);
  const char *get_pool_name(int64_t poolid_);
  ::ObjectOperation *prepare_assert_ops(IoCtxImpl *io, ::ObjectOperation *op);

  // snaps
  int snap_list(IoCtxImpl *io, vector<uint64_t> *snaps);
  int snap_lookup(IoCtxImpl *io, const char *name, uint64_t *snapid);
  int snap_get_name(IoCtxImpl *io, uint64_t snapid, std::string *s);
  int snap_get_stamp(IoCtxImpl *io, uint64_t snapid, time_t *t);
  int snap_create(rados_ioctx_t io, const char* snapname);
  int selfmanaged_snap_create(rados_ioctx_t io, uint64_t *snapid);
  int snap_remove(rados_ioctx_t io, const char* snapname);
  int rollback(rados_ioctx_t io_, const object_t& oid, const char *snapName);
  int selfmanaged_snap_remove(rados_ioctx_t io, uint64_t snapid);
  int selfmanaged_snap_rollback_object(rados_ioctx_t io, const object_t& oid,
                                       ::SnapContext& snapc, uint64_t snapid);

  // io
  int create(IoCtxImpl& io, const object_t& oid, bool exclusive);
  int create(IoCtxImpl& io, const object_t& oid, bool exclusive, const std::string& category);
  int write(IoCtxImpl& io, const object_t& oid, bufferlist& bl, size_t len, uint64_t off);
  int append(IoCtxImpl& io, const object_t& oid, bufferlist& bl, size_t len);
  int write_full(IoCtxImpl& io, const object_t& oid, bufferlist& bl);
  int clone_range(IoCtxImpl& io, const object_t& dst_oid, uint64_t dst_offset,
                  const object_t& src_oid, uint64_t src_offset, uint64_t len);
  int read(IoCtxImpl& io, const object_t& oid, bufferlist& bl, size_t len, uint64_t off);
  int mapext(IoCtxImpl& io, const object_t& oid, uint64_t off, size_t len,
	     std::map<uint64_t,uint64_t>& m);
  int sparse_read(IoCtxImpl& io, const object_t& oid, std::map<uint64_t,uint64_t>& m,
		  bufferlist& bl, size_t len, uint64_t off);
  int remove(IoCtxImpl& io, const object_t& oid);
  int stat(IoCtxImpl& io, const object_t& oid, uint64_t *psize, time_t *pmtime);
  int trunc(IoCtxImpl& io, const object_t& oid, uint64_t size);

  int tmap_update(IoCtxImpl& io, const object_t& oid, bufferlist& cmdbl);
  int tmap_put(IoCtxImpl& io, const object_t& oid, bufferlist& bl);
  int tmap_get(IoCtxImpl& io, const object_t& oid, bufferlist& bl);

  int exec(IoCtxImpl& io, const object_t& oid, const char *cls, const char *method, bufferlist& inbl, bufferlist& outbl);

  int getxattr(IoCtxImpl& io, const object_t& oid, const char *name, bufferlist& bl);
  int setxattr(IoCtxImpl& io, const object_t& oid, const char *name, bufferlist& bl);
  int getxattrs(IoCtxImpl& io, const object_t& oid, map<string, bufferlist>& attrset);
  int rmxattr(IoCtxImpl& io, const object_t& oid, const char *name);

  int pool_list(std::list<string>& ls);
  int get_pool_stats(std::list<string>& ls, map<string,::pool_stat_t>& result);
  int get_fs_stats(ceph_statfs& result);

  int pool_create(string& name, unsigned long long auid=0, __u8 crush_rule=0);
  int pool_create_async(string& name, PoolAsyncCompletionImpl *c, unsigned long long auid=0,
			__u8 crush_rule=0);
  int pool_delete(const char *name);
  int pool_change_auid(rados_ioctx_t io, unsigned long long auid);
  int pool_get_auid(rados_ioctx_t io, unsigned long long *auid);

  int pool_delete_async(const char *name, PoolAsyncCompletionImpl *c);
  int pool_change_auid_async(rados_ioctx_t io, unsigned long long auid, PoolAsyncCompletionImpl *c);

  int list(Objecter::ListContext *context, int max_entries);

  int operate(IoCtxImpl& io, const object_t& oid, ::ObjectOperation *o, time_t *pmtime);
  int operate_read(IoCtxImpl& io, const object_t& oid, ::ObjectOperation *o, bufferlist *pbl);
  int aio_operate(IoCtxImpl& io, const object_t& oid, ::ObjectOperation *o, AioCompletionImpl *c);
  int aio_operate_read(IoCtxImpl& io, const object_t& oid, ::ObjectOperation *o, AioCompletionImpl *c, bufferlist *pbl);

  struct C_aio_Ack : public Context {
    librados::AioCompletionImpl *c;
    C_aio_Ack(AioCompletionImpl *_c);
    void finish(int r);
  };

  struct C_aio_sparse_read_Ack : public Context {
    AioCompletionImpl *c;
    bufferlist *data_bl;
    std::map<uint64_t,uint64_t> *m;
    C_aio_sparse_read_Ack(AioCompletionImpl *_c);
    void finish(int r);
  };

  struct C_aio_Safe : public Context {
    AioCompletionImpl *c;
    C_aio_Safe(AioCompletionImpl *_c);
    void finish(int r);
  };

  int aio_read(IoCtxImpl& io, const object_t oid, AioCompletionImpl *c,
			  bufferlist *pbl, size_t len, uint64_t off);
  int aio_read(IoCtxImpl& io, object_t oid, AioCompletionImpl *c,
	       char *buf, size_t len, uint64_t off);
  int aio_sparse_read(IoCtxImpl& io, const object_t oid,
		    AioCompletionImpl *c, std::map<uint64_t,uint64_t> *m,
		    bufferlist *data_bl, size_t len, uint64_t off);
  int aio_write(IoCtxImpl& io, const object_t &oid, AioCompletionImpl *c,
		const bufferlist& bl, size_t len, uint64_t off);
  int aio_append(IoCtxImpl& io, const object_t &oid, AioCompletionImpl *c,
		 const bufferlist& bl, size_t len);
  int aio_write_full(IoCtxImpl& io, const object_t &oid, AioCompletionImpl *c,
		     const bufferlist& bl);
  int aio_exec(IoCtxImpl& io, const object_t& oid, AioCompletionImpl *c,
               const char *cls, const char *method, bufferlist& inbl, bufferlist *outbl);

  struct C_PoolAsync_Safe : public Context {
    PoolAsyncCompletionImpl *c;
    C_PoolAsync_Safe(PoolAsyncCompletionImpl *_c);
    void finish(int r);
  };

  static PoolAsyncCompletionImpl *pool_async_create_completion();
  static PoolAsyncCompletionImpl *pool_async_create_completion(void *cb_arg,
							       rados_callback_t cb);
  static AioCompletionImpl *aio_create_completion();
  static AioCompletionImpl *aio_create_completion(void *cb_arg,
						  rados_callback_t cb_complete,
						  rados_callback_t cb_safe);

  // watch/notify
  struct WatchContext {
    IoCtxImpl *io_ctx_impl;
    const object_t oid;
    uint64_t cookie;
    uint64_t ver;
    librados::WatchCtx *ctx;
    uint64_t linger_id;

    WatchContext(IoCtxImpl *io_ctx_impl_,
		 const object_t& _oc,
		 librados::WatchCtx *_ctx);
    ~WatchContext();
    void notify(RadosClient *client, MWatchNotify *m);
  };

  struct C_NotifyComplete : public librados::WatchCtx {
    Mutex *lock;
    Cond *cond;
    bool *done;

    C_NotifyComplete(Mutex *_l, Cond *_c, bool *_d);
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl);
  };

  uint64_t max_watch_cookie;
  map<uint64_t, WatchContext *> watchers;

  void set_sync_op_version(IoCtxImpl& io, eversion_t& ver);

  void register_watcher(IoCtxImpl& io, const object_t& oid,
			librados::WatchCtx *ctx, uint64_t *cookie,
			WatchContext **pwc = NULL);
  void unregister_watcher(uint64_t cookie);
  void watch_notify(MWatchNotify *m);
  int watch(IoCtxImpl& io, const object_t& oid, uint64_t ver, uint64_t *cookie, librados::WatchCtx *ctx);
  int unwatch(IoCtxImpl& io, const object_t& oid, uint64_t cookie);
  int notify(IoCtxImpl& io, const object_t& oid, uint64_t ver, bufferlist& bl);
  int _notify_ack(IoCtxImpl& io, const object_t& oid, uint64_t notify_id, uint64_t ver);

  eversion_t last_version(IoCtxImpl& io);
  void set_assert_version(IoCtxImpl& io, uint64_t ver);
  void set_assert_src_version(IoCtxImpl& io, const object_t& oid, uint64_t ver);
  void set_notify_timeout(IoCtxImpl& io, uint32_t timeout);
};

#endif
