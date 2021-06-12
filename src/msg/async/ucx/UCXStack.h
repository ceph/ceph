// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright(c) 2021 Liu, Changcheng <changcheng.liu@aliyun.com>
 *
 */

#ifndef CEPH_MSG_UCXSTACK_H
#define CEPH_MSG_UCXSTACK_H

#include <sys/eventfd.h>
#include <ucp/api/ucp.h>
#include <ucs/datastruct/list.h>

#include <list>
#include <vector>
#include <thread>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "msg/msg_types.h"
#include "msg/async/Stack.h"

class UCXWorker;
class UCXConSktImpl;

typedef struct {
    ucp_conn_request_h conn_request;
    struct timeval     arrival_time;
} conn_req_t;


/*
 * UCX callback for send/receive completion
 */
class UcxCallback {
public:
    virtual ~UcxCallback();
    virtual void operator()(ucs_status_t status) = 0;
};

struct ucx_request {
    UcxCallback *callback;
    ucs_status_t status;
    bool completed;
    uint32_t conn_id;
    size_t recv_length;
    ucs_list_link_t pos;
};

class UCXProEngine {
private:
  CephContext *cct;
  ceph::mutex lock = ceph::make_mutex("UCXProEngine::lock");
  std::thread thread_engine;
  ucp_worker_h ucp_worker;
  std::map<uint64_t, UCXConSktImpl*> ucx_connections;
public:
  UCXProEngine(CephContext *cct, ucp_worker_h ucp_worker);

  void fire_polling();
  void progress();
  ucp_worker_h get_ucp_worker();
  void add_connections(uint64_t conn_id, UCXConSktImpl* ucx_con);

  static ucs_status_t
  am_recv_callback(void *arg, const void *header,
                   size_t header_length,
                   void *data, size_t length,
                   const ucp_am_recv_param_t *param);

};

class UCXConSktImpl : public ConnectedSocketImpl {
public:
  CephContext *cct;
private:
  UCXWorker* ucx_worker;
  std::shared_ptr<UCXProEngine> ucp_worker_engine;
  int connected = -1;
  int data_event_fd = -1;
  conn_req_t conn_request;
  uint64_t conn_id = -1;
  ucp_ep_h conn_ep = NULL;

  ceph::mutex lock = ceph::make_mutex("UCXConSktImpl::lock");
  bool is_server;
  bool active;
  bool pending;

  void handle_connection_error(ucs_status_t status);
  static
  void ep_error_cb(void *arg, ucp_ep_h ep, ucs_status_t status);

public:
  UCXConSktImpl(CephContext *cct, UCXWorker *ucx_worker,
                std::shared_ptr<UCXProEngine> ucp_worker_engine);
  ~UCXConSktImpl();

  int is_connected() override;
  ssize_t read(char* buf, size_t len) override;
  ssize_t send(ceph::bufferlist &bl, bool more) override;
  void shutdown() override;
  void close() override;
  int fd() const override;
  void set_conn_request(const conn_req_t &conn_request);
  ucs_status_t create_server_ep();
  void set_connection_status(int con_status);
  int client_start_connect(const entity_addr_t &server_addr, const SocketOptions &opts);
};

class UCXSerSktImpl : public ServerSocketImpl {
public:
  CephContext *cct;
private:
  ceph::NetHandler net;
  ucp_listener_h ucp_ser_listener = nullptr;
  int listen_skt_notify_fd = -1;
  UCXWorker *ucx_worker;
  std::shared_ptr<UCXProEngine> ucp_worker_engine;
  entity_addr_t listen_addr;
  std::deque<conn_req_t> conn_requests;

  const std::string sockaddr_str(const struct sockaddr* saddr, size_t addrlen);

public:
  UCXSerSktImpl(CephContext *cct, UCXWorker *ucx_worker,
                std::shared_ptr<UCXProEngine> ucp_worker_engine,
                entity_addr_t& listen_addr, unsigned addr_slot);

  int listen(const SocketOptions &skt_opts);

  int accept(ConnectedSocket *ser_con_socket, const SocketOptions &opts,
             entity_addr_t *peer_addr, Worker *ucx_worker) override;
  void abort_accept() override;
  int fd() const override;

  void listen_notify();
  static void recv_req_con_cb(ucp_conn_request_h conn_req, void *arg);
};

class UCXWorker : public Worker {
  std::shared_ptr<UCXProEngine> ucp_worker_engine;
  std::list<UCXConSktImpl*> pending_sent_conns;
  ceph::mutex lock = ceph::make_mutex("UCXWorker::lock");

public:
  explicit
  UCXWorker(CephContext *cct, unsigned worker_id,
            std::shared_ptr<UCXProEngine> ucp_worker_engine);
  ~UCXWorker();

  int listen(entity_addr_t &listen_addr, unsigned addr_slot,
             const SocketOptions &skt_opts, ServerSocket *ser_skt) override;
  int connect(const entity_addr_t &peer_addr,
              const SocketOptions &peer_opts,
              ConnectedSocket *peer_skt) override;
  void destroy() override;
  void initialize() override;

  void remove_pending_conn(UCXConSktImpl *remove_obj) {
    ceph_assert(center.in_thread());
    pending_sent_conns.remove(remove_obj);
  }
};

class UCXStack : public NetworkStack {
private:
  ucp_context_h ucp_ctx;
  std::shared_ptr<UCXProEngine> ucp_worker_engine;
  std::vector<std::thread> worker_threads;
  Worker* create_worker(CephContext *cct, unsigned worker_id) override;

public:
  explicit UCXStack(CephContext *cct);
  ~UCXStack();

  void spawn_worker(std::function<void ()> &&worker_func) override;
  void join_worker(unsigned idx) override;

  static void request_init(void *request);
  static void request_reset(ucx_request *r);
  static void request_release(void *request);

};

#endif //CEPH_MSG_UCXSTACK_H
