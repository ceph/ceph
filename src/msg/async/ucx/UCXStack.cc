// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright(c) 2021 Liu, Changcheng <changcheng.liu@aliyun.com>
 *
 */

#include <poll.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "include/str_list.h"
#include "include/compat.h"
#include "common/Cycles.h"
#include "common/deleter.h"
#include "UCXStack.h"

#define FN_NAME (__CEPH_ASSERT_FUNCTION == nullptr ? __func__ : __CEPH_ASSERT_FUNCTION)
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ucx_msg
#undef dout_prefix
#define dout_prefix _prefix(_dout, FN_NAME)
static ostream& _prefix(std::ostream *_dout,
                        std::string_view func_name)
{
  return *_dout << "UCXStack: " << func_name << ": ";
}

UCXWorker::UCXWorker(CephContext *cct, unsigned worker_id,
                     std::shared_ptr<UCXProEngine> ucp_worker_engine)
  : Worker(cct, worker_id), ucp_worker_engine(ucp_worker_engine)
{
  ldout(cct, 20) << "Creating UCXWorker: " << worker_id << dendl;
}

UCXWorker::~UCXWorker()
{
}

int UCXWorker::listen(entity_addr_t &listen_addr, unsigned addr_slot,
                      const SocketOptions &ser_opts, ServerSocket *ser_skt)
{
  int rst = 0;
  ucp_worker_engine->fire_polling();

  auto ucx_serskt = new UCXSerSktImpl(cct, this, ucp_worker_engine, listen_addr, addr_slot);
  rst = ucx_serskt->listen(ser_opts);
  if (rst < 0) {
    lderr(cct) << "listen" << listen_addr << " failed." << dendl;
    delete ucx_serskt;
    return rst;
  }

  *ser_skt = ServerSocket(std::unique_ptr<ServerSocketImpl>(ucx_serskt));
  return 0;
}

int UCXWorker::connect(const entity_addr_t &peer_addr,
                       const SocketOptions &peer_opts,
                       ConnectedSocket *peer_skt)
{
  int rst = 0;
  ucp_worker_engine->fire_polling();

  auto ucx_peerskt = new UCXConSktImpl(cct, this, ucp_worker_engine);
  rst = ucx_peerskt->client_start_connect(peer_addr, peer_opts);

  if (rst < 0) {
    lderr(cct) << "connect" << peer_addr << " failed." << dendl;
    delete ucx_peerskt;
    return rst;
  }

  ucx_peerskt->active = true;
  *peer_skt = ConnectedSocket(std::unique_ptr<UCXConSktImpl>(ucx_peerskt));

  return rst;
}

void UCXWorker::destroy()
{
}

void UCXWorker::initialize()
{
  ceph_assert(ucp_worker_engine != nullptr);
}

UCXProEngine::UCXProEngine(CephContext *cct, ucp_worker_h ucp_worker)
  : cct(cct), ucp_worker(ucp_worker)
{
}

UCXProEngine::~UCXProEngine(void)
{
  ldout(cct, 20) << __LINE__ << dendl;
  engine_status = -1;
  thread_engine.join();
  ldout(cct, 20) << __LINE__ << dendl;
}

void UCXProEngine::fire_polling()
{
  std::lock_guard lk{lock};

  if (thread_engine.joinable()) {
    return;
  }

  thread_engine = std::thread(&UCXProEngine::progress, this);
  ceph_pthread_setname(thread_engine.native_handle(), "ucx-progress");
}

void UCXProEngine::progress()
{
  ldout(cct, 20) << " ucp_worker_progress start " << dendl;
  engine_status = 1;
  while (true) {
    if (engine_status != 1) {
      break;
    }
    ucp_worker_progress(ucp_worker);
  }
}

ucp_worker_h UCXProEngine::get_ucp_worker()
{
  return ucp_worker;
}

void UCXProEngine::add_connections(uint64_t conn_id, UCXConSktImpl* ucx_con)
{
  ucx_connections[conn_id] = ucx_con;
}

wait_status_t
UCXProEngine::wait_completion(ucs_status_ptr_t status_ptr, double timeout)
{
    if (status_ptr == NULL) {
        return WAIT_STATUS_OK;
    } else if (UCS_PTR_IS_PTR(status_ptr)) {
        ucx_request *request = (ucx_request*)UCS_STATUS_PTR(status_ptr);
        ucs_status_t status;
        struct timeval tv_start;
        gettimeofday(&tv_start, NULL);
        do {
            struct timeval tv_current, elapsed;
            gettimeofday(&tv_current, NULL);
            timersub(&tv_current, &tv_start, &elapsed);
            if (elapsed.tv_sec + (elapsed.tv_usec * 1e-6) > timeout) {
                return WAIT_STATUS_TIMED_OUT;
            }
            ucp_worker_progress(ucp_worker);
            status = ucp_request_check_status(request);
        } while (status == UCS_INPROGRESS);
        return (status == UCS_OK) ? WAIT_STATUS_OK : WAIT_STATUS_FAILED;
    } else {
        assert(UCS_PTR_IS_ERR(status_ptr));
        return WAIT_STATUS_FAILED;
    }
}

ucs_status_t
UCXProEngine::am_recv_callback(void *arg, const void *header,
                               size_t header_length,
                               void *data, size_t length,
                               const ucp_am_recv_param_t *param)
{
  // TODO: really need to solve this problem:
  //       store all connections into UCXProEngine
  UCXProEngine *self_this = reinterpret_cast<UCXProEngine*>(arg);

  ceph_assert(param->recv_attr & UCP_AM_RECV_ATTR_FIELD_REPLY_EP);

  uint64_t conn_id = reinterpret_cast<uint64_t>(param->reply_ep);
  auto iter = self_this->ucx_connections.find(conn_id);
  if (iter == self_this->ucx_connections.end()) {
    lderr(self_this->cct) << "can not find connection with ep " << param->reply_ep
                          << "(" << conn_id << ")"
			  << dendl;
  }
  auto ucx_con = iter->second;
  self_this->dispatch_am_message(ucx_con, header, header_length, data, param);

  return UCS_OK;
}

void UCXProEngine::dispatch_am_message(UCXConSktImpl *ucx_con,
                         const void *header, size_t header_length,
                         void *data, const ucp_am_recv_param_t *param)
{
  const iomsg_t *msg = reinterpret_cast<const iomsg_t*>(header);
  ldout(cct, 15) << "got io (AM) message " << io_op_names[msg->op_code] << ", "
                 << "sn " << msg->sn << ", "
		 << "data size " << msg->data_size
		 << dendl;
  ceph_assert(msg->op_code == IO_WRITE);
  ucx_con->handle_io_am_write_request(msg, data, param);
}


UCXStack::UCXStack(CephContext *cct)
  : NetworkStack(cct)
{
  ldout(cct, 20) << "creating UCXStack: " << this << dendl;

  ucs_status_t status = UCS_OK;
  /* Create context */
  ucp_params_t ucp_params;
  memset(&ucp_params, 0, sizeof(ucp_params));
  ucp_params.field_mask   = UCP_PARAM_FIELD_FEATURES |
                            UCP_PARAM_FIELD_REQUEST_INIT |
                            UCP_PARAM_FIELD_REQUEST_SIZE;
  ucp_params.features     = UCP_FEATURE_AM; // TODO: refine wakeup
  ucp_params.request_init = request_init;
  ucp_params.request_size = sizeof(ucx_request);
  status = ucp_init(&ucp_params, NULL, &ucp_ctx);
  if (status != UCS_OK) {
    lderr(cct) << "ucp_init() failed: " << ucs_status_string(status) << dendl;
  }

  /* Create worker */
  ucp_worker_h ucp_worker;
  ucp_worker_params_t worker_params;
  memset(&worker_params, 0, sizeof(worker_params));
  worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  worker_params.thread_mode = UCS_THREAD_MODE_MULTI;
  status = ucp_worker_create(ucp_ctx, &worker_params, &ucp_worker);
  if (status != UCS_OK) {
    lderr(cct) << "ucp_worker_create() failed: " << ucs_status_string(status) << dendl;
  }

  ucp_worker_engine = std::make_shared<UCXProEngine>(cct, ucp_worker);

  ucp_am_handler_param_t param;
  param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                     UCP_AM_HANDLER_PARAM_FIELD_CB |
                     UCP_AM_HANDLER_PARAM_FIELD_ARG;
  param.id         = 0xcafebeef;
  param.cb         = UCXProEngine::am_recv_callback;
  param.arg        = ucp_worker_engine.get();
  ceph_assert(UCS_OK == ucp_worker_set_am_recv_handler(ucp_worker, &param));
}

UCXStack::~UCXStack()
{
}

Worker* UCXStack::create_worker(CephContext *cct, unsigned worker_id)
{
  auto ucx_worker = new UCXWorker(cct, worker_id, ucp_worker_engine);
  return ucx_worker;
}

void UCXStack::spawn_worker(std::function<void ()> &&worker_func)
{
  worker_threads.emplace_back(std::move(worker_func));
}

void UCXStack::join_worker(unsigned idx)
{
  ceph_assert(worker_threads.size() > idx && worker_threads[idx].joinable());
  worker_threads[idx].join();
}

void UCXStack::request_init(void *request)
{
  ucx_request *req = reinterpret_cast<ucx_request*>(request);
  request_reset(req);
}

void UCXStack::request_reset(ucx_request *req)
{
  req->completed   = false;
  req->callback    = NULL;
  req->status      = UCS_OK;
  req->recv_length = 0;
  req->pos.next    = NULL;
  req->pos.prev    = NULL;
}

void UCXStack::request_release(void *request)
{
  ucx_request *req = reinterpret_cast<ucx_request*>(request);
  request_reset(req);

  ucp_request_free(request);
}
