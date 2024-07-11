#include "RDMAStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " RDMAIWARPConnectedSocketImpl "

#define TIMEOUT_MS 3000
#define RETRY_COUNT 7

RDMAIWARPConnectedSocketImpl::RDMAIWARPConnectedSocketImpl(CephContext *cct, std::shared_ptr<Infiniband>& ib,
                                                           std::shared_ptr<RDMADispatcher>& rdma_dispatcher,
                                                           RDMAWorker *w, RDMACMInfo *info)
  : RDMAConnectedSocketImpl(cct, ib, rdma_dispatcher, w), cm_con_handler(new C_handle_cm_connection(this))
{
  status = IDLE;
  notify_fd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);
  if (info) {
    is_server = true;
    cm_id = info->cm_id;
    cm_channel = info->cm_channel;
    status = RDMA_ID_CREATED;
    peer_qpn = info->qp_num;
    if (alloc_resource()) {
      close_notify();
      return;
    }
    worker->center.submit_to(worker->center.get_id(), [this]() {
      worker->center.create_file_event(cm_channel->fd, EVENT_READABLE, cm_con_handler);
      status = CHANNEL_FD_CREATED;
    }, false);
    status = RESOURCE_ALLOCATED;
    qp->get_local_cm_meta().peer_qpn = peer_qpn;
    qp->get_peer_cm_meta().local_qpn = peer_qpn;
  } else {
    is_server = false;
    cm_channel = rdma_create_event_channel();
    rdma_create_id(cm_channel, &cm_id, NULL, RDMA_PS_TCP);
    status = RDMA_ID_CREATED;
    ldout(cct, 20) << __func__ << " successfully created cm id: " << cm_id << dendl;
  }
}

RDMAIWARPConnectedSocketImpl::~RDMAIWARPConnectedSocketImpl() {
  ldout(cct, 20) << __func__ << " destruct." << dendl;
  std::unique_lock l(close_mtx);
  close_condition.wait(l, [&] { return closed; });
  if (status >= RDMA_ID_CREATED) {
    rdma_destroy_id(cm_id);
    rdma_destroy_event_channel(cm_channel);
  }
}

int RDMAIWARPConnectedSocketImpl::try_connect(const entity_addr_t& peer_addr, const SocketOptions &opts) {
  worker->center.create_file_event(cm_channel->fd, EVENT_READABLE, cm_con_handler);
  status = CHANNEL_FD_CREATED;
  if (rdma_resolve_addr(cm_id, NULL, const_cast<struct sockaddr*>(peer_addr.get_sockaddr()), TIMEOUT_MS)) {
    lderr(cct) << __func__ << " failed to resolve addr" << dendl;
    return -1;
  }
  return 0;
}

void RDMAIWARPConnectedSocketImpl::close() {
  error = ECONNRESET;
  active = false;
  if (status >= CONNECTED) {
    rdma_disconnect(cm_id);
  }
  close_notify();
}

void RDMAIWARPConnectedSocketImpl::shutdown() {
  error = ECONNRESET;
  active = false;
}

void RDMAIWARPConnectedSocketImpl::handle_cm_connection() {
  struct rdma_cm_event *event;
  rdma_get_cm_event(cm_channel, &event);
  ldout(cct, 20) << __func__ << " event name: " << rdma_event_str(event->event)
                             << " (cm id: " << cm_id << ")" << dendl;
  struct rdma_conn_param cm_params;
  switch (event->event) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
      status = ADDR_RESOLVED;
      if (rdma_resolve_route(cm_id, TIMEOUT_MS)) {
        lderr(cct) << __func__ << " failed to resolve rdma addr" << dendl;
        notify();
      }
      break;

    case RDMA_CM_EVENT_ROUTE_RESOLVED:
      status = ROUTE_RESOLVED;
      if (alloc_resource()) {
        lderr(cct) << __func__ << " failed to alloc resource while resolving the route" << dendl;
        connected = -ECONNREFUSED;
        notify();
        break;
      }

      // FIPS zeroization audit 20191115: this memset is not security related.
      memset(&cm_params, 0, sizeof(cm_params));
      cm_params.retry_count = RETRY_COUNT;
      cm_params.qp_num = local_qpn;
      if (rdma_connect(cm_id, &cm_params)) {
        lderr(cct) << __func__ << " failed to connect remote rdma port" << dendl;
        connected = -ECONNREFUSED;
        notify();
      }
      break;

    case RDMA_CM_EVENT_ESTABLISHED:
      ldout(cct, 20) << __func__ << " qp_num=" << cm_id->qp->qp_num << dendl;
      status = CONNECTED;
      if (!is_server) {
        peer_qpn = event->param.conn.qp_num;
        activate();
        qp->get_local_cm_meta().peer_qpn = peer_qpn;
        qp->get_peer_cm_meta().local_qpn = peer_qpn;
        notify();
      }
      break;

    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_UNREACHABLE:
    case RDMA_CM_EVENT_REJECTED:
      lderr(cct) << __func__ << " rdma connection rejected" << dendl;
      connected = -ECONNREFUSED;
      notify();
      break;

    case RDMA_CM_EVENT_DISCONNECTED:
      status = DISCONNECTED;
      close_notify();
      if (!error) {
        error = ECONNRESET;
        notify();
      }
      break;

    case RDMA_CM_EVENT_DEVICE_REMOVAL:
      break;

    default:
      ceph_abort_msg("unhandled event");
      break;
  }
  rdma_ack_cm_event(event);
}

void RDMAIWARPConnectedSocketImpl::activate() {
  ldout(cct, 30) << __func__ << dendl;
  active = true;
  connected = 1;
}

int RDMAIWARPConnectedSocketImpl::alloc_resource() {
  ldout(cct, 30) << __func__ << dendl;
  qp = ib->create_queue_pair(cct, dispatcher->get_tx_cq(),
      dispatcher->get_rx_cq(), IBV_QPT_RC, cm_id);
  if (!qp) {
    return -1;
  }
  local_qpn = qp->get_local_qp_number();
  dispatcher->register_qp(qp, this);
  dispatcher->perf_logger->inc(l_msgr_rdma_created_queue_pair);
  dispatcher->perf_logger->inc(l_msgr_rdma_active_queue_pair);
  return 0;
}

void RDMAIWARPConnectedSocketImpl::close_notify() {
  ldout(cct, 30) << __func__ << dendl;
  if (status >= CHANNEL_FD_CREATED) {
    worker->center.delete_file_event(cm_channel->fd, EVENT_READABLE);
  }
  std::unique_lock l(close_mtx);
  if (!closed) {
    closed = true;
    close_condition.notify_all();
  }
}
