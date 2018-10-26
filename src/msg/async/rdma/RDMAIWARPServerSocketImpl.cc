#include <poll.h>

#include "msg/async/net_handler.h"
#include "RDMAStack.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " RDMAIWARPServerSocketImpl "

RDMAIWARPServerSocketImpl::RDMAIWARPServerSocketImpl(
  CephContext *cct, Infiniband* i,
  RDMADispatcher *s, RDMAWorker *w, entity_addr_t& a, unsigned addr_slot)
  : RDMAServerSocketImpl(cct, i, s, w, a, addr_slot)
{
}

int RDMAIWARPServerSocketImpl::listen(entity_addr_t &sa,
				      const SocketOptions &opt)
{
  ldout(cct, 20) << __func__ << " bind to rdma point" << dendl;
  cm_channel = rdma_create_event_channel();
  rdma_create_id(cm_channel, &cm_id, NULL, RDMA_PS_TCP);
  ldout(cct, 20) << __func__ << " successfully created cm id: " << cm_id << dendl;
  int rc = rdma_bind_addr(cm_id, const_cast<struct sockaddr*>(sa.get_sockaddr()));
  if (rc < 0) {
    rc = -errno;
    ldout(cct, 10) << __func__ << " unable to bind to " << sa.get_sockaddr()
                   << " on port " << sa.get_port() << ": " << cpp_strerror(errno) << dendl;
    goto err;
  }
  rc = rdma_listen(cm_id, 128);
  if (rc < 0) {
    rc = -errno;
    ldout(cct, 10) << __func__ << " unable to listen to " << sa.get_sockaddr()
                   << " on port " << sa.get_port() << ": " << cpp_strerror(errno) << dendl;
    goto err;
  }
  server_setup_socket = cm_channel->fd;
  ldout(cct, 20) << __func__ << " fd of cm_channel is " << server_setup_socket << dendl;
  return 0;

err:
  server_setup_socket = -1;
  rdma_destroy_id(cm_id);
  rdma_destroy_event_channel(cm_channel);
  return rc;
}

int RDMAIWARPServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt,
    entity_addr_t *out, Worker *w)
{
  ldout(cct, 15) << __func__ << dendl;

  ceph_assert(sock);
  struct pollfd pfd = {
    .fd = cm_channel->fd,
    .events = POLLIN,
  };
  int ret = poll(&pfd, 1, 0);
  ceph_assert(ret >= 0);
  if (!ret)
    return -EAGAIN;

  struct rdma_cm_event *cm_event;
  rdma_get_cm_event(cm_channel, &cm_event);
  ldout(cct, 20) << __func__ << " event name: " << rdma_event_str(cm_event->event) << dendl;

  struct rdma_cm_id *event_cm_id = cm_event->id;
  struct rdma_event_channel *event_channel = rdma_create_event_channel();

  rdma_migrate_id(event_cm_id, event_channel);

  struct rdma_cm_id *new_cm_id = event_cm_id;
  struct rdma_conn_param *remote_conn_param = &cm_event->param.conn;
  struct rdma_conn_param local_conn_param;

  RDMACMInfo info(new_cm_id, event_channel, remote_conn_param->qp_num);
  RDMAIWARPConnectedSocketImpl* server =
    new RDMAIWARPConnectedSocketImpl(cct, infiniband, dispatcher, dynamic_cast<RDMAWorker*>(w), &info);

  memset(&local_conn_param, 0, sizeof(local_conn_param));
  local_conn_param.qp_num = server->get_local_qpn();

  if (rdma_accept(new_cm_id, &local_conn_param)) {
    return -EAGAIN;
  }
  server->activate();
  ldout(cct, 20) << __func__ << " accepted a new QP" << dendl;

  rdma_ack_cm_event(cm_event);

  std::unique_ptr<RDMAConnectedSocketImpl> csi(server);
  *sock = ConnectedSocket(std::move(csi));
  struct sockaddr *addr = &new_cm_id->route.addr.dst_addr;
  out->set_sockaddr(addr);

  return 0;
}

void RDMAIWARPServerSocketImpl::abort_accept()
{
  if (server_setup_socket >= 0) {
    rdma_destroy_id(cm_id);
    rdma_destroy_event_channel(cm_channel);
  }
}
