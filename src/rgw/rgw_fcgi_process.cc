// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/errno.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"

#include "rgw_rados.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_loadgen.h"
#include "rgw_client_io.h"

#define dout_subsys ceph_subsys_rgw

void RGWFCGXProcess::run()
{
  string socket_path;
  string socket_port;
  string socket_host;
  int socket_backlog;

  conf->get_val("socket_path", "", &socket_path);
  conf->get_val("socket_port", g_conf->rgw_port, &socket_port);
  conf->get_val("socket_host", g_conf->rgw_host, &socket_host);
  socket_backlog = g_conf->rgw_fcgi_socket_backlog;

  if (socket_path.empty() && socket_port.empty() && socket_host.empty()) {
    socket_path = g_conf->rgw_socket_path;
    if (socket_path.empty()) {
      dout(0) << "ERROR: no socket server point defined, cannot "
	"start fcgi frontend" << dendl;
      return;
    }
  }

  if (!socket_path.empty()) {
    string path_str = socket_path;

    /* this is necessary, as FCGX_OpenSocket might not return an
	 * error, but rather ungracefully exit */
    int fd = open(path_str.c_str(), O_CREAT, 0644);
    if (fd < 0) {
      int err = errno;
      /* ENXIO is actually expected, we'll get that if we try to open
	   * a unix domain socket */
      if (err != ENXIO) {
		  dout(0) << "ERROR: cannot create socket: path=" << path_str
				  << " error=" << cpp_strerror(err) << dendl;
		  return;
      }
    } else {
		close(fd);
    }

    const char *path = path_str.c_str();
    sock_fd = FCGX_OpenSocket(path, socket_backlog);
    if (sock_fd < 0) {
      dout(0) << "ERROR: FCGX_OpenSocket (" << path << ") returned "
	      << sock_fd << dendl;
      return;
    }
    if (chmod(path, 0777) < 0) {
      dout(0) << "WARNING: couldn't set permissions on unix domain socket"
	      << dendl;
    }
  } else if (!socket_port.empty()) {
    string bind = socket_host + ":" + socket_port;
    sock_fd = FCGX_OpenSocket(bind.c_str(), socket_backlog);
    if (sock_fd < 0) {
      dout(0) << "ERROR: FCGX_OpenSocket (" << bind.c_str() << ") returned "
	      << sock_fd << dendl;
      return;
    }
  }

  m_tp.start();

  FCGX_Request fcgx_reqs[max_connections];

  QueueRing<FCGX_Request*> qr(max_connections);
  for (int i = 0; i < max_connections; i++) {
    FCGX_Request* fcgx = &fcgx_reqs[i];
    FCGX_InitRequest(fcgx, sock_fd, 0);
    qr.enqueue(fcgx);
  }

  for (;;) {
    RGWFCGXRequest* req = new RGWFCGXRequest(store->get_new_req_id(), &qr);
    dout(10) << "allocated request req=" << hex << req << dec << dendl;
    req_throttle.get(1);
    int ret = FCGX_Accept_r(req->fcgx);
    if (ret < 0) {
      delete req;
      dout(0) << "ERROR: FCGX_Accept_r returned " << ret << dendl;
      req_throttle.put(1);
      break;
    }
    req_wq.queue(req);
  }

  m_tp.drain(&req_wq);
  m_tp.stop();

  dout(20) << "cleaning up fcgx connections" << dendl;

  for (int i = 0; i < max_connections; i++) {
    FCGX_Finish_r(&fcgx_reqs[i]);
  }
} /* RGWFCGXProcess::run */

void RGWFCGXProcess::handle_request(RGWRequest* r)
{
  RGWFCGXRequest* req = static_cast<RGWFCGXRequest*>(r);
  FCGX_Request* fcgx = req->fcgx;
  RGWFCGX client_io(fcgx);

 
  int ret = process_request(store, rest, req, &client_io, olog);
  if (ret < 0) {
    /* we don't really care about return code */
    dout(20) << "process_request() returned " << ret << dendl;
  }

  FCGX_Finish_r(fcgx);

  delete req;
} /* RGWFCGXProcess::handle_request */
