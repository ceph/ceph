// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdlib.h>

#include "common/debug.h"
#include "common/errno.h"
#include "Driver.h"
#include "Request.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd::ggate::Driver: " << this \
                           << " " << __func__ << ": "

namespace rbd {
namespace ggate {

int Driver::load() {

  return ggate_drv_load();
}

int Driver::kill(const std::string &devname) {

  int r = ggate_drv_kill(devname.c_str());

  return r;
}

int Driver::list(std::map<std::string, DevInfo> *devices) {
  size_t size = 1024;
  ggate_drv_info *devs = nullptr;
  int r;

  while (size <= 1024 * 1024) {
    devs = static_cast<ggate_drv_info *>(
        realloc(static_cast<void *>(devs), size * sizeof(*devs)));
    r = ggate_drv_list(devs, &size);
    if (r != -ERANGE) {
      break;
    }
  }
  if (r < 0) {
    goto free;
  }

  devices->clear();
  for (size_t i = 0; i < size; i++) {
    auto &dev = devs[i];
    (*devices)[dev.id] = {dev.name, dev.info};
  }

free:
  free(devs);

  return r;
}

Driver::Driver(const std::string &devname, size_t sectorsize, size_t mediasize,
               bool readonly, const std::string &info)
  : m_devname(devname), m_sectorsize(sectorsize), m_mediasize(mediasize),
    m_readonly(readonly), m_info(info) {
}

int Driver::init() {
  dout(20) << dendl;

  char name[PATH_MAX];
  size_t namelen;

  if (m_devname.empty()) {
    name[0] = '\0';
    namelen = PATH_MAX;
  } else {
    namelen = m_devname.size();
    if (namelen >= PATH_MAX) {
      return -ENAMETOOLONG;
    }
    strncpy(name, m_devname.c_str(), namelen + 1);
  }

  int r = ggate_drv_create(name, namelen, m_sectorsize, m_mediasize, m_readonly,
                           m_info.c_str(), &m_drv);
  if (r < 0) {
    return r;
  }

  if (m_devname.empty()) {
    m_devname = name;
  }

  return 0;
}

std::string Driver::get_devname() const {
  dout(30) << m_devname << dendl;

  return m_devname;
}

void Driver::shut_down() {
  dout(20) << dendl;

  ggate_drv_destroy(m_drv);
}

int Driver::resize(size_t newsize) {
  dout(20) << "newsize=" << newsize << dendl;

  int r = ggate_drv_resize(m_drv, newsize);
  if (r < 0) {
    return r;
  }

  m_mediasize = newsize;
  return 0;
}

int Driver::recv(Request **req) {
  dout(20) << dendl;

  ggate_drv_req_t req_;

  int r = ggate_drv_recv(m_drv, &req_);
  if (r < 0) {
    return r;
  }

  *req = new Request(req_);

  dout(20) << "req=" << *req << dendl;

  if (ggate_drv_req_cmd(req_) == GGATE_DRV_CMD_WRITE) {
    bufferptr ptr(buffer::claim_malloc(
                    ggate_drv_req_length(req_),
                    static_cast<char *>(ggate_drv_req_release_buf(req_))));
    (*req)->bl.push_back(ptr);
  }

  return 0;
}

int Driver::send(Request *req) {
  dout(20) << "req=" << req << dendl;

  if (ggate_drv_req_cmd(req->req) == GGATE_DRV_CMD_READ &&
      ggate_drv_req_error(req->req) == 0) {
    assert(req->bl.length() == ggate_drv_req_length(req->req));
    // TODO: avoid copying?
    req->bl.copy(0, ggate_drv_req_length(req->req),
                 static_cast<char *>(ggate_drv_req_buf(req->req)));
    dout(20) << "copied resulting " << req->bl.length() << " bytes to "
             << ggate_drv_req_buf(req->req) << dendl;
  }

  int r = ggate_drv_send(m_drv, req->req);

  delete req;
  return r;
}

} // namespace ggate
} // namespace rbd
