// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_GGATE_DRIVER_H
#define CEPH_RBD_GGATE_DRIVER_H

#include <map>
#include <string>

#include "ggate_drv.h"

namespace rbd {
namespace ggate {

struct Request;

class Driver {
public:
  typedef std::pair<std::string, std::string> DevInfo;
  static int load();
  static int kill(const std::string &devname);
  static int list(std::map<std::string, DevInfo> *devices);

  Driver(const std::string &devname, size_t sectorsize, size_t mediasize,
         bool readonly, const std::string &info);

  int init();
  void shut_down();

  std::string get_devname() const;

  int recv(Request **req);
  int send(Request *req);

  int resize(size_t newsize);

private:
  std::string m_devname;
  size_t m_sectorsize;
  size_t m_mediasize;
  bool m_readonly;
  std::string m_info;
  ggate_drv_t m_drv = 0;
};

} // namespace ggate
} // namespace rbd

#endif // CEPH_RBD_GGATE_DRIVER_H

