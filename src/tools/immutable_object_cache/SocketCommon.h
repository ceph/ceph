// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_SOCKET_COMMON_H
#define CEPH_CACHE_SOCKET_COMMON_H

namespace ceph {
namespace immutable_obj_cache {

static const int RBDSC_REGISTER        =  0X11;
static const int RBDSC_READ            =  0X12;
static const int RBDSC_REGISTER_REPLY  =  0X13;
static const int RBDSC_READ_REPLY      =  0X14;
static const int RBDSC_READ_RADOS      =  0X15;

static const int ASIO_ERROR_READ = 0X01;
static const int ASIO_ERROR_WRITE = 0X02;
static const int ASIO_ERROR_CONNECT = 0X03;
static const int ASIO_ERROR_ACCEPT = 0X04;
static const int ASIO_ERROR_MSG_INCOMPLETE = 0X05;

class ObjectCacheRequest;
class CacheSession;

typedef std::function<void(CacheSession*, ObjectCacheRequest*)> ProcessMsg;

}  // namespace immutable_obj_cache
}  // namespace ceph
#endif  // CEPH_CACHE_SOCKET_COMMON_H
