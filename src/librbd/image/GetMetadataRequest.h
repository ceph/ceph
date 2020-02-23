// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_GET_METADATA_REQUEST_H
#define CEPH_LIBRBD_IMAGE_GET_METADATA_REQUEST_H

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include <string>
#include <map>

class CephContext;
class Context;

namespace librbd {

struct ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class GetMetadataRequest {
public:
  typedef std::map<std::string, bufferlist> KeyValues;

  static GetMetadataRequest* create(
      IoCtx &io_ctx, const std::string &oid, const std::string& filter,
      const std::string& last_key, uint32_t max_results, KeyValues* key_values,
      Context *on_finish) {
    return new GetMetadataRequest(io_ctx, oid, filter, last_key, max_results,
                                  key_values, on_finish);
  }

  GetMetadataRequest(
      IoCtx &io_ctx, const std::string &oid, const std::string& filter,
      const std::string& last_key, uint32_t max_results, KeyValues* key_values,
      Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |     /-------\
   *    |     |       |
   *    v     v       |
   * METADATA_LIST ---/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  librados::IoCtx m_io_ctx;
  std::string m_oid;
  std::string m_filter;
  std::string m_last_key;
  uint32_t m_max_results;
  KeyValues* m_key_values;
  Context* m_on_finish;

  CephContext* m_cct;
  bufferlist m_out_bl;
  uint32_t m_expected_results = 0;

  void metadata_list();
  void handle_metadata_list(int r);

  void finish(int r);

};

} //namespace image
} //namespace librbd

extern template class librbd::image::GetMetadataRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_GET_METADATA_REQUEST_H
