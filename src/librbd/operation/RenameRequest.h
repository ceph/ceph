// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_RENAME_REQUEST_H
#define CEPH_LIBRBD_RENAME_REQUEST_H

#include "librbd/operation/Request.h"
#include <iosfwd>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class RenameRequest : public Request<ImageCtxT>
{
public:
  /**
   * Rename goes through the following state machine:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * STATE_READ_SOURCE_HEADER
   *    |
   *    v
   * STATE_WRITE_DEST_HEADER
   *    |
   *    v
   * STATE_UPDATE_DIRECTORY
   *    |
   *    v
   * STATE_REMOVE_SOURCE_HEADER
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   *
   */
  enum State {
    STATE_READ_SOURCE_HEADER,
    STATE_WRITE_DEST_HEADER,
    STATE_UPDATE_DIRECTORY,
    STATE_REMOVE_SOURCE_HEADER
  };

  RenameRequest(ImageCtxT &image_ctx, Context *on_finish,
                const std::string &dest_name);

protected:
  virtual void send_op();
  virtual bool should_complete(int r);

  virtual journal::Event create_event(uint64_t op_tid) const {
    return journal::RenameEvent(op_tid, m_dest_name);
  }

private:
  std::string m_dest_name;

  std::string m_source_oid;
  std::string m_dest_oid;

  State m_state;

  bufferlist m_header_bl;

  int filter_state_return_code(int r);

  void send_read_source_header();
  void send_write_destination_header();
  void send_update_directory();
  void send_remove_source_header();

  void apply();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::RenameRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_RENAME_REQUEST_H
