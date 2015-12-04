// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/RenameRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::operation::RenameRequest: "

namespace librbd {
namespace operation {

namespace {

template <typename I>
std::ostream& operator<<(std::ostream& os,
                         const typename RenameRequest<I>::State& state) {
  switch(state) {
  case RenameRequest<I>::STATE_READ_SOURCE_HEADER:
    os << "READ_SOURCE_HEADER";
    break;
  case RenameRequest<I>::STATE_WRITE_DEST_HEADER:
    os << "WRITE_DEST_HEADER";
    break;
  case RenameRequest<I>::STATE_UPDATE_DIRECTORY:
    os << "UPDATE_DIRECTORY";
    break;
  case RenameRequest<I>::STATE_REMOVE_SOURCE_HEADER:
    os << "REMOVE_SOURCE_HEADER";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

} // anonymous namespace

template <typename I>
RenameRequest<I>::RenameRequest(I &image_ctx, Context *on_finish,
				const std::string &dest_name)
  : Request<I>(image_ctx, on_finish), m_dest_name(dest_name),
    m_source_oid(image_ctx.old_format ? old_header_name(image_ctx.name) :
                                        id_obj_name(image_ctx.name)),
    m_dest_oid(image_ctx.old_format ? old_header_name(dest_name) :
                                      id_obj_name(dest_name)) {
}

template <typename I>
void RenameRequest<I>::send_op() {
  send_read_source_header();
}

template <typename I>
bool RenameRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  r = filter_state_return_code(r);
  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
    return true;
  }

  RWLock::RLocker owner_lock(image_ctx.owner_lock);
  bool finished = false;
  switch (m_state) {
  case STATE_READ_SOURCE_HEADER:
    send_write_destination_header();
    break;
  case STATE_WRITE_DEST_HEADER:
    send_update_directory();
    break;
  case STATE_UPDATE_DIRECTORY:
    send_remove_source_header();
    break;
  case STATE_REMOVE_SOURCE_HEADER:
    finished = true;
    break;
  default:
    assert(false);
    break;
  }
  return finished;
}

template <typename I>
int RenameRequest<I>::filter_state_return_code(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (m_state == STATE_REMOVE_SOURCE_HEADER && r < 0) {
    if (r != -ENOENT) {
      lderr(cct) << "warning: couldn't remove old source object ("
                 << m_source_oid << ")" << dendl;
    }
    return 0;
  }
  return r;
}

template <typename I>
void RenameRequest<I>::send_read_source_header() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_READ_SOURCE_HEADER;

  librados::ObjectReadOperation op;
  op.read(0, 0, NULL, NULL);

  // TODO: old code read omap values but there are no omap values on the
  //       old format header nor the new format id object
  librados::AioCompletion *rados_completion = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(m_source_oid, rados_completion, &op,
                                       &m_header_bl);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void RenameRequest<I>::send_write_destination_header() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_WRITE_DEST_HEADER;

  librados::ObjectWriteOperation op;
  op.create(true);
  op.write_full(m_header_bl);

  librados::AioCompletion *rados_completion = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(m_dest_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void RenameRequest<I>::send_update_directory() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_UPDATE_DIRECTORY;

  librados::ObjectWriteOperation op;
  if (image_ctx.old_format) {
    bufferlist cmd_bl;
    bufferlist empty_bl;
    ::encode(static_cast<__u8>(CEPH_OSD_TMAP_SET), cmd_bl);
    ::encode(m_dest_name, cmd_bl);
    ::encode(empty_bl, cmd_bl);
    ::encode(static_cast<__u8>(CEPH_OSD_TMAP_RM), cmd_bl);
    ::encode(image_ctx.name, cmd_bl);
    op.tmap_update(cmd_bl);
  } else {
    cls_client::dir_rename_image(&op, image_ctx.name, m_dest_name,
                                 image_ctx.id);
  }

  librados::AioCompletion *rados_completion = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(RBD_DIRECTORY, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void RenameRequest<I>::send_remove_source_header() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_REMOVE_SOURCE_HEADER;

  librados::ObjectWriteOperation op;
  op.remove();

  librados::AioCompletion *rados_completion = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(m_source_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

} // namespace operation
} // namespace librbd

template class librbd::operation::RenameRequest<librbd::ImageCtx>;
