// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/RenameRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"

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
  case RenameRequest<I>::STATE_READ_DIRECTORY:
    os << "READ_DIRECTORY";
    break;
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
    m_source_oid(image_ctx.old_format ? util::old_header_name(image_ctx.name) :
                                        util::id_obj_name(image_ctx.name)),
    m_dest_oid(image_ctx.old_format ? util::old_header_name(dest_name) :
                                      util::id_obj_name(dest_name)) {
}

template <typename I>
void RenameRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  if (image_ctx.old_format) {
    send_read_source_header();
    return;
  }
  send_read_directory();
}

template <typename I>
bool RenameRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  r = filter_return_code(r);
  if (r < 0) {
    if (r == -EEXIST) {
      ldout(cct, 1) << "image already exists" << dendl;
    } else {
      lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
    }
    return true;
  }

  if (m_state == STATE_READ_DIRECTORY) {
    std::string name;
    auto it = m_source_name_bl.cbegin();
    r = cls_client::dir_get_name_finish(&it, &name);
    if (r < 0) {
      lderr(cct) << "could not read directory: " << cpp_strerror(r) << dendl;
      return true;
    }
    bool update = false;
    {
      std::shared_lock image_locker{image_ctx.image_lock};
      update = image_ctx.name != name;
    }
    if (update) {
      image_ctx.set_image_name(name);
      m_source_oid = util::id_obj_name(name);
    }
  } else if (m_state == STATE_UPDATE_DIRECTORY) {
    // update in-memory name before removing source header
    apply();
  } else if (m_state == STATE_REMOVE_SOURCE_HEADER) {
    return true;
  }

  std::shared_lock owner_lock{image_ctx.owner_lock};
  switch (m_state) {
  case STATE_READ_DIRECTORY:
    send_read_source_header();
    break;
  case STATE_READ_SOURCE_HEADER:
    send_write_destination_header();
    break;
  case STATE_WRITE_DEST_HEADER:
    send_update_directory();
    break;
  case STATE_UPDATE_DIRECTORY:
    send_remove_source_header();
    break;
  default:
    ceph_abort();
    break;
  }
  return false;
}

template <typename I>
int RenameRequest<I>::filter_return_code(int r) const {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (m_state == STATE_READ_SOURCE_HEADER && r == -ENOENT) {
    std::shared_lock image_locker{image_ctx.image_lock};
    if (image_ctx.name == m_dest_name) {
      // signal that replay raced with itself
      return -EEXIST;
    }
  } else if (m_state == STATE_REMOVE_SOURCE_HEADER && r < 0) {
    if (r != -ENOENT) {
      lderr(cct) << "warning: couldn't remove old source object ("
                 << m_source_oid << ")" << dendl;
    }
    return 0;
  }
  return r;
}

template <typename I>
void RenameRequest<I>::send_read_directory() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;
  m_state = STATE_READ_DIRECTORY;

  librados::ObjectReadOperation op;
  cls_client::dir_get_name_start(&op, image_ctx.id);

  auto comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(RBD_DIRECTORY, comp, &op,
                                       &m_source_name_bl);
  ceph_assert(r == 0);
  comp->release();
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
  ceph_assert(r == 0);
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
  ceph_assert(r == 0);
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
    encode(static_cast<__u8>(CEPH_OSD_TMAP_SET), cmd_bl);
    encode(m_dest_name, cmd_bl);
    encode(empty_bl, cmd_bl);
    encode(static_cast<__u8>(CEPH_OSD_TMAP_RM), cmd_bl);
    encode(image_ctx.name, cmd_bl);
    op.tmap_update(cmd_bl);
  } else {
    cls_client::dir_rename_image(&op, image_ctx.name, m_dest_name,
                                 image_ctx.id);
  }

  librados::AioCompletion *rados_completion = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(RBD_DIRECTORY, rados_completion, &op);
  ceph_assert(r == 0);
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
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
void RenameRequest<I>::apply() {
  I &image_ctx = this->m_image_ctx;
  image_ctx.set_image_name(m_dest_name);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::RenameRequest<librbd::ImageCtx>;
