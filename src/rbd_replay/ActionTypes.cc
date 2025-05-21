// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rbd_replay/ActionTypes.h"
#include "include/ceph_assert.h"
#include "include/byteorder.h"
#include "include/stringify.h"
#include "common/Formatter.h"
#include <iostream>

namespace rbd_replay {
namespace action {

namespace {

bool byte_swap_required(__u8 version) {
#if defined(CEPH_LITTLE_ENDIAN)
  return (version == 0);
#else
  return false;
#endif
}

void decode_big_endian_string(std::string &str, bufferlist::const_iterator &it) {
  using ceph::decode;
#if defined(CEPH_LITTLE_ENDIAN)
  uint32_t length;
  decode(length, it);
  length = swab(length);
  str.clear();
  it.copy(length, str);
#else
  ceph_abort();
#endif
}

class EncodeVisitor {
public:
  explicit EncodeVisitor(bufferlist &bl) : m_bl(bl) {
  }

  template <typename Action>
  inline void operator()(const Action &action) const {
    using ceph::encode;
    encode(static_cast<uint8_t>(Action::ACTION_TYPE), m_bl);
    action.encode(m_bl);
  }
private:
  bufferlist &m_bl;
};

class DecodeVisitor {
public:
  DecodeVisitor(__u8 version, bufferlist::const_iterator &iter)
    : m_version(version), m_iter(iter) {
  }

  template <typename Action>
  inline void operator()(Action &action) const {
    action.decode(m_version, m_iter);
  }
private:
  __u8 m_version;
  bufferlist::const_iterator &m_iter;
};

class DumpVisitor {
public:
  explicit DumpVisitor(Formatter *formatter) : m_formatter(formatter) {}

  template <typename Action>
  inline void operator()(const Action &action) const {
    ActionType action_type = Action::ACTION_TYPE;
    m_formatter->dump_string("action_type", stringify(action_type));
    action.dump(m_formatter);
  }
private:
  ceph::Formatter *m_formatter;
};

} // anonymous namespace

void Dependency::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(id, bl);
  encode(time_delta, bl);
}

void Dependency::decode(bufferlist::const_iterator &it) {
  decode(1, it);
}

void Dependency::decode(__u8 version, bufferlist::const_iterator &it) {
  using ceph::decode;
  decode(id, it);
  decode(time_delta, it);
  if (byte_swap_required(version)) {
    id = swab(id);
    time_delta = swab(time_delta);
  }
}

void Dependency::dump(Formatter *f) const {
  f->dump_unsigned("id", id);
  f->dump_unsigned("time_delta", time_delta);
}

void Dependency::generate_test_instances(std::list<Dependency *> &o) {
  o.push_back(new Dependency());
  o.push_back(new Dependency(1, 123456789));
}

void ActionBase::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(id, bl);
  encode(thread_id, bl);
  encode(dependencies, bl);
}

void ActionBase::decode(__u8 version, bufferlist::const_iterator &it) {
  using ceph::decode;
  decode(id, it);
  decode(thread_id, it);
  if (version == 0) {
    uint32_t num_successors;
    decode(num_successors, it);

    uint32_t num_completion_successors;
    decode(num_completion_successors, it);
  }

  if (byte_swap_required(version)) {
    id = swab(id);
    thread_id = swab(thread_id);

    uint32_t dep_count;
    decode(dep_count, it);
    dep_count = swab(dep_count);
    dependencies.resize(dep_count);
    for (uint32_t i = 0; i < dep_count; ++i) {
      dependencies[i].decode(0, it);
    }
  } else {
    decode(dependencies, it);
  }
}

void ActionBase::dump(Formatter *f) const {
  f->dump_unsigned("id", id);
  f->dump_unsigned("thread_id", thread_id);
  f->open_array_section("dependencies");
  for (size_t i = 0; i < dependencies.size(); ++i) {
    f->open_object_section("dependency");
    dependencies[i].dump(f);
    f->close_section();
  }
  f->close_section();
}

void ImageActionBase::encode(bufferlist &bl) const {
  using ceph::encode;
  ActionBase::encode(bl);
  encode(imagectx_id, bl);
}

void ImageActionBase::decode(__u8 version, bufferlist::const_iterator &it) {
  using ceph::decode;
  ActionBase::decode(version, it);
  decode(imagectx_id, it);
  if (byte_swap_required(version)) {
    imagectx_id = swab(imagectx_id);
  }
}

void ImageActionBase::dump(Formatter *f) const {
  ActionBase::dump(f);
  f->dump_unsigned("imagectx_id", imagectx_id);
}

void IoActionBase::encode(bufferlist &bl) const {
  using ceph::encode;
  ImageActionBase::encode(bl);
  encode(offset, bl);
  encode(length, bl);
}

void IoActionBase::decode(__u8 version, bufferlist::const_iterator &it) {
  using ceph::decode;
  ImageActionBase::decode(version, it);
  decode(offset, it);
  decode(length, it);
  if (byte_swap_required(version)) {
    offset = swab(offset);
    length = swab(length);
  }
}

void IoActionBase::dump(Formatter *f) const {
  ImageActionBase::dump(f);
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

void OpenImageAction::encode(bufferlist &bl) const {
  using ceph::encode;
  ImageActionBase::encode(bl);
  encode(name, bl);
  encode(snap_name, bl);
  encode(read_only, bl);
}

void OpenImageAction::decode(__u8 version, bufferlist::const_iterator &it) {
  using ceph::decode;
  ImageActionBase::decode(version, it);
  if (byte_swap_required(version)) {
    decode_big_endian_string(name, it);
    decode_big_endian_string(snap_name, it);
  } else {
    decode(name, it);
    decode(snap_name, it);
  }
  decode(read_only, it);
}

void OpenImageAction::dump(Formatter *f) const {
  ImageActionBase::dump(f);
  f->dump_string("name", name);
  f->dump_string("snap_name", snap_name);
  f->dump_bool("read_only", read_only);
}

void AioOpenImageAction::encode(bufferlist &bl) const {
  using ceph::encode;
  ImageActionBase::encode(bl);
  encode(name, bl);
  encode(snap_name, bl);
  encode(read_only, bl);
}

void AioOpenImageAction::decode(__u8 version, bufferlist::const_iterator &it) {
  using ceph::decode;
  ImageActionBase::decode(version, it);
  if (byte_swap_required(version)) {
    decode_big_endian_string(name, it);
    decode_big_endian_string(snap_name, it);
  } else {
    decode(name, it);
    decode(snap_name, it);
  }
  decode(read_only, it);
}

void AioOpenImageAction::dump(Formatter *f) const {
  ImageActionBase::dump(f);
  f->dump_string("name", name);
  f->dump_string("snap_name", snap_name);
  f->dump_bool("read_only", read_only);
}

void UnknownAction::encode(bufferlist &bl) const {
  ceph_abort();
}

void UnknownAction::decode(__u8 version, bufferlist::const_iterator &it) {
}

void UnknownAction::dump(Formatter *f) const {
}

void ActionEntry::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  std::visit(EncodeVisitor(bl), action);
  ENCODE_FINISH(bl);
}

void ActionEntry::decode(bufferlist::const_iterator &it) {
  DECODE_START(1, it);
  decode_versioned(struct_v, it);
  DECODE_FINISH(it);
}

void ActionEntry::decode_unversioned(bufferlist::const_iterator &it) {
  decode_versioned(0, it);
}

void ActionEntry::decode_versioned(__u8 version, bufferlist::const_iterator &it) {
  using ceph::decode;
  uint8_t action_type;
  decode(action_type, it);

  // select the correct action variant based upon the action_type
  switch (action_type) {
  case ACTION_TYPE_START_THREAD:
    action = StartThreadAction();
    break;
  case ACTION_TYPE_STOP_THREAD:
    action = StopThreadAction();
    break;
  case ACTION_TYPE_READ:
    action = ReadAction();
    break;
  case ACTION_TYPE_WRITE:
    action = WriteAction();
    break;
  case ACTION_TYPE_DISCARD:
    action = DiscardAction();
    break;
  case ACTION_TYPE_AIO_READ:
    action = AioReadAction();
    break;
  case ACTION_TYPE_AIO_WRITE:
    action = AioWriteAction();
    break;
  case ACTION_TYPE_AIO_DISCARD:
    action = AioDiscardAction();
    break;
  case ACTION_TYPE_OPEN_IMAGE:
    action = OpenImageAction();
    break;
  case ACTION_TYPE_CLOSE_IMAGE:
    action = CloseImageAction();
    break;
  case ACTION_TYPE_AIO_OPEN_IMAGE:
    action = AioOpenImageAction();
    break;
  case ACTION_TYPE_AIO_CLOSE_IMAGE:
    action = AioCloseImageAction();
    break;
  }

  std::visit(DecodeVisitor(version, it), action);
}

void ActionEntry::dump(Formatter *f) const {
  std::visit(DumpVisitor(f), action);
}

void ActionEntry::generate_test_instances(std::list<ActionEntry *> &o) {
  Dependencies dependencies;
  dependencies.push_back(Dependency(3, 123456789));
  dependencies.push_back(Dependency(4, 234567890));

  o.push_back(new ActionEntry(StartThreadAction()));
  o.push_back(new ActionEntry(StartThreadAction(1, 123456789, dependencies)));
  o.push_back(new ActionEntry(StopThreadAction()));
  o.push_back(new ActionEntry(StopThreadAction(1, 123456789, dependencies)));

  o.push_back(new ActionEntry(ReadAction()));
  o.push_back(new ActionEntry(ReadAction(1, 123456789, dependencies, 3, 4, 5)));
  o.push_back(new ActionEntry(WriteAction()));
  o.push_back(new ActionEntry(WriteAction(1, 123456789, dependencies, 3, 4,
                                          5)));
  o.push_back(new ActionEntry(DiscardAction()));
  o.push_back(new ActionEntry(DiscardAction(1, 123456789, dependencies, 3, 4,
                                            5)));
  o.push_back(new ActionEntry(AioReadAction()));
  o.push_back(new ActionEntry(AioReadAction(1, 123456789, dependencies, 3, 4,
                                            5)));
  o.push_back(new ActionEntry(AioWriteAction()));
  o.push_back(new ActionEntry(AioWriteAction(1, 123456789, dependencies, 3, 4,
                                             5)));
  o.push_back(new ActionEntry(AioDiscardAction()));
  o.push_back(new ActionEntry(AioDiscardAction(1, 123456789, dependencies, 3, 4,
                                               5)));

  o.push_back(new ActionEntry(OpenImageAction()));
  o.push_back(new ActionEntry(OpenImageAction(1, 123456789, dependencies, 3,
                                              "image_name", "snap_name",
                                              true)));
  o.push_back(new ActionEntry(CloseImageAction()));
  o.push_back(new ActionEntry(CloseImageAction(1, 123456789, dependencies, 3)));

  o.push_back(new ActionEntry(AioOpenImageAction()));
  o.push_back(new ActionEntry(AioOpenImageAction(1, 123456789, dependencies, 3,
                                              "image_name", "snap_name",
                                              true)));
  o.push_back(new ActionEntry(AioCloseImageAction()));
  o.push_back(new ActionEntry(AioCloseImageAction(1, 123456789, dependencies, 3)));
}

std::ostream &operator<<(std::ostream &out,
                         const ActionType &type) {
  using namespace rbd_replay::action;

  switch (type) {
  case ACTION_TYPE_START_THREAD:
    out << "StartThread";
    break;
  case ACTION_TYPE_STOP_THREAD:
    out << "StopThread";
    break;
  case ACTION_TYPE_READ:
    out << "Read";
    break;
  case ACTION_TYPE_WRITE:
    out << "Write";
    break;
  case ACTION_TYPE_DISCARD:
    out << "Discard";
    break;
  case ACTION_TYPE_AIO_READ:
    out << "AioRead";
    break;
  case ACTION_TYPE_AIO_WRITE:
    out << "AioWrite";
    break;
  case ACTION_TYPE_AIO_DISCARD:
    out << "AioDiscard";
    break;
  case ACTION_TYPE_OPEN_IMAGE:
    out << "OpenImage";
    break;
  case ACTION_TYPE_CLOSE_IMAGE:
    out << "CloseImage";
    break;
  case ACTION_TYPE_AIO_OPEN_IMAGE:
    out << "AioOpenImage";
    break;
  case ACTION_TYPE_AIO_CLOSE_IMAGE:
    out << "AioCloseImage";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(type) << ")";
    break;
  }
  return out;
}

} // namespace action
} // namespace rbd_replay
