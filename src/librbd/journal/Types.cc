// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/Types.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "include/types.h"
#include "common/Formatter.h"

namespace librbd {
namespace journal {

namespace {

template <typename E>
class GetTypeVisitor : public boost::static_visitor<E> {
public:
  template <typename T>
  inline E operator()(const T&) const {
    return T::TYPE;
  }
};

class EncodeVisitor : public boost::static_visitor<void> {
public:
  explicit EncodeVisitor(bufferlist &bl) : m_bl(bl) {
  }

  template <typename T>
  inline void operator()(const T& t) const {
    ::encode(static_cast<uint32_t>(T::TYPE), m_bl);
    t.encode(m_bl);
  }
private:
  bufferlist &m_bl;
};

class DecodeVisitor : public boost::static_visitor<void> {
public:
  DecodeVisitor(__u8 version, bufferlist::iterator &iter)
    : m_version(version), m_iter(iter) {
  }

  template <typename T>
  inline void operator()(T& t) const {
    t.decode(m_version, m_iter);
  }
private:
  __u8 m_version;
  bufferlist::iterator &m_iter;
};

class DumpVisitor : public boost::static_visitor<void> {
public:
  explicit DumpVisitor(Formatter *formatter, const std::string &key)
    : m_formatter(formatter), m_key(key) {}

  template <typename T>
  inline void operator()(const T& t) const {
    auto type = T::TYPE;
    m_formatter->dump_string(m_key.c_str(), stringify(type));
    t.dump(m_formatter);
  }
private:
  ceph::Formatter *m_formatter;
  std::string m_key;
};

} // anonymous namespace

void AioDiscardEvent::encode(bufferlist& bl) const {
  ::encode(offset, bl);
  ::encode(length, bl);
}

void AioDiscardEvent::decode(__u8 version, bufferlist::iterator& it) {
  ::decode(offset, it);
  ::decode(length, it);
}

void AioDiscardEvent::dump(Formatter *f) const {
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

void AioWriteEvent::encode(bufferlist& bl) const {
  ::encode(offset, bl);
  ::encode(length, bl);
  ::encode(data, bl);
}

void AioWriteEvent::decode(__u8 version, bufferlist::iterator& it) {
  ::decode(offset, it);
  ::decode(length, it);
  ::decode(data, it);
}

void AioWriteEvent::dump(Formatter *f) const {
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

void AioFlushEvent::encode(bufferlist& bl) const {
}

void AioFlushEvent::decode(__u8 version, bufferlist::iterator& it) {
}

void AioFlushEvent::dump(Formatter *f) const {
}

void OpEventBase::encode(bufferlist& bl) const {
  ::encode(op_tid, bl);
}

void OpEventBase::decode(__u8 version, bufferlist::iterator& it) {
  ::decode(op_tid, it);
}

void OpEventBase::dump(Formatter *f) const {
  f->dump_unsigned("op_tid", op_tid);
}

void OpFinishEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  ::encode(op_tid, bl);
  ::encode(r, bl);
}

void OpFinishEvent::decode(__u8 version, bufferlist::iterator& it) {
  OpEventBase::decode(version, it);
  ::decode(op_tid, it);
  ::decode(r, it);
}

void OpFinishEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_unsigned("op_tid", op_tid);
  f->dump_int("result", r);
}

void SnapEventBase::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  ::encode(snap_name, bl);
}

void SnapEventBase::decode(__u8 version, bufferlist::iterator& it) {
  OpEventBase::decode(version, it);
  ::decode(snap_name, it);
}

void SnapEventBase::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_string("snap_name", snap_name);
}

void SnapLimitEvent::encode(bufferlist &bl) const {
  OpEventBase::encode(bl);
  ::encode(limit, bl);
}

void SnapLimitEvent::decode(__u8 version, bufferlist::iterator& it) {
  OpEventBase::decode(version, it);
  ::decode(limit, it);
}

void SnapLimitEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_unsigned("limit", limit);
}

void SnapRenameEvent::encode(bufferlist& bl) const {
  SnapEventBase::encode(bl);
  ::encode(snap_id, bl);
}

void SnapRenameEvent::decode(__u8 version, bufferlist::iterator& it) {
  SnapEventBase::decode(version, it);
  ::decode(snap_id, it);
}

void SnapRenameEvent::dump(Formatter *f) const {
  SnapEventBase::dump(f);
  f->dump_unsigned("src_snap_id", snap_id);
  f->dump_string("dest_snap_name", snap_name);
}

void RenameEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  ::encode(image_name, bl);
}

void RenameEvent::decode(__u8 version, bufferlist::iterator& it) {
  OpEventBase::decode(version, it);
  ::decode(image_name, it);
}

void RenameEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_string("image_name", image_name);
}

void ResizeEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  ::encode(size, bl);
}

void ResizeEvent::decode(__u8 version, bufferlist::iterator& it) {
  OpEventBase::decode(version, it);
  ::decode(size, it);
}

void ResizeEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_unsigned("size", size);
}

void DemoteEvent::encode(bufferlist& bl) const {
}

void DemoteEvent::decode(__u8 version, bufferlist::iterator& it) {
}

void DemoteEvent::dump(Formatter *f) const {
}

void UnknownEvent::encode(bufferlist& bl) const {
  assert(false);
}

void UnknownEvent::decode(__u8 version, bufferlist::iterator& it) {
}

void UnknownEvent::dump(Formatter *f) const {
}

EventType EventEntry::get_event_type() const {
  return boost::apply_visitor(GetTypeVisitor<EventType>(), event);
}

void EventEntry::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  boost::apply_visitor(EncodeVisitor(bl), event);
  ENCODE_FINISH(bl);
}

void EventEntry::decode(bufferlist::iterator& it) {
  DECODE_START(1, it);

  uint32_t event_type;
  ::decode(event_type, it);

  // select the correct payload variant based upon the encoded op
  switch (event_type) {
  case EVENT_TYPE_AIO_DISCARD:
    event = AioDiscardEvent();
    break;
  case EVENT_TYPE_AIO_WRITE:
    event = AioWriteEvent();
    break;
  case EVENT_TYPE_AIO_FLUSH:
    event = AioFlushEvent();
    break;
  case EVENT_TYPE_OP_FINISH:
    event = OpFinishEvent();
    break;
  case EVENT_TYPE_SNAP_CREATE:
    event = SnapCreateEvent();
    break;
  case EVENT_TYPE_SNAP_REMOVE:
    event = SnapRemoveEvent();
    break;
  case EVENT_TYPE_SNAP_RENAME:
    event = SnapRenameEvent();
    break;
  case EVENT_TYPE_SNAP_PROTECT:
    event = SnapProtectEvent();
    break;
  case EVENT_TYPE_SNAP_UNPROTECT:
    event = SnapUnprotectEvent();
    break;
  case EVENT_TYPE_SNAP_ROLLBACK:
    event = SnapRollbackEvent();
    break;
  case EVENT_TYPE_RENAME:
    event = RenameEvent();
    break;
  case EVENT_TYPE_RESIZE:
    event = ResizeEvent();
    break;
  case EVENT_TYPE_FLATTEN:
    event = FlattenEvent();
    break;
  case EVENT_TYPE_DEMOTE:
    event = DemoteEvent();
    break;
  default:
    event = UnknownEvent();
    break;
  }

  boost::apply_visitor(DecodeVisitor(struct_v, it), event);
  DECODE_FINISH(it);
}

void EventEntry::dump(Formatter *f) const {
  boost::apply_visitor(DumpVisitor(f, "event_type"), event);
}

void EventEntry::generate_test_instances(std::list<EventEntry *> &o) {
  o.push_back(new EventEntry(AioDiscardEvent()));
  o.push_back(new EventEntry(AioDiscardEvent(123, 345)));

  bufferlist bl;
  bl.append(std::string(32, '1'));
  o.push_back(new EventEntry(AioWriteEvent()));
  o.push_back(new EventEntry(AioWriteEvent(123, 456, bl)));

  o.push_back(new EventEntry(AioFlushEvent()));

  o.push_back(new EventEntry(OpFinishEvent(123, -1)));

  o.push_back(new EventEntry(SnapCreateEvent()));
  o.push_back(new EventEntry(SnapCreateEvent(234, "snap")));

  o.push_back(new EventEntry(SnapRemoveEvent()));
  o.push_back(new EventEntry(SnapRemoveEvent(345, "snap")));

  o.push_back(new EventEntry(SnapRenameEvent()));
  o.push_back(new EventEntry(SnapRenameEvent(456, 1, "snap")));

  o.push_back(new EventEntry(SnapProtectEvent()));
  o.push_back(new EventEntry(SnapProtectEvent(567, "snap")));

  o.push_back(new EventEntry(SnapUnprotectEvent()));
  o.push_back(new EventEntry(SnapUnprotectEvent(678, "snap")));

  o.push_back(new EventEntry(SnapRollbackEvent()));
  o.push_back(new EventEntry(SnapRollbackEvent(789, "snap")));

  o.push_back(new EventEntry(RenameEvent()));
  o.push_back(new EventEntry(RenameEvent(890, "image name")));

  o.push_back(new EventEntry(ResizeEvent()));
  o.push_back(new EventEntry(ResizeEvent(901, 1234)));

  o.push_back(new EventEntry(FlattenEvent(123)));

  o.push_back(new EventEntry(DemoteEvent()));
}

// Journal Client

void ImageClientMeta::encode(bufferlist& bl) const {
  ::encode(tag_class, bl);
  ::encode(resync_requested, bl);
}

void ImageClientMeta::decode(__u8 version, bufferlist::iterator& it) {
  ::decode(tag_class, it);
  ::decode(resync_requested, it);
}

void ImageClientMeta::dump(Formatter *f) const {
  f->dump_unsigned("tag_class", tag_class);
  f->dump_bool("resync_requested", resync_requested);
}

void MirrorPeerSyncPoint::encode(bufferlist& bl) const {
  ::encode(snap_name, bl);
  ::encode(from_snap_name, bl);
  ::encode(object_number, bl);
}

void MirrorPeerSyncPoint::decode(__u8 version, bufferlist::iterator& it) {
  ::decode(snap_name, it);
  ::decode(from_snap_name, it);
  ::decode(object_number, it);
}

void MirrorPeerSyncPoint::dump(Formatter *f) const {
  f->dump_string("snap_name", snap_name);
  f->dump_string("from_snap_name", from_snap_name);
  if (object_number) {
    f->dump_unsigned("object_number", *object_number);
  }
}

void MirrorPeerClientMeta::encode(bufferlist& bl) const {
  ::encode(image_id, bl);
  ::encode(static_cast<uint32_t>(state), bl);
  ::encode(sync_object_count, bl);
  ::encode(static_cast<uint32_t>(sync_points.size()), bl);
  for (auto &sync_point : sync_points) {
    sync_point.encode(bl);
  }
  ::encode(snap_seqs, bl);
}

void MirrorPeerClientMeta::decode(__u8 version, bufferlist::iterator& it) {
  ::decode(image_id, it);

  uint32_t decode_state;
  ::decode(decode_state, it);
  state = static_cast<MirrorPeerState>(decode_state);

  ::decode(sync_object_count, it);

  uint32_t sync_point_count;
  ::decode(sync_point_count, it);
  sync_points.resize(sync_point_count);
  for (auto &sync_point : sync_points) {
    sync_point.decode(version, it);
  }

  ::decode(snap_seqs, it);
}

void MirrorPeerClientMeta::dump(Formatter *f) const {
  f->dump_string("image_id", image_id);
  f->dump_stream("state") << state;
  f->dump_unsigned("sync_object_count", sync_object_count);
  f->open_array_section("sync_points");
  for (auto &sync_point : sync_points) {
    f->open_object_section("sync_point");
    sync_point.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("snap_seqs");
  for (auto &pair : snap_seqs) {
    f->open_object_section("snap_seq");
    f->dump_unsigned("local_snap_seq", pair.first);
    f->dump_unsigned("peer_snap_seq", pair.second);
    f->close_section();
  }
  f->close_section();
}

void CliClientMeta::encode(bufferlist& bl) const {
}

void CliClientMeta::decode(__u8 version, bufferlist::iterator& it) {
}

void CliClientMeta::dump(Formatter *f) const {
}

void UnknownClientMeta::encode(bufferlist& bl) const {
  assert(false);
}

void UnknownClientMeta::decode(__u8 version, bufferlist::iterator& it) {
}

void UnknownClientMeta::dump(Formatter *f) const {
}

ClientMetaType ClientData::get_client_meta_type() const {
  return boost::apply_visitor(GetTypeVisitor<ClientMetaType>(), client_meta);
}

void ClientData::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  boost::apply_visitor(EncodeVisitor(bl), client_meta);
  ENCODE_FINISH(bl);
}

void ClientData::decode(bufferlist::iterator& it) {
  DECODE_START(1, it);

  uint32_t client_meta_type;
  ::decode(client_meta_type, it);

  // select the correct payload variant based upon the encoded op
  switch (client_meta_type) {
  case IMAGE_CLIENT_META_TYPE:
    client_meta = ImageClientMeta();
    break;
  case MIRROR_PEER_CLIENT_META_TYPE:
    client_meta = MirrorPeerClientMeta();
    break;
  case CLI_CLIENT_META_TYPE:
    client_meta = CliClientMeta();
    break;
  default:
    client_meta = UnknownClientMeta();
    break;
  }

  boost::apply_visitor(DecodeVisitor(struct_v, it), client_meta);
  DECODE_FINISH(it);
}

void ClientData::dump(Formatter *f) const {
  boost::apply_visitor(DumpVisitor(f, "client_meta_type"), client_meta);
}

void ClientData::generate_test_instances(std::list<ClientData *> &o) {
  o.push_back(new ClientData(ImageClientMeta()));
  o.push_back(new ClientData(ImageClientMeta(123)));
  o.push_back(new ClientData(MirrorPeerClientMeta()));
  o.push_back(new ClientData(MirrorPeerClientMeta("image_id",
                                                  {{"snap 2", "snap 1", 123}},
                                                  {{1, 2}, {3, 4}})));
  o.push_back(new ClientData(CliClientMeta()));
}

// Journal Tag

void TagData::encode(bufferlist& bl) const {
  ::encode(mirror_uuid, bl);
  ::encode(predecessor_mirror_uuid, bl);
  ::encode(predecessor_commit_valid, bl);
  ::encode(predecessor_tag_tid, bl);
  ::encode(predecessor_entry_tid, bl);
}

void TagData::decode(bufferlist::iterator& it) {
  ::decode(mirror_uuid, it);
  ::decode(predecessor_mirror_uuid, it);
  ::decode(predecessor_commit_valid, it);
  ::decode(predecessor_tag_tid, it);
  ::decode(predecessor_entry_tid, it);
}

void TagData::dump(Formatter *f) const {
  f->dump_string("mirror_uuid", mirror_uuid);
  f->dump_string("predecessor_mirror_uuid", predecessor_mirror_uuid);
  f->dump_string("predecessor_commit_valid",
                 predecessor_commit_valid ? "true" : "false");
  f->dump_unsigned("predecessor_tag_tid", predecessor_tag_tid);
  f->dump_unsigned("predecessor_entry_tid", predecessor_entry_tid);
}

void TagData::generate_test_instances(std::list<TagData *> &o) {
  o.push_back(new TagData());
  o.push_back(new TagData("mirror-uuid"));
  o.push_back(new TagData("mirror-uuid", "remote-mirror-uuid", true, 123, 234));
}

std::ostream &operator<<(std::ostream &out, const EventType &type) {
  using namespace librbd::journal;

  switch (type) {
  case EVENT_TYPE_AIO_DISCARD:
    out << "AioDiscard";
    break;
  case EVENT_TYPE_AIO_WRITE:
    out << "AioWrite";
    break;
  case EVENT_TYPE_AIO_FLUSH:
    out << "AioFlush";
    break;
  case EVENT_TYPE_OP_FINISH:
    out << "OpFinish";
    break;
  case EVENT_TYPE_SNAP_CREATE:
    out << "SnapCreate";
    break;
  case EVENT_TYPE_SNAP_REMOVE:
    out << "SnapRemove";
    break;
  case EVENT_TYPE_SNAP_RENAME:
    out << "SnapRename";
    break;
  case EVENT_TYPE_SNAP_PROTECT:
    out << "SnapProtect";
    break;
  case EVENT_TYPE_SNAP_UNPROTECT:
    out << "SnapUnprotect";
    break;
  case EVENT_TYPE_SNAP_ROLLBACK:
    out << "SnapRollback";
    break;
  case EVENT_TYPE_RENAME:
    out << "Rename";
    break;
  case EVENT_TYPE_RESIZE:
    out << "Resize";
    break;
  case EVENT_TYPE_FLATTEN:
    out << "Flatten";
    break;
  case EVENT_TYPE_DEMOTE:
    out << "Demote";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(type) << ")";
    break;
  }
  return out;
}

std::ostream &operator<<(std::ostream &out, const ClientMetaType &type) {
  using namespace librbd::journal;

  switch (type) {
  case IMAGE_CLIENT_META_TYPE:
    out << "Master Image";
    break;
  case MIRROR_PEER_CLIENT_META_TYPE:
    out << "Mirror Peer";
    break;
  case CLI_CLIENT_META_TYPE:
    out << "CLI Tool";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(type) << ")";
    break;
  }
  return out;
}

std::ostream &operator<<(std::ostream &out, const ImageClientMeta &meta) {
  out << "[tag_class=" << meta.tag_class << "]";
  return out;
}

std::ostream &operator<<(std::ostream &out, const MirrorPeerSyncPoint &sync) {
  out << "[snap_name=" << sync.snap_name << ", "
      << "from_snap_name=" << sync.from_snap_name;
  if (sync.object_number) {
    out << ", " << *sync.object_number;
  }
  out << "]";
  return out;
}

std::ostream &operator<<(std::ostream &out, const MirrorPeerState &state) {
  switch (state) {
  case MIRROR_PEER_STATE_SYNCING:
    out << "Syncing";
    break;
  case MIRROR_PEER_STATE_REPLAYING:
    out << "Replaying";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return out;
}

std::ostream &operator<<(std::ostream &out, const MirrorPeerClientMeta &meta) {
  out << "[image_id=" << meta.image_id << ", "
      << "state=" << meta.state << ", "
      << "sync_object_count=" << meta.sync_object_count << ", "
      << "sync_points=[";
  std::string delimiter;
  for (auto &sync_point : meta.sync_points) {
    out << delimiter << "[" << sync_point << "]";
    delimiter = ", ";
  }
  out << "], snap_seqs=[";
  delimiter = "";
  for (auto &pair : meta.snap_seqs) {
    out << delimiter << "["
        << "local_snap_seq=" << pair.first << ", "
        << "peer_snap_seq" << pair.second << "]";
    delimiter = ", ";
  }
  out << "]";
  return out;
}

std::ostream &operator<<(std::ostream &out, const TagData &tag_data) {
  out << "["
      << "mirror_uuid=" << tag_data.mirror_uuid << ", "
      << "predecessor_mirror_uuid=" << tag_data.predecessor_mirror_uuid;
  if (tag_data.predecessor_commit_valid) {
    out << ", "
        << "predecessor_tag_tid=" << tag_data.predecessor_tag_tid << ", "
        << "predecessor_entry_tid=" << tag_data.predecessor_entry_tid;
  }
  out << "]";
  return out;
}

} // namespace journal
} // namespace librbd

