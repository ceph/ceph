// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/Types.h"
#include "include/ceph_assert.h"
#include "include/stringify.h"
#include "include/types.h"
#include "common/Formatter.h"

namespace librbd {
namespace journal {

using ceph::encode;
using ceph::decode;

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
    encode(static_cast<uint32_t>(T::TYPE), m_bl);
    t.encode(m_bl);
  }
private:
  bufferlist &m_bl;
};

class DecodeVisitor : public boost::static_visitor<void> {
public:
  DecodeVisitor(__u8 version, bufferlist::const_iterator &iter)
    : m_version(version), m_iter(iter) {
  }

  template <typename T>
  inline void operator()(T& t) const {
    t.decode(m_version, m_iter);
  }
private:
  __u8 m_version;
  bufferlist::const_iterator &m_iter;
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
  using ceph::encode;
  encode(offset, bl);
  encode(length, bl);
  bool skip_partial_discard = (discard_granularity_bytes > 0);
  encode(skip_partial_discard, bl);
  encode(discard_granularity_bytes, bl);
}

void AioDiscardEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(offset, it);
  decode(length, it);

  bool skip_partial_discard = false;
  if (version >= 4) {
    decode(skip_partial_discard, it);
  }

  if (version >= 5) {
    decode(discard_granularity_bytes, it);
  } else {
    if (skip_partial_discard) {
      // use a size larger than the maximum object size which will
      // truncated down to object size during IO processing
      discard_granularity_bytes = std::numeric_limits<uint32_t>::max();
    } else {
      discard_granularity_bytes = 0;
    }
  }
}

void AioDiscardEvent::dump(Formatter *f) const {
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
  f->dump_unsigned("discard_granularity_bytes", discard_granularity_bytes);
}

uint32_t AioWriteEvent::get_fixed_size() {
  return EventEntry::get_fixed_size() + 16 /* offset, length */;
}

void AioWriteEvent::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(offset, bl);
  encode(length, bl);
  encode(data, bl);
}

void AioWriteEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(offset, it);
  decode(length, it);
  decode(data, it);
}

void AioWriteEvent::dump(Formatter *f) const {
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

void AioWriteSameEvent::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(offset, bl);
  encode(length, bl);
  encode(data, bl);
}

void AioWriteSameEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(offset, it);
  decode(length, it);
  decode(data, it);
}

void AioWriteSameEvent::dump(Formatter *f) const {
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

uint32_t AioCompareAndWriteEvent::get_fixed_size() {
  return EventEntry::get_fixed_size() + 32 /* offset, length */;
}

void AioCompareAndWriteEvent::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(offset, bl);
  encode(length, bl);
  encode(cmp_data, bl);
  encode(write_data, bl);
}

void AioCompareAndWriteEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(offset, it);
  decode(length, it);
  decode(cmp_data, it);
  decode(write_data, it);
}

void AioCompareAndWriteEvent::dump(Formatter *f) const {
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

void AioFlushEvent::encode(bufferlist& bl) const {
}

void AioFlushEvent::decode(__u8 version, bufferlist::const_iterator& it) {
}

void AioFlushEvent::dump(Formatter *f) const {
}

void OpEventBase::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(op_tid, bl);
}

void OpEventBase::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(op_tid, it);
}

void OpEventBase::dump(Formatter *f) const {
  f->dump_unsigned("op_tid", op_tid);
}

void OpFinishEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  using ceph::encode;
  encode(op_tid, bl);
  encode(r, bl);
}

void OpFinishEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  OpEventBase::decode(version, it);
  using ceph::decode;
  decode(op_tid, it);
  decode(r, it);
}

void OpFinishEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_unsigned("op_tid", op_tid);
  f->dump_int("result", r);
}

void SnapEventBase::encode(bufferlist& bl) const {
  using ceph::encode;
  OpEventBase::encode(bl);
  encode(snap_name, bl);
  encode(snap_namespace, bl);
}

void SnapEventBase::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  OpEventBase::decode(version, it);
  using ceph::decode;
  decode(snap_name, it);
  if (version >= 4) {
    decode(snap_namespace, it);
  }
}

void SnapEventBase::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_string("snap_name", snap_name);
  snap_namespace.dump(f);
}

void SnapCreateEvent::encode(bufferlist &bl) const {
  SnapEventBase::encode(bl);
}

void SnapCreateEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  SnapEventBase::decode(version, it);
  if (version == 3) {
    decode(snap_namespace, it);
  }
}

void SnapCreateEvent::dump(Formatter *f) const {
  SnapEventBase::dump(f);
}

void SnapLimitEvent::encode(bufferlist &bl) const {
  OpEventBase::encode(bl);
  using ceph::encode;
  encode(limit, bl);
}

void SnapLimitEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  OpEventBase::decode(version, it);
  using ceph::decode;
  decode(limit, it);
}

void SnapLimitEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_unsigned("limit", limit);
}

void SnapRenameEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  using ceph::encode;
  encode(dst_snap_name, bl);
  encode(snap_id, bl);
  encode(src_snap_name, bl);
}

void SnapRenameEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  OpEventBase::decode(version, it);
  decode(dst_snap_name, it);
  decode(snap_id, it);
  if (version >= 2) {
    decode(src_snap_name, it);
  }
}

void SnapRenameEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_unsigned("src_snap_id", snap_id);
  f->dump_string("src_snap_name", src_snap_name);
  f->dump_string("dest_snap_name", dst_snap_name);
}

void RenameEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  using ceph::encode;
  encode(image_name, bl);
}

void RenameEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  OpEventBase::decode(version, it);
  using ceph::decode;
  decode(image_name, it);
}

void RenameEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_string("image_name", image_name);
}

void ResizeEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  using ceph::encode;
  encode(size, bl);
}

void ResizeEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  OpEventBase::decode(version, it);
  using ceph::decode;
  decode(size, it);
}

void ResizeEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_unsigned("size", size);
}

void DemotePromoteEvent::encode(bufferlist& bl) const {
}

void DemotePromoteEvent::decode(__u8 version, bufferlist::const_iterator& it) {
}

void DemotePromoteEvent::dump(Formatter *f) const {
}

void UpdateFeaturesEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  using ceph::encode;
  encode(features, bl);
  encode(enabled, bl);
}

void UpdateFeaturesEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  OpEventBase::decode(version, it);
  using ceph::decode;
  decode(features, it);
  decode(enabled, it);
}

void UpdateFeaturesEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_unsigned("features", features);
  f->dump_bool("enabled", enabled);
}

void MetadataSetEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  using ceph::encode;
  encode(key, bl);
  encode(value, bl);
}

void MetadataSetEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  OpEventBase::decode(version, it);
  using ceph::decode;
  decode(key, it);
  decode(value, it);
}

void MetadataSetEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_string("key", key);
  f->dump_string("value", value);
}

void MetadataRemoveEvent::encode(bufferlist& bl) const {
  OpEventBase::encode(bl);
  using ceph::encode;
  encode(key, bl);
}

void MetadataRemoveEvent::decode(__u8 version, bufferlist::const_iterator& it) {
  OpEventBase::decode(version, it);
  using ceph::decode;
  decode(key, it);
}

void MetadataRemoveEvent::dump(Formatter *f) const {
  OpEventBase::dump(f);
  f->dump_string("key", key);
}

void UnknownEvent::encode(bufferlist& bl) const {
  ceph_abort();
}

void UnknownEvent::decode(__u8 version, bufferlist::const_iterator& it) {
}

void UnknownEvent::dump(Formatter *f) const {
}

EventType EventEntry::get_event_type() const {
  return boost::apply_visitor(GetTypeVisitor<EventType>(), event);
}

void EventEntry::encode(bufferlist& bl) const {
  ENCODE_START(5, 1, bl);
  boost::apply_visitor(EncodeVisitor(bl), event);
  ENCODE_FINISH(bl);
  encode_metadata(bl);
}

void EventEntry::decode(bufferlist::const_iterator& it) {
  DECODE_START(1, it);

  uint32_t event_type;
  decode(event_type, it);

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
  case EVENT_TYPE_DEMOTE_PROMOTE:
    event = DemotePromoteEvent();
    break;
  case EVENT_TYPE_UPDATE_FEATURES:
    event = UpdateFeaturesEvent();
    break;
  case EVENT_TYPE_METADATA_SET:
    event = MetadataSetEvent();
    break;
  case EVENT_TYPE_METADATA_REMOVE:
    event = MetadataRemoveEvent();
    break;
  case EVENT_TYPE_AIO_WRITESAME:
    event = AioWriteSameEvent();
    break;
  case EVENT_TYPE_AIO_COMPARE_AND_WRITE:
    event = AioCompareAndWriteEvent();
    break;
  default:
    event = UnknownEvent();
    break;
  }

  boost::apply_visitor(DecodeVisitor(struct_v, it), event);
  DECODE_FINISH(it);
  if (struct_v >= 4) {
    decode_metadata(it);
  }
}

void EventEntry::dump(Formatter *f) const {
  boost::apply_visitor(DumpVisitor(f, "event_type"), event);
  f->dump_stream("timestamp") << timestamp;
}

void EventEntry::encode_metadata(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(timestamp, bl);
  ENCODE_FINISH(bl);
}

void EventEntry::decode_metadata(bufferlist::const_iterator& it) {
  DECODE_START(1, it);
  decode(timestamp, it);
  DECODE_FINISH(it);
}

void EventEntry::generate_test_instances(std::list<EventEntry *> &o) {
  o.push_back(new EventEntry(AioDiscardEvent()));
  o.push_back(new EventEntry(AioDiscardEvent(123, 345, 4096), utime_t(1, 1)));

  bufferlist bl;
  bl.append(std::string(32, '1'));
  o.push_back(new EventEntry(AioWriteEvent()));
  o.push_back(new EventEntry(AioWriteEvent(123, 456, bl), utime_t(1, 1)));

  o.push_back(new EventEntry(AioFlushEvent()));

  o.push_back(new EventEntry(OpFinishEvent(123, -1), utime_t(1, 1)));

  o.push_back(new EventEntry(SnapCreateEvent(), utime_t(1, 1)));
  o.push_back(new EventEntry(SnapCreateEvent(234, cls::rbd::UserSnapshotNamespace(), "snap"), utime_t(1, 1)));

  o.push_back(new EventEntry(SnapRemoveEvent()));
  o.push_back(new EventEntry(SnapRemoveEvent(345, cls::rbd::UserSnapshotNamespace(), "snap"), utime_t(1, 1)));

  o.push_back(new EventEntry(SnapRenameEvent()));
  o.push_back(new EventEntry(SnapRenameEvent(456, 1, "src snap", "dest snap"),
                             utime_t(1, 1)));

  o.push_back(new EventEntry(SnapProtectEvent()));
  o.push_back(new EventEntry(SnapProtectEvent(567, cls::rbd::UserSnapshotNamespace(), "snap"), utime_t(1, 1)));

  o.push_back(new EventEntry(SnapUnprotectEvent()));
  o.push_back(new EventEntry(SnapUnprotectEvent(678, cls::rbd::UserSnapshotNamespace(), "snap"), utime_t(1, 1)));

  o.push_back(new EventEntry(SnapRollbackEvent()));
  o.push_back(new EventEntry(SnapRollbackEvent(789, cls::rbd::UserSnapshotNamespace(), "snap"), utime_t(1, 1)));

  o.push_back(new EventEntry(RenameEvent()));
  o.push_back(new EventEntry(RenameEvent(890, "image name"), utime_t(1, 1)));

  o.push_back(new EventEntry(ResizeEvent()));
  o.push_back(new EventEntry(ResizeEvent(901, 1234), utime_t(1, 1)));

  o.push_back(new EventEntry(FlattenEvent(123), utime_t(1, 1)));

  o.push_back(new EventEntry(DemotePromoteEvent()));

  o.push_back(new EventEntry(UpdateFeaturesEvent()));
  o.push_back(new EventEntry(UpdateFeaturesEvent(123, 127, true), utime_t(1, 1)));

  o.push_back(new EventEntry(MetadataSetEvent()));
  o.push_back(new EventEntry(MetadataSetEvent(123, "key", "value"), utime_t(1, 1)));

  o.push_back(new EventEntry(MetadataRemoveEvent()));
  o.push_back(new EventEntry(MetadataRemoveEvent(123, "key"), utime_t(1, 1)));
}

// Journal Client

void ImageClientMeta::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(tag_class, bl);
  encode(resync_requested, bl);
}

void ImageClientMeta::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(tag_class, it);
  decode(resync_requested, it);
}

void ImageClientMeta::dump(Formatter *f) const {
  f->dump_unsigned("tag_class", tag_class);
  f->dump_bool("resync_requested", resync_requested);
}

void MirrorPeerSyncPoint::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(snap_name, bl);
  encode(from_snap_name, bl);
  encode(object_number, bl);
  encode(snap_namespace, bl);
}

void MirrorPeerSyncPoint::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(snap_name, it);
  decode(from_snap_name, it);
  decode(object_number, it);
  if (version >= 2) {
    decode(snap_namespace, it);
  }
}

void MirrorPeerSyncPoint::dump(Formatter *f) const {
  f->dump_string("snap_name", snap_name);
  f->dump_string("from_snap_name", from_snap_name);
  if (object_number) {
    f->dump_unsigned("object_number", *object_number);
  }
  snap_namespace.dump(f);
}

void MirrorPeerClientMeta::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(image_id, bl);
  encode(static_cast<uint32_t>(state), bl);
  encode(sync_object_count, bl);
  encode(static_cast<uint32_t>(sync_points.size()), bl);
  for (auto &sync_point : sync_points) {
    sync_point.encode(bl);
  }
  encode(snap_seqs, bl);
}

void MirrorPeerClientMeta::decode(__u8 version, bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(image_id, it);

  uint32_t decode_state;
  decode(decode_state, it);
  state = static_cast<MirrorPeerState>(decode_state);

  decode(sync_object_count, it);

  uint32_t sync_point_count;
  decode(sync_point_count, it);
  sync_points.resize(sync_point_count);
  for (auto &sync_point : sync_points) {
    sync_point.decode(version, it);
  }

  decode(snap_seqs, it);
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

void CliClientMeta::decode(__u8 version, bufferlist::const_iterator& it) {
}

void CliClientMeta::dump(Formatter *f) const {
}

void UnknownClientMeta::encode(bufferlist& bl) const {
  ceph_abort();
}

void UnknownClientMeta::decode(__u8 version, bufferlist::const_iterator& it) {
}

void UnknownClientMeta::dump(Formatter *f) const {
}

ClientMetaType ClientData::get_client_meta_type() const {
  return boost::apply_visitor(GetTypeVisitor<ClientMetaType>(), client_meta);
}

void ClientData::encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);
  boost::apply_visitor(EncodeVisitor(bl), client_meta);
  ENCODE_FINISH(bl);
}

void ClientData::decode(bufferlist::const_iterator& it) {
  DECODE_START(1, it);

  uint32_t client_meta_type;
  decode(client_meta_type, it);

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
                                                  {{{}, "snap 2", "snap 1", 123}},
                                                  {{1, 2}, {3, 4}})));
  o.push_back(new ClientData(CliClientMeta()));
}

// Journal Tag

void TagPredecessor::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(mirror_uuid, bl);
  encode(commit_valid, bl);
  encode(tag_tid, bl);
  encode(entry_tid, bl);
}

void TagPredecessor::decode(bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(mirror_uuid, it);
  decode(commit_valid, it);
  decode(tag_tid, it);
  decode(entry_tid, it);
}

void TagPredecessor::dump(Formatter *f) const {
  f->dump_string("mirror_uuid", mirror_uuid);
  f->dump_string("commit_valid", commit_valid ? "true" : "false");
  f->dump_unsigned("tag_tid", tag_tid);
  f->dump_unsigned("entry_tid", entry_tid);
}

void TagData::encode(bufferlist& bl) const {
  using ceph::encode;
  encode(mirror_uuid, bl);
  predecessor.encode(bl);
}

void TagData::decode(bufferlist::const_iterator& it) {
  using ceph::decode;
  decode(mirror_uuid, it);
  predecessor.decode(it);
}

void TagData::dump(Formatter *f) const {
  f->dump_string("mirror_uuid", mirror_uuid);
  f->open_object_section("predecessor");
  predecessor.dump(f);
  f->close_section();
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
  case EVENT_TYPE_DEMOTE_PROMOTE:
    out << "Demote/Promote";
    break;
  case EVENT_TYPE_UPDATE_FEATURES:
    out << "UpdateFeatures";
    break;
  case EVENT_TYPE_METADATA_SET:
    out << "MetadataSet";
    break;
  case EVENT_TYPE_METADATA_REMOVE:
    out << "MetadataRemove";
    break;
  case EVENT_TYPE_AIO_WRITESAME:
    out << "AioWriteSame";
    break;
  case EVENT_TYPE_AIO_COMPARE_AND_WRITE:
    out << "AioCompareAndWrite";
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

std::ostream &operator<<(std::ostream &out, const TagPredecessor &predecessor) {
  out << "["
      << "mirror_uuid=" << predecessor.mirror_uuid;
  if (predecessor.commit_valid) {
    out << ", "
        << "tag_tid=" << predecessor.tag_tid << ", "
        << "entry_tid=" << predecessor.entry_tid;
  }
  out << "]";
  return out;
}

std::ostream &operator<<(std::ostream &out, const TagData &tag_data) {
  out << "["
      << "mirror_uuid=" << tag_data.mirror_uuid << ", "
      << "predecessor=" << tag_data.predecessor
      << "]";
  return out;
}

} // namespace journal
} // namespace librbd

