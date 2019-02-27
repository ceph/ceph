// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_TYPES_H
#define CEPH_LIBRBD_JOURNAL_TYPES_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/types.h"
#include "include/utime.h"
#include "librbd/Types.h"
#include <iosfwd>
#include <list>
#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <boost/mpl/vector.hpp>

namespace ceph {
class Formatter;
}

namespace librbd {
namespace journal {

enum EventType {
  EVENT_TYPE_AIO_DISCARD           = 0,
  EVENT_TYPE_AIO_WRITE             = 1,
  EVENT_TYPE_AIO_FLUSH             = 2,
  EVENT_TYPE_OP_FINISH             = 3,
  EVENT_TYPE_SNAP_CREATE           = 4,
  EVENT_TYPE_SNAP_REMOVE           = 5,
  EVENT_TYPE_SNAP_RENAME           = 6,
  EVENT_TYPE_SNAP_PROTECT          = 7,
  EVENT_TYPE_SNAP_UNPROTECT        = 8,
  EVENT_TYPE_SNAP_ROLLBACK         = 9,
  EVENT_TYPE_RENAME                = 10,
  EVENT_TYPE_RESIZE                = 11,
  EVENT_TYPE_FLATTEN               = 12,
  EVENT_TYPE_DEMOTE_PROMOTE        = 13,
  EVENT_TYPE_SNAP_LIMIT            = 14,
  EVENT_TYPE_UPDATE_FEATURES       = 15,
  EVENT_TYPE_METADATA_SET          = 16,
  EVENT_TYPE_METADATA_REMOVE       = 17,
  EVENT_TYPE_AIO_WRITESAME         = 18,
  EVENT_TYPE_AIO_COMPARE_AND_WRITE = 19,
};

struct AioDiscardEvent {
  static const EventType TYPE = EVENT_TYPE_AIO_DISCARD;

  uint64_t offset = 0;
  uint64_t length = 0;
  uint32_t discard_granularity_bytes = 0;

  AioDiscardEvent() {
  }
  AioDiscardEvent(uint64_t _offset, uint64_t _length,
                  uint32_t discard_granularity_bytes)
    : offset(_offset), length(_length),
      discard_granularity_bytes(discard_granularity_bytes) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct AioWriteEvent {
  static const EventType TYPE = EVENT_TYPE_AIO_WRITE;

  uint64_t offset;
  uint64_t length;
  bufferlist data;

  static uint32_t get_fixed_size();

  AioWriteEvent() : offset(0), length(0) {
  }
  AioWriteEvent(uint64_t _offset, uint64_t _length, const bufferlist &_data)
    : offset(_offset), length(_length), data(_data) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct AioWriteSameEvent {
  static const EventType TYPE = EVENT_TYPE_AIO_WRITESAME;

  uint64_t offset;
  uint64_t length;
  bufferlist data;

  AioWriteSameEvent() : offset(0), length(0) {
  }
  AioWriteSameEvent(uint64_t _offset, uint64_t _length,
                    const bufferlist &_data)
    : offset(_offset), length(_length), data(_data) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct AioCompareAndWriteEvent {
  static const EventType TYPE = EVENT_TYPE_AIO_COMPARE_AND_WRITE;

  uint64_t offset;
  uint64_t length;
  bufferlist cmp_data;
  bufferlist write_data;

  static uint32_t get_fixed_size();

  AioCompareAndWriteEvent() : offset(0), length(0) {
  }
  AioCompareAndWriteEvent(uint64_t _offset, uint64_t _length,
                          const bufferlist &_cmp_data, const bufferlist &_write_data)
    : offset(_offset), length(_length), cmp_data(_cmp_data), write_data(_write_data) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct AioFlushEvent {
  static const EventType TYPE = EVENT_TYPE_AIO_FLUSH;

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct OpEventBase {
  uint64_t op_tid;

protected:
  OpEventBase() : op_tid(0) {
  }
  OpEventBase(uint64_t op_tid) : op_tid(op_tid) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct OpFinishEvent : public OpEventBase {
  static const EventType TYPE = EVENT_TYPE_OP_FINISH;

  int r;

  OpFinishEvent() : r(0) {
  }
  OpFinishEvent(uint64_t op_tid, int r) : OpEventBase(op_tid), r(r) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct SnapEventBase : public OpEventBase {
  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;

protected:
  SnapEventBase() {
  }
  SnapEventBase(uint64_t op_tid, const cls::rbd::SnapshotNamespace& _snap_namespace,
		const std::string &_snap_name)
    : OpEventBase(op_tid),
      snap_namespace(_snap_namespace),
      snap_name(_snap_name) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct SnapCreateEvent : public SnapEventBase {
  static const EventType TYPE = EVENT_TYPE_SNAP_CREATE;

  SnapCreateEvent() {
  }
  SnapCreateEvent(uint64_t op_tid, const cls::rbd::SnapshotNamespace& snap_namespace,
		  const std::string &snap_name)
    : SnapEventBase(op_tid, snap_namespace, snap_name) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct SnapRemoveEvent : public SnapEventBase {
  static const EventType TYPE = EVENT_TYPE_SNAP_REMOVE;

  SnapRemoveEvent() {
  }
  SnapRemoveEvent(uint64_t op_tid, const cls::rbd::SnapshotNamespace& snap_namespace,
		  const std::string &snap_name)
    : SnapEventBase(op_tid, snap_namespace, snap_name) {
  }

  using SnapEventBase::encode;
  using SnapEventBase::decode;
  using SnapEventBase::dump;
};

struct SnapRenameEvent : public OpEventBase{
  static const EventType TYPE = EVENT_TYPE_SNAP_RENAME;

  uint64_t snap_id;
  std::string src_snap_name;
  std::string dst_snap_name;

  SnapRenameEvent() : snap_id(CEPH_NOSNAP) {
  }
  SnapRenameEvent(uint64_t op_tid, uint64_t src_snap_id,
                  const std::string &src_snap_name,
                  const std::string &dest_snap_name)
    : OpEventBase(op_tid),
      snap_id(src_snap_id),
      src_snap_name(src_snap_name),
      dst_snap_name(dest_snap_name) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct SnapProtectEvent : public SnapEventBase {
  static const EventType TYPE = EVENT_TYPE_SNAP_PROTECT;

  SnapProtectEvent() {
  }
  SnapProtectEvent(uint64_t op_tid, const cls::rbd::SnapshotNamespace& snap_namespace,
		   const std::string &snap_name)
    : SnapEventBase(op_tid, snap_namespace, snap_name) {
  }

  using SnapEventBase::encode;
  using SnapEventBase::decode;
  using SnapEventBase::dump;
};

struct SnapUnprotectEvent : public SnapEventBase {
  static const EventType TYPE = EVENT_TYPE_SNAP_UNPROTECT;

  SnapUnprotectEvent() {
  }
  SnapUnprotectEvent(uint64_t op_tid, const cls::rbd::SnapshotNamespace &snap_namespace,
		     const std::string &snap_name)
    : SnapEventBase(op_tid, snap_namespace, snap_name) {
  }

  using SnapEventBase::encode;
  using SnapEventBase::decode;
  using SnapEventBase::dump;
};

struct SnapLimitEvent : public OpEventBase {
  static const EventType TYPE = EVENT_TYPE_SNAP_LIMIT;
  uint64_t limit;

  SnapLimitEvent() {
  }
  SnapLimitEvent(uint64_t op_tid, const uint64_t _limit)
    : OpEventBase(op_tid), limit(_limit) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct SnapRollbackEvent : public SnapEventBase {
  static const EventType TYPE = EVENT_TYPE_SNAP_ROLLBACK;

  SnapRollbackEvent() {
  }
  SnapRollbackEvent(uint64_t op_tid, const cls::rbd::SnapshotNamespace& snap_namespace,
		    const std::string &snap_name)
    : SnapEventBase(op_tid, snap_namespace, snap_name) {
  }

  using SnapEventBase::encode;
  using SnapEventBase::decode;
  using SnapEventBase::dump;
};

struct RenameEvent : public OpEventBase {
  static const EventType TYPE = EVENT_TYPE_RENAME;

  std::string image_name;

  RenameEvent() {
  }
  RenameEvent(uint64_t op_tid, const std::string &_image_name)
    : OpEventBase(op_tid), image_name(_image_name) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct ResizeEvent : public OpEventBase {
  static const EventType TYPE = EVENT_TYPE_RESIZE;

  uint64_t size;

  ResizeEvent() : size(0) {
  }
  ResizeEvent(uint64_t op_tid, uint64_t _size)
    : OpEventBase(op_tid), size(_size) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct FlattenEvent : public OpEventBase {
  static const EventType TYPE = EVENT_TYPE_FLATTEN;

  FlattenEvent() {
  }
  FlattenEvent(uint64_t op_tid) : OpEventBase(op_tid) {
  }

  using OpEventBase::encode;
  using OpEventBase::decode;
  using OpEventBase::dump;
};

struct DemotePromoteEvent {
  static const EventType TYPE = static_cast<EventType>(
    EVENT_TYPE_DEMOTE_PROMOTE);

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct UpdateFeaturesEvent : public OpEventBase {
  static const EventType TYPE = EVENT_TYPE_UPDATE_FEATURES;

  uint64_t features;
  bool enabled;

  UpdateFeaturesEvent() : features(0), enabled(false) {
  }
  UpdateFeaturesEvent(uint64_t op_tid, uint64_t _features, bool _enabled)
    : OpEventBase(op_tid), features(_features), enabled(_enabled) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct MetadataSetEvent : public OpEventBase {
  static const EventType TYPE = EVENT_TYPE_METADATA_SET;

  string key;
  string value;

  MetadataSetEvent() {
  }
  MetadataSetEvent(uint64_t op_tid, const string &_key, const string &_value)
    : OpEventBase(op_tid), key(_key), value(_value) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct MetadataRemoveEvent : public OpEventBase {
  static const EventType TYPE = EVENT_TYPE_METADATA_REMOVE;

  string key;

  MetadataRemoveEvent() {
  }
  MetadataRemoveEvent(uint64_t op_tid, const string &_key)
    : OpEventBase(op_tid), key(_key) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct UnknownEvent {
  static const EventType TYPE = static_cast<EventType>(-1);

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

typedef boost::mpl::vector<AioDiscardEvent,
                           AioWriteEvent,
                           AioFlushEvent,
                           OpFinishEvent,
                           SnapCreateEvent,
                           SnapRemoveEvent,
                           SnapRenameEvent,
                           SnapProtectEvent,
                           SnapUnprotectEvent,
                           SnapRollbackEvent,
                           RenameEvent,
                           ResizeEvent,
                           FlattenEvent,
                           DemotePromoteEvent,
                           SnapLimitEvent,
                           UpdateFeaturesEvent,
                           MetadataSetEvent,
                           MetadataRemoveEvent,
                           AioWriteSameEvent,
                           AioCompareAndWriteEvent,
                           UnknownEvent> EventVector;
typedef boost::make_variant_over<EventVector>::type Event;

struct EventEntry {
  static uint32_t get_fixed_size() {
    return EVENT_FIXED_SIZE + METADATA_FIXED_SIZE;
  }

  EventEntry() : event(UnknownEvent()) {
  }
  EventEntry(const Event &_event, const utime_t &_timestamp = utime_t())
    : event(_event), timestamp(_timestamp) {
  }

  Event event;
  utime_t timestamp;

  EventType get_event_type() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<EventEntry *> &o);

private:
  static const uint32_t EVENT_FIXED_SIZE = 14; /// version encoding, type
  static const uint32_t METADATA_FIXED_SIZE = 14; /// version encoding, timestamp

  void encode_metadata(bufferlist& bl) const;
  void decode_metadata(bufferlist::const_iterator& it);
};

// Journal Client data structures

enum ClientMetaType {
  IMAGE_CLIENT_META_TYPE       = 0,
  MIRROR_PEER_CLIENT_META_TYPE = 1,
  CLI_CLIENT_META_TYPE         = 2
};

struct ImageClientMeta {
  static const ClientMetaType TYPE = IMAGE_CLIENT_META_TYPE;

  uint64_t tag_class = 0;
  bool resync_requested = false;

  ImageClientMeta() {
  }
  ImageClientMeta(uint64_t tag_class) : tag_class(tag_class) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct MirrorPeerSyncPoint {
  typedef boost::optional<uint64_t> ObjectNumber;

  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;
  std::string from_snap_name;
  ObjectNumber object_number;

  MirrorPeerSyncPoint() : MirrorPeerSyncPoint({}, "", "", boost::none) {
  }
  MirrorPeerSyncPoint(const cls::rbd::SnapshotNamespace& snap_namespace,
		      const std::string &snap_name,
                      const ObjectNumber &object_number)
    : MirrorPeerSyncPoint(snap_namespace, snap_name, "", object_number) {
  }
  MirrorPeerSyncPoint(const cls::rbd::SnapshotNamespace& snap_namespace,
		      const std::string &snap_name,
                      const std::string &from_snap_name,
                      const ObjectNumber &object_number)
    : snap_namespace(snap_namespace), snap_name(snap_name),
    from_snap_name(from_snap_name), object_number(object_number) {
  }

  inline bool operator==(const MirrorPeerSyncPoint &sync) const {
    return (snap_name == sync.snap_name &&
            from_snap_name == sync.from_snap_name &&
            object_number == sync.object_number &&
	    snap_namespace == sync.snap_namespace);
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

enum MirrorPeerState {
  MIRROR_PEER_STATE_SYNCING,
  MIRROR_PEER_STATE_REPLAYING
};

struct MirrorPeerClientMeta {
  typedef std::list<MirrorPeerSyncPoint> SyncPoints;

  static const ClientMetaType TYPE = MIRROR_PEER_CLIENT_META_TYPE;

  std::string image_id;
  MirrorPeerState state = MIRROR_PEER_STATE_SYNCING; ///< replay state
  uint64_t sync_object_count = 0; ///< maximum number of objects ever sync'ed
  SyncPoints sync_points;         ///< max two in-use snapshots for sync
  SnapSeqs snap_seqs;             ///< local to peer snap seq mapping

  MirrorPeerClientMeta() {
  }
  MirrorPeerClientMeta(const std::string &image_id,
                       const SyncPoints &sync_points = SyncPoints(),
                       const SnapSeqs &snap_seqs = SnapSeqs())
    : image_id(image_id), sync_points(sync_points), snap_seqs(snap_seqs) {
  }

  inline bool operator==(const MirrorPeerClientMeta &meta) const {
    return (image_id == meta.image_id &&
            state == meta.state &&
            sync_object_count == meta.sync_object_count &&
            sync_points == meta.sync_points &&
            snap_seqs == meta.snap_seqs);
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct CliClientMeta {
  static const ClientMetaType TYPE = CLI_CLIENT_META_TYPE;

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct UnknownClientMeta {
  static const ClientMetaType TYPE = static_cast<ClientMetaType>(-1);

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

typedef boost::variant<ImageClientMeta,
                       MirrorPeerClientMeta,
                       CliClientMeta,
                       UnknownClientMeta> ClientMeta;

struct ClientData {
  ClientData() {
  }
  ClientData(const ClientMeta &client_meta) : client_meta(client_meta) {
  }

  ClientMeta client_meta;

  ClientMetaType get_client_meta_type() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<ClientData *> &o);
};

// Journal Tag data structures

struct TagPredecessor {
  std::string mirror_uuid; // empty if local
  bool commit_valid = false;
  uint64_t tag_tid = 0;
  uint64_t entry_tid = 0;

  TagPredecessor() {
  }
  TagPredecessor(const std::string &mirror_uuid, bool commit_valid,
                 uint64_t tag_tid, uint64_t entry_tid)
    : mirror_uuid(mirror_uuid), commit_valid(commit_valid), tag_tid(tag_tid),
      entry_tid(entry_tid) {
  }

  inline bool operator==(const TagPredecessor &rhs) const {
    return (mirror_uuid == rhs.mirror_uuid &&
            commit_valid == rhs.commit_valid &&
            tag_tid == rhs.tag_tid &&
            entry_tid == rhs.entry_tid);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
};

struct TagData {
  // owner of the tag (exclusive lock epoch)
  std::string mirror_uuid; // empty if local

  // mapping to last committed record of previous tag
  TagPredecessor predecessor;

  TagData() {
  }
  TagData(const std::string &mirror_uuid) : mirror_uuid(mirror_uuid) {
  }
  TagData(const std::string &mirror_uuid,
          const std::string &predecessor_mirror_uuid,
          bool predecessor_commit_valid,
          uint64_t predecessor_tag_tid, uint64_t predecessor_entry_tid)
    : mirror_uuid(mirror_uuid),
      predecessor(predecessor_mirror_uuid, predecessor_commit_valid,
                  predecessor_tag_tid, predecessor_entry_tid) {
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<TagData *> &o);
};

std::ostream &operator<<(std::ostream &out, const EventType &type);
std::ostream &operator<<(std::ostream &out, const ClientMetaType &type);
std::ostream &operator<<(std::ostream &out, const ImageClientMeta &meta);
std::ostream &operator<<(std::ostream &out, const MirrorPeerSyncPoint &sync);
std::ostream &operator<<(std::ostream &out, const MirrorPeerState &meta);
std::ostream &operator<<(std::ostream &out, const MirrorPeerClientMeta &meta);
std::ostream &operator<<(std::ostream &out, const TagPredecessor &predecessor);
std::ostream &operator<<(std::ostream &out, const TagData &tag_data);

struct Listener {
  virtual ~Listener() {
  }

  /// invoked when journal close is requested
  virtual void handle_close() = 0;

  /// invoked when journal is promoted to primary
  virtual void handle_promoted() = 0;

  /// invoked when journal resync is requested
  virtual void handle_resync() = 0;
};

WRITE_CLASS_ENCODER(EventEntry);
WRITE_CLASS_ENCODER(ClientData);
WRITE_CLASS_ENCODER(TagData);

} // namespace journal
} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_TYPES_H
