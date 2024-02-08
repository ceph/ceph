// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_BLUEFS_TYPES_H
#define CEPH_OS_BLUESTORE_BLUEFS_TYPES_H

#include <optional>

#include "bluestore_types.h"
#include "include/utime.h"
#include "include/encoding.h"
#include "include/denc.h"

class bluefs_extent_t {
public:
  uint64_t offset = 0;
  uint32_t length = 0;
  uint8_t bdev;

  bluefs_extent_t(uint8_t b = 0, uint64_t o = 0, uint32_t l = 0)
    : offset(o), length(l), bdev(b) {}

  uint64_t end() const { return  offset + length; }
  DENC(bluefs_extent_t, v, p) {
    DENC_START(1, 1, p);
    denc_lba(v.offset, p);
    denc_varint_lowz(v.length, p);
    denc(v.bdev, p);
    DENC_FINISH(p);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluefs_extent_t*>&);
};
WRITE_CLASS_DENC(bluefs_extent_t)

std::ostream& operator<<(std::ostream& out, const bluefs_extent_t& e);

struct bluefs_fnode_delta_t {
  uint64_t ino;
  uint64_t size;
  utime_t mtime;
  uint64_t offset; // Contains offset in file of extents.
                   // Equal to 'allocated' when created.
                   // Used for consistency checking.
  mempool::bluefs::vector<bluefs_extent_t> extents;

  DENC(bluefs_fnode_delta_t, v, p) {
    DENC_START(1, 1, p);
    denc_varint(v.ino, p);
    denc_varint(v.size, p);
    denc(v.mtime, p);
    denc(v.offset, p);
    denc(v.extents, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(bluefs_fnode_delta_t)

std::ostream& operator<<(std::ostream& out, const bluefs_fnode_delta_t& delta);

struct bluefs_fnode_t {
  uint64_t ino;
  uint64_t size;
  utime_t mtime;
  uint8_t __unused__ = 0; // was prefer_bdev
  mempool::bluefs::vector<bluefs_extent_t> extents;

  // precalculated logical offsets for extents vector entries
  // allows fast lookup for extent index by the offset value via upper_bound()
  mempool::bluefs::vector<uint64_t> extents_index;

  uint64_t allocated;
  uint64_t allocated_commited;

  bluefs_fnode_t() : ino(0), size(0), allocated(0), allocated_commited(0) {}
  bluefs_fnode_t(uint64_t _ino, uint64_t _size, utime_t _mtime) :
    ino(_ino), size(_size), mtime(_mtime), allocated(0), allocated_commited(0) {}
  bluefs_fnode_t(const bluefs_fnode_t& other) :
    ino(other.ino), size(other.size), mtime(other.mtime),
    allocated(other.allocated),
    allocated_commited(other.allocated_commited) {
    clone_extents(other);
  }

  uint64_t get_allocated() const {
    return allocated;
  }

  void recalc_allocated() {
    allocated = 0;
    extents_index.reserve(extents.size());
    for (auto& p : extents) {
      extents_index.emplace_back(allocated);
      allocated += p.length;
    }
    allocated_commited = allocated;
  }

  DENC_HELPERS
  void bound_encode(size_t& p) const {
    _denc_friend(*this, p);
  }
  void encode(ceph::buffer::list::contiguous_appender& p) const {
    DENC_DUMP_PRE(bluefs_fnode_t);
    _denc_friend(*this, p);
  }
  void decode(ceph::buffer::ptr::const_iterator& p) {
    _denc_friend(*this, p);
    recalc_allocated();
  }
  template<typename T, typename P>
  friend std::enable_if_t<std::is_same_v<bluefs_fnode_t, std::remove_const_t<T>>>
  _denc_friend(T& v, P& p) {
    DENC_START(1, 1, p);
    denc_varint(v.ino, p);
    denc_varint(v.size, p);
    denc(v.mtime, p);
    denc(v.__unused__, p);
    denc(v.extents, p);
    DENC_FINISH(p);
  }
  void reset_delta() {
    allocated_commited = allocated;
  }
  void clone_extents(const bluefs_fnode_t& fnode) {
    for (const auto& p : fnode.extents) {
      append_extent(p);
    }
  }
  void claim_extents(mempool::bluefs::vector<bluefs_extent_t>& extents) {
    for (const auto& p : extents) {
      append_extent(p);
    }
    extents.clear();
  }
  void append_extent(const bluefs_extent_t& ext) {
    if (!extents.empty() &&
	extents.back().end() == ext.offset &&
	extents.back().bdev == ext.bdev &&
	(uint64_t)extents.back().length + (uint64_t)ext.length < 0xffffffff) {
      extents.back().length += ext.length;
    } else {
      extents_index.emplace_back(allocated);
      extents.push_back(ext);
    }
    allocated += ext.length;
  }

  void pop_front_extent() {
    auto it = extents.begin();
    allocated -= it->length;
    extents_index.erase(extents_index.begin());
    for (auto& i: extents_index) {
      i -= it->length;
    }
    extents.erase(it);
  }
  
  void swap(bluefs_fnode_t& other) {
    std::swap(ino, other.ino);
    std::swap(size, other.size);
    std::swap(mtime, other.mtime);
    swap_extents(other);
  }
  void swap_extents(bluefs_fnode_t& other) {
    other.extents.swap(extents);
    other.extents_index.swap(extents_index);
    std::swap(allocated, other.allocated);
    std::swap(allocated_commited, other.allocated_commited);
  }
  void clear_extents() {
    extents_index.clear();
    extents.clear();
    allocated = 0;
    allocated_commited = 0;
  }

  mempool::bluefs::vector<bluefs_extent_t>::iterator seek(
    uint64_t off, uint64_t *x_off);
  bluefs_fnode_delta_t* make_delta(bluefs_fnode_delta_t* delta);

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluefs_fnode_t*>& ls);

};
WRITE_CLASS_DENC(bluefs_fnode_t)

std::ostream& operator<<(std::ostream& out, const bluefs_fnode_t& file);

struct bluefs_layout_t {
  unsigned shared_bdev = 0;         ///< which bluefs bdev we are sharing
  bool dedicated_db = false;        ///< whether block.db is present
  bool dedicated_wal = false;       ///< whether block.wal is present

  bool single_shared_device() const {
    return !dedicated_db && !dedicated_wal;
  }

  bool operator==(const bluefs_layout_t& other) const {
    return shared_bdev == other.shared_bdev &&
           dedicated_db == other.dedicated_db &&
           dedicated_wal == other.dedicated_wal;
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluefs_layout_t*>& ls);
};
WRITE_CLASS_ENCODER(bluefs_layout_t)

struct bluefs_super_t {
  uuid_d uuid;      ///< unique to this bluefs instance
  uuid_d osd_uuid;  ///< matches the osd that owns us
  uint64_t version;
  uint32_t block_size;

  bluefs_fnode_t log_fnode;

  std::optional<bluefs_layout_t> memorized_layout;

  bluefs_super_t()
    : version(0),
      block_size(4096) { }

  uint64_t block_mask() const {
    return ~((uint64_t)block_size - 1);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluefs_super_t*>& ls);
};
WRITE_CLASS_ENCODER(bluefs_super_t)

std::ostream& operator<<(std::ostream&, const bluefs_super_t& s);


struct bluefs_transaction_t {
  typedef enum {
    OP_NONE = 0,
    OP_INIT,        ///< initial (empty) file system marker
    OP_ALLOC_ADD,   ///< OBSOLETE: add extent to available block storage (extent)
    OP_ALLOC_RM,    ///< OBSOLETE: remove extent from available block storage (extent)
    OP_DIR_LINK,    ///< (re)set a dir entry (dirname, filename, ino)
    OP_DIR_UNLINK,  ///< remove a dir entry (dirname, filename)
    OP_DIR_CREATE,  ///< create a dir (dirname)
    OP_DIR_REMOVE,  ///< remove a dir (dirname)
    OP_FILE_UPDATE, ///< set/update file metadata (file)
    OP_FILE_REMOVE, ///< remove file (ino)
    OP_JUMP,        ///< jump the seq # and offset
    OP_JUMP_SEQ,    ///< jump the seq #
    OP_FILE_UPDATE_INC, ///< incremental update file metadata (file)
  } op_t;

  uuid_d uuid;          ///< fs uuid
  uint64_t seq;         ///< sequence number
  ceph::buffer::list op_bl;     ///< encoded transaction ops

  bluefs_transaction_t() : seq(0) {}

  void clear() {
    *this = bluefs_transaction_t();
  }
  bool empty() const {
    return op_bl.length() == 0;
  }

  void op_init() {
    using ceph::encode;
    encode((__u8)OP_INIT, op_bl);
  }
  void op_dir_create(std::string_view dir) {
    using ceph::encode;
    encode((__u8)OP_DIR_CREATE, op_bl);
    encode(dir, op_bl);
  }
  void op_dir_remove(std::string_view dir) {
    using ceph::encode;
    encode((__u8)OP_DIR_REMOVE, op_bl);
    encode(dir, op_bl);
  }
  void op_dir_link(std::string_view dir, std::string_view file, uint64_t ino) {
    using ceph::encode;
    encode((__u8)OP_DIR_LINK, op_bl);
    encode(dir, op_bl);
    encode(file, op_bl);
    encode(ino, op_bl);
  }
  void op_dir_unlink(std::string_view dir, std::string_view file) {
    using ceph::encode;
    encode((__u8)OP_DIR_UNLINK, op_bl);
    encode(dir, op_bl);
    encode(file, op_bl);
  }
  void op_file_update(bluefs_fnode_t& file) {
    using ceph::encode;
    encode((__u8)OP_FILE_UPDATE, op_bl);
    encode(file, op_bl);
    file.reset_delta();
  }
  /* streams update to bufferlist and clears update state */
  void op_file_update_inc(bluefs_fnode_t& file) {
    using ceph::encode;
    bluefs_fnode_delta_t delta;
    file.make_delta(&delta);
    encode((__u8)OP_FILE_UPDATE_INC, op_bl);
    encode(delta, op_bl);
    file.reset_delta();
  }

  void op_file_remove(uint64_t ino) {
    using ceph::encode;
    encode((__u8)OP_FILE_REMOVE, op_bl);
    encode(ino, op_bl);
  }
  void op_jump(uint64_t next_seq, uint64_t offset) {
    using ceph::encode;
    encode((__u8)OP_JUMP, op_bl);
    encode(next_seq, op_bl);
    encode(offset, op_bl);
  }
  void op_jump_seq(uint64_t next_seq) {
    using ceph::encode;
    encode((__u8)OP_JUMP_SEQ, op_bl);
    encode(next_seq, op_bl);
  }
  void claim_ops(bluefs_transaction_t& from) {
    op_bl.claim_append(from.op_bl);
  }

  void bound_encode(size_t &s) const;
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<bluefs_transaction_t*>& ls);
};
WRITE_CLASS_ENCODER(bluefs_transaction_t)

std::ostream& operator<<(std::ostream& out, const bluefs_transaction_t& t);
#endif
