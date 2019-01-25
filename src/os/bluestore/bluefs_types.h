// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_BLUEFS_TYPES_H
#define CEPH_OS_BLUESTORE_BLUEFS_TYPES_H

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

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluefs_extent_t*>&);
};
WRITE_CLASS_DENC(bluefs_extent_t)

ostream& operator<<(ostream& out, const bluefs_extent_t& e);


struct bluefs_fnode_t {
  uint64_t ino;
  uint64_t size;
  utime_t mtime;
  uint8_t prefer_bdev;
  mempool::bluefs::vector<bluefs_extent_t> extents;
  uint64_t allocated;

  bluefs_fnode_t() : ino(0), size(0), prefer_bdev(0), allocated(0) {}

  uint64_t get_allocated() const {
    return allocated;
  }

  void recalc_allocated() {
    allocated = 0;
    for (auto& p : extents)
      allocated += p.length;
  }

  DENC_HELPERS
  void bound_encode(size_t& p) const {
    _denc_friend(*this, p);
  }
  void encode(bufferlist::contiguous_appender& p) const {
    DENC_DUMP_PRE(bluefs_fnode_t);
    _denc_friend(*this, p);
    DENC_DUMP_POST(bluefs_fnode_t);
  }
  void decode(buffer::ptr::const_iterator& p) {
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
    denc(v.prefer_bdev, p);
    denc(v.extents, p);
    DENC_FINISH(p);
  }

  void append_extent(const bluefs_extent_t& ext) {
    extents.push_back(ext);
    allocated += ext.length;
  }

  void pop_front_extent() {
    auto it = extents.begin();
    allocated -= it->length;
    extents.erase(it);
  }
  
  void swap_extents(bluefs_fnode_t& other) {
    other.extents.swap(extents);
    std::swap(allocated, other.allocated);
  }
  void swap_extents(mempool::bluefs::vector<bluefs_extent_t>& swap_to, uint64_t& new_allocated) {
    swap_to.swap(extents);
    std::swap(allocated, new_allocated);
  }
  void clear_extents() {
    extents.clear();
    allocated = 0;
  }

  mempool::bluefs::vector<bluefs_extent_t>::iterator seek(
    uint64_t off, uint64_t *x_off);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluefs_fnode_t*>& ls);

};
WRITE_CLASS_DENC(bluefs_fnode_t)

ostream& operator<<(ostream& out, const bluefs_fnode_t& file);


struct bluefs_super_t {
  uuid_d uuid;      ///< unique to this bluefs instance
  uuid_d osd_uuid;  ///< matches the osd that owns us
  uint64_t version;
  uint32_t block_size;

  bluefs_fnode_t log_fnode;

  bluefs_super_t()
    : version(0),
      block_size(4096) { }

  uint64_t block_mask() const {
    return ~((uint64_t)block_size - 1);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluefs_super_t*>& ls);
};
WRITE_CLASS_ENCODER(bluefs_super_t)

ostream& operator<<(ostream&, const bluefs_super_t& s);


struct bluefs_transaction_t {
  typedef enum {
    OP_NONE = 0,
    OP_INIT,        ///< initial (empty) file system marker
    OP_ALLOC_ADD,   ///< add extent to available block storage (extent)
    OP_ALLOC_RM,    ///< remove extent from available block storage (extent)
    OP_DIR_LINK,    ///< (re)set a dir entry (dirname, filename, ino)
    OP_DIR_UNLINK,  ///< remove a dir entry (dirname, filename)
    OP_DIR_CREATE,  ///< create a dir (dirname)
    OP_DIR_REMOVE,  ///< remove a dir (dirname)
    OP_FILE_UPDATE, ///< set/update file metadata (file)
    OP_FILE_REMOVE, ///< remove file (ino)
    OP_JUMP,        ///< jump the seq # and offset
    OP_JUMP_SEQ,    ///< jump the seq #
  } op_t;

  uuid_d uuid;          ///< fs uuid
  uint64_t seq;         ///< sequence number
  bufferlist op_bl;     ///< encoded transaction ops

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
  void op_alloc_add(uint8_t id, uint64_t offset, uint64_t length) {
    using ceph::encode;
    encode((__u8)OP_ALLOC_ADD, op_bl);
    encode(id, op_bl);
    encode(offset, op_bl);
    encode(length, op_bl);
  }
  void op_alloc_rm(uint8_t id, uint64_t offset, uint64_t length) {
    using ceph::encode;
    encode((__u8)OP_ALLOC_RM, op_bl);
    encode(id, op_bl);
    encode(offset, op_bl);
    encode(length, op_bl);
  }
  void op_dir_create(const string& dir) {
    using ceph::encode;
    encode((__u8)OP_DIR_CREATE, op_bl);
    encode(dir, op_bl);
  }
  void op_dir_remove(const string& dir) {
    using ceph::encode;
    encode((__u8)OP_DIR_REMOVE, op_bl);
    encode(dir, op_bl);
  }
  void op_dir_link(const string& dir, const string& file, uint64_t ino) {
    using ceph::encode;
    encode((__u8)OP_DIR_LINK, op_bl);
    encode(dir, op_bl);
    encode(file, op_bl);
    encode(ino, op_bl);
  }
  void op_dir_unlink(const string& dir, const string& file) {
    using ceph::encode;
    encode((__u8)OP_DIR_UNLINK, op_bl);
    encode(dir, op_bl);
    encode(file, op_bl);
  }
  void op_file_update(const bluefs_fnode_t& file) {
    using ceph::encode;
    encode((__u8)OP_FILE_UPDATE, op_bl);
    encode(file, op_bl);
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

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluefs_transaction_t*>& ls);
};
WRITE_CLASS_ENCODER(bluefs_transaction_t)

ostream& operator<<(ostream& out, const bluefs_transaction_t& t);

#endif
