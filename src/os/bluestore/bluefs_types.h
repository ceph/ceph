// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_BLUEFS_TYPES_H
#define CEPH_OS_BLUESTORE_BLUEFS_TYPES_H

#include "bluestore_types.h"
#include "include/utime.h"
#include "include/encoding.h"

struct bluefs_extent_t {
  uint64_t offset;
  uint32_t length;
  uint16_t bdev;

  bluefs_extent_t(uint16_t b = 0, uint64_t o = 0, uint32_t l = 0)
    : offset(o), length(l), bdev(b) {}

  uint64_t end() const {
    return offset + length;
  }

  void encode(bufferlist&) const;
  void decode(bufferlist::iterator&);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluefs_extent_t*>&);
};
WRITE_CLASS_ENCODER(bluefs_extent_t)

ostream& operator<<(ostream& out, bluefs_extent_t e);


struct bluefs_fnode_t {
  uint64_t ino;
  uint64_t size;
  utime_t mtime;
  uint8_t prefer_bdev;
  vector<bluefs_extent_t> extents;

  bluefs_fnode_t() : ino(0), size(0), prefer_bdev(0) {}

  uint64_t get_allocated() const {
    uint64_t r = 0;
    for (auto& p : extents)
      r += p.length;
    return r;
  }

  vector<bluefs_extent_t>::iterator seek(uint64_t off, uint64_t *x_off);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<bluefs_fnode_t*>& ls);
};
WRITE_CLASS_ENCODER(bluefs_fnode_t)

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
    return ~(block_size - 1);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
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
    OP_ALLOC_RM,    ///< remove extent from availabe block storage (extent)
    OP_DIR_LINK,    ///< (re)set a dir entry (dirname, filename, ino)
    OP_DIR_UNLINK,  ///< remove a dir entry (dirname, filename)
    OP_DIR_CREATE,  ///< create a dir (dirname)
    OP_DIR_REMOVE,  ///< remove a dir (dirname)
    OP_FILE_UPDATE, ///< set/update file metadata (file)
    OP_FILE_REMOVE, ///< remove file (ino)
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
    ::encode((__u8)OP_INIT, op_bl);
  }
  void op_alloc_add(uint8_t id, uint64_t offset, uint64_t length) {
    ::encode((__u8)OP_ALLOC_ADD, op_bl);
    ::encode(id, op_bl);
    ::encode(offset, op_bl);
    ::encode(length, op_bl);
  }
  void op_alloc_rm(uint8_t id, uint64_t offset, uint64_t length) {
    ::encode((__u8)OP_ALLOC_RM, op_bl);
    ::encode(id, op_bl);
    ::encode(offset, op_bl);
    ::encode(length, op_bl);
  }
  void op_dir_create(const string& dir) {
    ::encode((__u8)OP_DIR_CREATE, op_bl);
    ::encode(dir, op_bl);
  }
  void op_dir_remove(const string& dir) {
    ::encode((__u8)OP_DIR_REMOVE, op_bl);
    ::encode(dir, op_bl);
  }
  void op_dir_link(const string& dir, const string& file, uint64_t ino) {
    ::encode((__u8)OP_DIR_LINK, op_bl);
    ::encode(dir, op_bl);
    ::encode(file, op_bl);
    ::encode(ino, op_bl);
  }
  void op_dir_unlink(const string& dir, const string& file) {
    ::encode((__u8)OP_DIR_UNLINK, op_bl);
    ::encode(dir, op_bl);
    ::encode(file, op_bl);
  }
  void op_file_update(const bluefs_fnode_t& file) {
    ::encode((__u8)OP_FILE_UPDATE, op_bl);
    ::encode(file, op_bl);
  }
  void op_file_remove(uint64_t ino) {
    ::encode((__u8)OP_FILE_REMOVE, op_bl);
    ::encode(ino, op_bl);
  }
  void op_jump_seq(uint64_t next_seq) {
    ::encode((__u8)OP_JUMP_SEQ, op_bl);
    ::encode(next_seq, op_bl);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instance(list<bluefs_transaction_t*>& ls);
};
WRITE_CLASS_ENCODER(bluefs_transaction_t)

ostream& operator<<(ostream& out, const bluefs_transaction_t& t);

#endif
