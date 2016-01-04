// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_INCLUDE_FS_TYPES_H
#define CEPH_INCLUDE_FS_TYPES_H

#include "types.h"
#include "utime.h"

// --------------------------------------
// ino

typedef uint64_t _inodeno_t;

struct inodeno_t {
  _inodeno_t val;
  inodeno_t() : val(0) {}
  // cppcheck-suppress noExplicitConstructor
  inodeno_t(_inodeno_t v) : val(v) {}
  inodeno_t operator+=(inodeno_t o) { val += o.val; return *this; }
  operator _inodeno_t() const { return val; }

  void encode(bufferlist& bl) const {
    ::encode(val, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(val, p);
  }
} __attribute__ ((__may_alias__));
WRITE_CLASS_ENCODER(inodeno_t)

inline ostream& operator<<(ostream& out, inodeno_t ino) {
  return out << hex << ino.val << dec;
}

namespace std {
  template<> struct hash< inodeno_t >
  {
    size_t operator()( const inodeno_t& x ) const
    {
      static rjhash<uint64_t> H;
      return H(x.val);
    }
  };
} // namespace std


// file modes

static inline bool file_mode_is_readonly(int mode) {
  return (mode & CEPH_FILE_MODE_WR) == 0;
}


// dentries
#define MAX_DENTRY_LEN 255

// --
namespace ceph {
  class Formatter;
}
void dump(const ceph_file_layout& l, ceph::Formatter *f);
void dump(const ceph_dir_layout& l, ceph::Formatter *f);



// file_layout_t

struct file_layout_t {
  // file -> object mapping
  uint32_t stripe_unit;   ///< stripe unit, in bytes,
  uint32_t stripe_count;  ///< over this many objects
  uint32_t object_size;   ///< until objects are this big

  int64_t pool_id;        ///< rados pool id
  string pool_ns;         ///< rados pool namespace

  file_layout_t(uint32_t su=0, uint32_t sc=0, uint32_t os=0)
    : stripe_unit(su),
      stripe_count(sc),
      object_size(os),
      pool_id(-1) {
  }

  static file_layout_t get_default() {
    return file_layout_t(1<<22, 1, 1<<22);
  }

  uint64_t get_period() const {
    return stripe_count * object_size;
  }

  void from_legacy(const ceph_file_layout& fl);
  void to_legacy(ceph_file_layout *fl) const;

  bool is_valid() const;

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<file_layout_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(file_layout_t)

WRITE_EQ_OPERATORS_5(file_layout_t, stripe_unit, stripe_count, object_size, pool_id, pool_ns);

#endif
