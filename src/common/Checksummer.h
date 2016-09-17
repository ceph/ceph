// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_CHECKSUMMER
#define CEPH_OS_BLUESTORE_CHECKSUMMER

#include "include/buffer.h"
#include "xxHash/xxhash.h"

class Checksummer {
public:
  struct crc32c {
    typedef __le32 value_t;

    // we have no execution context/state.
    typedef int state_t;
    static void init(state_t *state) {
    }
    static void fini(state_t *state) {
    }

    static value_t calc(
      state_t state,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      return p.crc32c(len, -1);
    }
  };

  struct crc32c_16 {
    typedef __le16 value_t;

    // we have no execution context/state.
    typedef int state_t;
    static void init(state_t *state) {
    }
    static void fini(state_t *state) {
    }

    static value_t calc(
      state_t state,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      return p.crc32c(len, -1) & 0xffff;
    }
  };

  struct crc32c_8 {
    typedef __u8 value_t;

    // we have no execution context/state.
    typedef int state_t;
    static void init(state_t *state) {
    }
    static void fini(state_t *state) {
    }

    static value_t calc(
      state_t state,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      return p.crc32c(len, -1) & 0xff;
    }
  };

  struct xxhash32 {
    typedef __le32 value_t;

    typedef XXH32_state_t *state_t;
    static void init(state_t *s) {
      *s = XXH32_createState();
    }
    static void fini(state_t *s) {
      XXH32_freeState(*s);
    }

    static value_t calc(
      state_t state,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      XXH32_reset(state, -1);
      while (len > 0) {
	const char *data;
	size_t l = p.get_ptr_and_advance(len, &data);
	XXH32_update(state, data, l);
	len -= l;
      }
      return XXH32_digest(state);
    }
  };

  struct xxhash64 {
    typedef __le64 value_t;

    typedef XXH64_state_t *state_t;
    static void init(state_t *s) {
      *s = XXH64_createState();
    }
    static void fini(state_t *s) {
      XXH64_freeState(*s);
    }

    static value_t calc(
      state_t state,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      XXH64_reset(state, -1);
      while (len > 0) {
	const char *data;
	size_t l = p.get_ptr_and_advance(len, &data);
	XXH64_update(state, data, l);
	len -= l;
      }
      return XXH64_digest(state);
    }
  };

  template<class Alg>
  static int calculate(
    size_t csum_block_size,
    size_t offset,
    size_t length,
    const bufferlist &bl,
    bufferptr* csum_data
    ) {
    assert(length % csum_block_size == 0);
    size_t blocks = length / csum_block_size;
    bufferlist::const_iterator p = bl.begin();
    assert(bl.length() >= length);

    typename Alg::state_t state;
    Alg::init(&state);

    assert(csum_data->length() >= (offset + length) / csum_block_size *
	   sizeof(typename Alg::value_t));

    typename Alg::value_t *pv =
      reinterpret_cast<typename Alg::value_t*>(csum_data->c_str());
    pv += offset / csum_block_size;
    while (blocks--) {
      *pv = Alg::calc(state, csum_block_size, p);
      ++pv;
    }
    Alg::fini(&state);
    return 0;
  }

  template<class Alg>
  static int verify(
    size_t csum_block_size,
    size_t offset,
    size_t length,
    const bufferlist &bl,
    const bufferptr& csum_data,
    uint64_t *bad_csum=0
    ) {
    assert(length % csum_block_size == 0);
    bufferlist::const_iterator p = bl.begin();
    assert(bl.length() >= length);

    typename Alg::state_t state;
    Alg::init(&state);

    const typename Alg::value_t *pv =
      reinterpret_cast<const typename Alg::value_t*>(csum_data.c_str());
    pv += offset / csum_block_size;
    size_t pos = offset;
    while (length > 0) {
      typename Alg::value_t v = Alg::calc(state, csum_block_size, p);
      if (*pv != v) {
	if (bad_csum) {
	  *bad_csum = v;
	}
	return pos;
      }
      ++pv;
      pos += csum_block_size;
      length -= csum_block_size;
    }
    Alg::fini(&state);
    return -1;  // no errors
  }
};

#endif
