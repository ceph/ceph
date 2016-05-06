// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_CHECKSUMMER
#define CEPH_OS_BLUESTORE_CHECKSUMMER

#include "include/buffer.h"
#include "xxHash/xxhash.h"
#include "bluestore_types.h"

class Checksummer {

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
      bufferlist t;
      p.copy(len, t);
      return t.crc32c(-1);
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

  template<class Alg>
  int calculate(
    size_t csum_block_size,
    size_t offset,
    size_t length,
    const bufferlist &bl,
    vector<char>* csum_data
    ) {
    assert(length % csum_block_size == 0);
    bufferlist::const_iterator p = bl.begin();
    if (offset)
      p.advance(offset);
    assert(bl.length() - offset >= length);

    typename Alg::state_t state;
    Alg::init(&state);

    assert(csum_data->size() >= (offset + length) / csum_block_size *
	   sizeof(typename Alg::value_t));
    
    typename Alg::value_t *pv =
      reinterpret_cast<typename Alg::value_t*>(&csum_data[0]);
    pv += offset / csum_block_size;
    while (length > 0) {
      *pv = Alg::calc(state, csum_block_size, p);
      ++pv;
    }
    Alg::fini(&state);
    return 0;
  }

  template<class Alg>
  int verify(
    size_t csum_block_size,
    size_t offset,
    size_t length,
    const bufferlist &bl,
    const vector<char>& csum_data
    ) {
    assert(length % csum_block_size == 0);
    bufferlist::iterator p = bl.begin();
    if (offset)
      p.advance(offset);
    assert(bl.length() - offset >= length);

    typename Alg::state_t state;
    Alg::init(&state);

    const typename Alg::value_t *pv =
      reinterpret_cast<const typename Alg::value_t*>(&csum_data[0]);
    pv += offset / csum_block_size;
    size_t pos = offset;
    while (length > 0) {
      typename Alg::value_t v = Alg::calc(state, csum_block_size, p);
      if (*pv != v) {
	return pos;
      }
      ++pv;
      pos += csum_block_size;
    }
    Alg::fini(&state);
    return -1;  // no errors
  }

  int calculate(
    bluestore_blob_t::CSumType type,
    size_t csum_block_size,
    size_t offset,
    size_t length,
    const bufferlist &bl,
    vector<char>* csum_data
    ) {
    switch (type) {
    case bluestore_blob_t::CSUM_NONE:
      return 0;
    case bluestore_blob_t::CSUM_XXHASH32:
      return calculate<xxhash32>(csum_block_size, offset, length, bl, csum_data);
    case bluestore_blob_t::CSUM_CRC32C:
      return calculate<crc32c>(csum_block_size, offset, length, bl, csum_data);
    default:
      return -EOPNOTSUPP;
    }
  }
  
};

#endif
