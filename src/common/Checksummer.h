// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_CHECKSUMMER
#define CEPH_OS_BLUESTORE_CHECKSUMMER

#include "xxHash/xxhash.h"

class Checksummer {
public:
  enum CSumType {
    CSUM_NONE = 1,	//intentionally set to 1 to be aligned with OSDMnitor's pool_opts_t handling - it treats 0 as unset while we need to distinguish none and unset cases
    CSUM_XXHASH32 = 2,
    CSUM_XXHASH64 = 3,
    CSUM_CRC32C = 4,
    CSUM_CRC32C_16 = 5, // low 16 bits of crc32c
    CSUM_CRC32C_8 = 6,  // low 8 bits of crc32c
    CSUM_MAX,
  };
  static const char *get_csum_type_string(unsigned t) {
    switch (t) {
    case CSUM_NONE: return "none";
    case CSUM_XXHASH32: return "xxhash32";
    case CSUM_XXHASH64: return "xxhash64";
    case CSUM_CRC32C: return "crc32c";
    case CSUM_CRC32C_16: return "crc32c_16";
    case CSUM_CRC32C_8: return "crc32c_8";
    default: return "???";
    }
  }
  static int get_csum_string_type(const std::string &s) {
    if (s == "none")
      return CSUM_NONE;
    if (s == "xxhash32")
      return CSUM_XXHASH32;
    if (s == "xxhash64")
      return CSUM_XXHASH64;
    if (s == "crc32c")
      return CSUM_CRC32C;
    if (s == "crc32c_16")
      return CSUM_CRC32C_16;
    if (s == "crc32c_8")
      return CSUM_CRC32C_8;
    return -EINVAL;
  }

  static size_t get_csum_init_value_size(int csum_type) {
    switch (csum_type) {
    case CSUM_NONE: return 0;
    case CSUM_XXHASH32: return sizeof(xxhash32::init_value_t);
    case CSUM_XXHASH64: return sizeof(xxhash64::init_value_t);
    case CSUM_CRC32C: return sizeof(crc32c::init_value_t);
    case CSUM_CRC32C_16: return sizeof(crc32c_16::init_value_t);
    case CSUM_CRC32C_8: return sizeof(crc32c_8::init_value_t);
    default: return 0;
    }
  }
  static size_t get_csum_value_size(int csum_type) {
    switch (csum_type) {
    case CSUM_NONE: return 0;
    case CSUM_XXHASH32: return 4;
    case CSUM_XXHASH64: return 8;
    case CSUM_CRC32C: return 4;
    case CSUM_CRC32C_16: return 2;
    case CSUM_CRC32C_8: return 1;
    default: return 0;
    }
  }

  struct crc32c {
    typedef uint32_t init_value_t;
    typedef __le32 value_t;

    // we have no execution context/state.
    typedef int state_t;
    static void init(state_t *state) {
    }
    static void fini(state_t *state) {
    }

    static value_t calc(
      state_t state,
      init_value_t init_value,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      return p.crc32c(len, init_value);
    }
  };

  struct crc32c_16 {
    typedef uint32_t init_value_t;
    typedef __le16 value_t;

    // we have no execution context/state.
    typedef int state_t;
    static void init(state_t *state) {
    }
    static void fini(state_t *state) {
    }

    static value_t calc(
      state_t state,
      init_value_t init_value,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      return p.crc32c(len, init_value) & 0xffff;
    }
  };

  struct crc32c_8 {
    typedef uint32_t init_value_t;
    typedef __u8 value_t;

    // we have no execution context/state.
    typedef int state_t;
    static void init(state_t *state) {
    }
    static void fini(state_t *state) {
    }

    static value_t calc(
      state_t state,
      init_value_t init_value,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      return p.crc32c(len, init_value) & 0xff;
    }
  };

  struct xxhash32 {
    typedef uint32_t init_value_t;
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
      init_value_t init_value,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      XXH32_reset(state, init_value);
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
    typedef uint64_t init_value_t;
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
      init_value_t init_value,
      size_t len,
      bufferlist::const_iterator& p
      ) {
      XXH64_reset(state, init_value);
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
    return calculate<Alg>(-1, csum_block_size, offset, length, bl, csum_data);
  }

  template<class Alg>
  static int calculate(
      typename Alg::init_value_t init_value,
      size_t csum_block_size,
      size_t offset,
      size_t length,
      const bufferlist &bl,
      bufferptr* csum_data) {
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
      *pv = Alg::calc(state, init_value, csum_block_size, p);
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
      typename Alg::value_t v = Alg::calc(state, -1, csum_block_size, p);
      if (*pv != v) {
	if (bad_csum) {
	  *bad_csum = v;
	}
	Alg::fini(&state);
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
