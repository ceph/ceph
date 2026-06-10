// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Adam Kupczyk <akupczyk@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
  The intention is to get output like this:

  sbid = { varint :80 :97 :04 = 0x16531 }
  bluestore_blob_t = {
    extents = {
      num = { varint :97 :a5 = 0x8791 }
      bluestore_pextent_t = {
        offset = { lba:76:[18]32:81 = 0x49878390000 }
        length = { varint_lowz:12:65:89 = 0x878501 }
      }
    }
    flags = { varint :06 = 0x06 } CSUM+COMPR
    logical_length = { varint_lowz:22:33 = 0x4342 }
    compressed_length = { varint_lowz:55:66 = 0x3231 }
    logical_length = 0x5000
    csum_type = { :04 = 0x4 } crc32c
    csum_chunk_order = { :0c = 12} 0x1000
    len = { varint :38 = 0x08 }
    csum_data = { :01:02:03:[20]04:05:06:07:08}
    :a1:a2:a3:a4:[28]a5:
    unused = { :02:04 = 0x0402 } 00010000100001001
*/



// TODO FIX RATIONALE
// If you #include "include/encoding.h" you get the old-style *and*
// the new-style definitions.  (The old-style needs denc_traits<> in
// order to disable the container helpers when new-style traits are
// present.)

// You can also just #include "include/denc.h" and get only the
// new-style helpers.  The eventual goal is to drop the legacy
// definitions.

#ifndef DENC_ANOTE_H
#define DENC_ANOTE_H

#include <sstream>
#include "include/buffer.h"

namespace anote {
using bptr_c_it_t = ::ceph::buffer::ptr::const_iterator;
// annotator works as iterator for denc purposes
// 
class annotator {
  bptr_c_it_t& it;
  bptr_c_it_t last_it;
  std::stringstream printed;
  uint32_t depth = 0;
  bool just_newlined = true;
  public:
  annotator(bptr_c_it_t& it)
    : it(it), last_it(it)
  {}

  // There are 2 functions to conversion to bptr_c_it_t:
  // 1) Transition from annotator-ed functions to regular bptr_c_it_t decoders.
  //    This is useful to implement annotating of important parts, without need to transform all.
  // 2) In ANOTE* macros allow constexpr else parts to be semantically correct.
  operator bptr_c_it_t& () {
    return it;
  }

  bptr_c_it_t& get_it() {
    return it;
  }

  bool end() {
    return it.end();
  }

  const char* get_pos_add(size_t n) {
    return it.get_pos_add(n);
  }

  auto get_ptr(size_t len) {
    return it.get_ptr(len);
  }

  void open_scope(const std::string& name) {
    catchup();
    indent();
    depth++;
    print(name + " { ");
  };

  void open_small_scope(const std::string& name) {
    catchup();
    indent();
    depth++;
    print(name + " { ");
  };

  void close_scope(){
    depth--;
    indent();
    catchup();
    print("} ");
  };

  void close_small_scope(){
    depth--;
    catchup();
    print(" } ");
  };

  void abort_scope(){
    depth--;
    catchup();
  };

  anote::annotator& print(uint64_t v) {
    printed << std::hex << "0x" << v << std::dec;
    just_newlined = false;
    return *this;
  }

  anote::annotator& print(const std::string& s) {
    printed << s;
    just_newlined = false;
    return *this;
  }

  anote::annotator& indent() {
    newline();
    printed << std::string(depth * 2, ' ');
    just_newlined = false;
    return *this;
  }

  anote::annotator& newline() {
    if(!just_newlined) {
      printed << "\n";
      just_newlined = true;
    }
    return *this;
  }

  // Displays bytes from bytestream that have been decoded already.
  // Fills the gap between what `it` has actually decoded and
  // what `annotator` know was decoded.
  // Usually prints bytes happens inside denc primitives:
  // logical_length = { varint_lowz:23:45 = 0x1148000 }
  // Entities untracked by annotator are printed outside scopes:
  // csum_data = { :01:02:03:[20]04:05:06:07:08 }
  //  :a1:a2:a3:a4:[28]a5: <- annotator untracked bytes
  // unused = { :02:04 = 0x0402 } 00010000100001001
  //
  // Format: sequence of hex bytes ":1a:2b:3c:4d",
  // with bytestream offset injected every 8 bytes: ":1a:2b:[18]3c:4d".
  void catchup() {
    while (last_it.get_pos() < it.get_pos()) {
      char s[30];
      size_t ofs = last_it.get_offset();
      if ((ofs % 16) == 0) {
        snprintf(s, 29, ":[%lx]%2.2x", ofs, (uint8_t)(*last_it.get_pos()));
      } else {
        snprintf(s, 29, ":%2.2x", (uint8_t)(*last_it.get_pos()));
      }
      printed << s;
      last_it += 1;
    }
  }
  // Retrieve decoded result.
  std::string get_string() {
    return printed.str();
  }
};

// promote() is used to get annotator& from type p, when
// p can be technically either bptr_c_it_t or annotator.
// The portion that will actually be executed will always be contained
// within 'if constexpr', but the actions have to be semantically
// correct.
[[deprecated("this function is only for compilation of if constexpr statements")]]
inline annotator& promote(bptr_c_it_t& it) {
  // This is not used in practice.
  return *reinterpret_cast<annotator*>(&it);
}
inline annotator& promote(annotator& an) {
  return an;
}

class injected_scope {
  annotator* an;
  public:
  injected_scope(annotator& _an, const std::string& name)
  : an(&_an) {
    if (an)
      an->open_scope(name);
  }
  injected_scope(bptr_c_it_t& it, const std::string& name)
  : an(nullptr) {}
  ~injected_scope() {
    if (an) {
      if (std::uncaught_exceptions()) {
        an->abort_scope();
      } else {
        an->close_scope();
      }
    }
  }

  operator annotator& () {
    return *an;
  }
  operator bptr_c_it_t& () {
    return *an;
  }
  annotator& get() {
    return *an;
  }
};

class small_scope {
  annotator* an;
  public:
  small_scope(annotator& _an, const std::string& name)
  : an(&_an) {
    if (an)
      an->open_small_scope(name);
  }
  small_scope(bptr_c_it_t& it, const std::string& name)
  : an(nullptr) {}
  ~small_scope() {
    if (an) {
      if (std::uncaught_exceptions()) {
        an->abort_scope();
      } else {
        an->close_small_scope();
      }
    }
  }
  operator annotator& () {
    return *an;
  }
  operator bptr_c_it_t& () {
    return *an;
  }
  annotator& get() {
    return *an;
  }
};




struct conveyor_wrapper {
  bptr_c_it_t& it;
  conveyor_wrapper(bptr_c_it_t& it)
  : it(it) {}
  bptr_c_it_t& get() {
    return it;
  }
  operator bptr_c_it_t& () {
    return it;
  }
};


// converstion to iterator when callee does not accept `annotator` but likes `const_iterator`


inline conveyor_wrapper
scope(bptr_c_it_t& it, const std::string& name) {
  return conveyor_wrapper(it);
}
// provide temporary object that will open scope in annotator on creation
// and close scope on destruction
inline injected_scope scope(annotator& ca_it, const std::string& name) {
  return injected_scope(ca_it, name);
}


#define ANOTE_NAME_NONL(decoder_func, element_name_str, element_variable, iterator) \
if constexpr (!std::same_as<decltype(iterator), anote::annotator&>) { \
  decoder_func(element_variable, iterator); \
} else { \
  { anote::small_scope _(iterator, element_name_str); \
    anote::promote(iterator).print(#decoder_func); \
    anote::promote(iterator).print(" "); \
    decoder_func(element_variable, iterator); } \
  anote::promote(iterator).print("= "); \
  anote::promote(iterator).print(element_variable); \
  anote::promote(iterator).print(" "); \
}

#define ANOTE_NAME(decoder_func, element_name_str, element_variable, iterator) \
ANOTE_NAME_NONL(decoder_func, element_name_str, element_variable, iterator) \
if constexpr (std::same_as<decltype(iterator), anote::annotator&>) { \
  anote::promote(iterator).newline(); \
}

#define ANOTE(decoder_func, element_variable, iterator) \
ANOTE_NAME(decoder_func, #element_variable, element_variable, iterator)

#define ANOTE_NONL(decoder_func, element_variable, iterator) \
ANOTE_NAME_NONL(decoder_func, #element_variable, element_variable, iterator)



// A macro to simulate (is_annotating)?when_anote_expression:other_expression .
// The intention is to compile expression that have side effects only for annotating.
// The main decode has to not be burdened with extra code.
//
// Example:
// ANOTE_NAME(denc_lba, to_string(extent_id), disk_offset, p)
//                      ^evaluates in all cases
// ANOTE_NAME(denc_lba, ANOTE_EXPR(to_string(extent_id), "", p), disk_offset, p)
//                               ^only evaluates when p = annotator
#define ANOTE_EXPR(when_anote_expression, other_expression, iterator)   \
[&](){                                                                  \
  if constexpr (std::same_as<decltype(iterator), anote::annotator&>)    \
    { return when_anote_expression; } else { return other_expression; } \
  } ()

#define ANOTE_IF(iterator)                                              \
  if constexpr (std::same_as<decltype(iterator), anote::annotator&>)    \

// Defines concept that allows just 2 types:
// - bptr_c_it_t (aka ::ceph::buffer::ptr::const_iterator)
// - anote::annotator
// The concept is useful when transforming exising decoding function into
// dual function template:
// - one instance is doing exactly the same thing as before (iterator = bptr_c_it_t)
// - one instance is annotating its decoding process (iterator = annotator)

  template<typename T>
  concept annotator_or_iterator =
  (std::same_as<T, bptr_c_it_t> || std::same_as<T, annotator> );

} // namespace anote

#endif