#ifndef TEST_STRING_H
#define TEST_STRING_H

#include "common/Formatter.h"

// wrapper for std::string that implements the dencoder interface
class string_wrapper {
  std::string s;
  public:
   string_wrapper() = default;
   string_wrapper(std::string s1)
    : s(s1)
   {}

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(s, bl);
  }

  void decode(ceph::buffer::list::const_iterator &bl) {
    using ceph::decode;
    decode(s, bl);
  }

  void dump(Formatter* f) {
    f->dump_string("s", s);
  }

  static std::list<string_wrapper> generate_test_instances() {
    std::list<string_wrapper> ls;
    ls.push_back(string_wrapper());
    // initialize strings that fit in internal storage
    std::string s1 = "abcdef";
    ls.push_back(string_wrapper(s1));
    return ls;
  }
};
WRITE_CLASS_ENCODER(string_wrapper)

#endif
