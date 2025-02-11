#ifndef TEST_SSTRING_H
#define TEST_SSTRING_H

#include "common/sstring.hh"

// wrapper for sstring that implements the dencoder interface
class sstring_wrapper {
  using sstring16 = basic_sstring<char, uint32_t, 16>;
  sstring16 s1;
  using sstring24 = basic_sstring<char8_t, uint16_t, 24>;
  sstring24 s2;
 public:
  sstring_wrapper() = default;
  sstring_wrapper(sstring16&& s1, sstring24&& s2)
    : s1(std::move(s1)), s2(std::move(s2))
  {}

  DENC(sstring_wrapper, w, p) {
    DENC_START(1, 1, p);
    denc(w.s1, p);
    denc(w.s2, p);
    DENC_FINISH(p);
  }
  void dump(Formatter* f) {
    f->dump_string("s1", s1.c_str());
    f->dump_string("s2", reinterpret_cast<const char*>(s2.c_str()));
  }
  static void generate_test_instances(std::list<sstring_wrapper*>& ls) {
    ls.push_back(new sstring_wrapper());
    // initialize sstrings that fit in internal storage
    constexpr auto cstr6 = "abcdef";
    ls.push_back(new sstring_wrapper(sstring16{cstr6}, sstring24{cstr6}));
    // initialize sstrings that overflow into external storage
    constexpr auto cstr26 = "abcdefghijklmnopqrstuvwxyz";
    ls.push_back(new sstring_wrapper(sstring16{cstr26}, sstring24{cstr26}));
  }
};
WRITE_CLASS_DENC(sstring_wrapper)

#endif
