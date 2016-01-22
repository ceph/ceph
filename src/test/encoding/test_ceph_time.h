#ifndef TEST_CEPH_TIME_H
#define TEST_CEPH_TIME_H

#include <list>
#include "common/ceph_time.h"

// wrapper for ceph::real_time that implements the dencoder interface
class real_time_wrapper {
  ceph::real_time t;
 public:
  real_time_wrapper() = default;
  real_time_wrapper(const ceph::real_time& t) : t(t) {}

  void encode(bufferlist& bl) const {
    ::encode(t, bl);
  }
  void decode(bufferlist::iterator &p) {
    ::decode(t, p);
  }
  void dump(Formatter* f) {
    auto epoch_time = ceph::real_clock::to_time_t(t);
    f->dump_string("time", std::ctime(&epoch_time));
  }
  static void generate_test_instances(std::list<real_time_wrapper*>& ls) {
    constexpr time_t t{455500800}; // Ghostbusters release date
    ls.push_back(new real_time_wrapper(ceph::real_clock::from_time_t(t)));
  }
};

#endif
